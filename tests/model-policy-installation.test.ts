import {beforeAll,beforeEach,afterEach,afterAll,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {PgBoss} from 'pg-boss';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {prepareSchema,migrationReceiptKey} from '../server/schema-readiness.js';
import {prepareBacklogSchema,backlogMigrationReceiptKey} from '../server/backlog-schema-readiness.js';
import {claimWorkerDrain,admitWorkerRestart,installWorkerRolloutAdmission} from '../server/worker-rollout.js';
import {installOperatorModelPolicy,modelPolicyDigest,type PolicyInstallationObservation} from '../server/model-policy-installation.js';
import {assertProtectedPolicyMetadata,modelPolicyInstallMain} from '../scripts/install-model-policy.js';

const policy={version:2 as const,mode:'exclusive-model' as const,accountId:'agentos-api-11111111-2222-4333-8444-555555555555',
  bridgeId:'agentos:api:11111111-2222-4333-8444-555555555555',model:'deepseek-v4-pro-0813' as const,requestLimit:6 as const,sessionLimit:12 as const};
const previous={version:1,mode:'glm-only',accountId:'agentos-api-00000000-0000-4000-8000-000000000001',
  bridgeId:'agentos:api:00000000-0000-4000-8000-000000000001',model:'glm-5.3-flash',requestLimit:6,sessionLimit:12};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await prepareSchema();await prepareBacklogSchema();await installWorkerRolloutAdmission();
  const boss=new PgBoss({connectionString:config.databaseUrl,schema:'agentos_queue'});boss.on('error',()=>{});
  await boss.start();await boss.createQueue('runs');await boss.stop({graceful:true});
});
beforeEach(async()=>{
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','model-policy') OR id LIKE 'model-policy-install:%' OR id LIKE 'planning-source-preparation:%'");
  await pool.query('TRUNCATE goals,agents,accounts,cycles,chats,events CASCADE');await pool.query('TRUNCATE agentos_queue.job CASCADE');
  await seedRegistry();await prepareSchema();await prepareBacklogSchema();
});
afterEach(async()=>{await pool.query("DELETE FROM settings WHERE id IN('maintenance','model-policy')");});
afterAll(()=>pool.end());
async function fixture(){
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,bridge_id,connection_status)
    VALUES($1,'Fixture','openai',$2,$3,true,12,$4,'connected')`,[policy.accountId,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
  await pool.query("INSERT INTO settings(id,value) VALUES('model-policy',$1)",[JSON.stringify(previous)]);
  const record={candidate:'/opt/agentos/releases/managed-'+randomUUID(),digest:'a'.repeat(64)};
  const drain=await tx(db=>claimWorkerDrain(db,record,'global'));expect(drain.status).toBe('draining');
  const token=(drain as any).token;expect((await tx(db=>admitWorkerRestart(db,token))).ready).toBe(true);
  const input={version:1 as const,...record,token,expectedPolicyFileDigest:'b'.repeat(64),expectedPreviousPolicyDigest:modelPolicyDigest(previous)};
  const observe=async():Promise<PolicyInstallationObservation>=>({checkedAt:Date.now(),sourceDigest:record.digest,fileDigest:input.expectedPolicyFileDigest,
    fileIdentity:'protected-inode-fixture',policy,servicesStopped:true,gateQuiescent:true,publicationLockHeld:true});
  return {input,observe};
}
async function current(){return (await pool.query("SELECT value FROM settings WHERE id='model-policy'")).rows[0]?.value;}
async function noEffect(){expect(await current()).toEqual(previous);expect((await pool.query("SELECT 1 FROM events WHERE kind='MODEL_POLICY_INSTALLED'")).rowCount).toBe(0);}
it('installs once atomically, preserves the drain, and reconciles concurrent/lost replies without a second event',async()=>{
  const f=await fixture(),maintenance=(await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0].value;
  const goal=randomUUID(),task=randomUUID(),agent=(await pool.query('SELECT id FROM agents LIMIT 1')).rows[0].id;
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Historical','Preserve QA','fixture',$1::text)",[goal]);
  await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,model,criteria) VALUES($1,$2,'qa','Historical QA','Preserve exact acceptance',$3,'glm-5.3-flash','[]')",[task,goal,agent]);
  const original=(await pool.query('SELECT * FROM tasks WHERE id=$1',[task])).rows[0];
  const results=await Promise.all([installOperatorModelPolicy(f.input,f.observe),installOperatorModelPolicy(f.input,f.observe)]);
  expect(results.map(result=>result.alreadyApplied).sort()).toEqual([false,true]);expect(await current()).toEqual(policy);
  expect((await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0].value).toEqual(maintenance);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MODEL_POLICY_INSTALLED'")).rowCount).toBe(1);
  const replay=await installOperatorModelPolicy(f.input,f.observe);expect(replay.installedAt).toBe(results[0].installedAt);
  expect(replay.alreadyApplied).toBe(true);
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[task])).rows[0]).toEqual(original);
});
it.each(['token','digest','candidate','phase','mode','owner','draining'] as const)('refuses altered drain authority: %s',async field=>{
  const f=await fixture();const row=(await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0].value;
  if(field==='owner')row.owner='other';else if(field==='draining')row.draining=false;else row.workerRollout[field]='changed';
  await pool.query("UPDATE settings SET value=$1 WHERE id='maintenance'",[JSON.stringify(row)]);
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toMatchObject({code:'MODEL_POLICY_DRAIN_OWNER'});await noEffect();
});
it.each(['disconnected','unknown','reconnect_required'])('refuses account state %s',async connection=>{
  const f=await fixture();await pool.query('UPDATE accounts SET connection_status=$1 WHERE id=$2',[connection,policy.accountId]);
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toMatchObject({code:'MODEL_POLICY_ACCOUNT_UNAVAILABLE'});await noEffect();
});
it.each(['model','bridge','limit','disabled'])('refuses changed account authority %s',async kind=>{
  const f=await fixture();const sql={model:"models='[]'",bridge:"bridge_id='unverified'",limit:'max_concurrency=13',disabled:'enabled=false'}[kind]!;
  await pool.query('UPDATE accounts SET '+sql+' WHERE id=$1',[policy.accountId]);
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toMatchObject({code:'MODEL_POLICY_ACCOUNT_UNAVAILABLE'});await noEffect();
});
it.each(['stale','source','file','gate','process','lock'])('refuses stale or uncertain host proof %s',async kind=>{
  const f=await fixture(),observation:any=await f.observe();
  if(kind==='stale')observation.checkedAt-=31000;if(kind==='source')observation.sourceDigest='c'.repeat(64);
  if(kind==='file')observation.fileDigest='c'.repeat(64);if(kind==='gate')observation.gateQuiescent=false;
  if(kind==='process')observation.servicesStopped=false;if(kind==='lock')observation.publicationLockHeld=false;
  await expect(installOperatorModelPolicy(f.input,async()=>observation)).rejects.toThrow();await noEffect();
});
it.each([2,3])('rolls back policy, receipt and event when the host changes at recapture %s',async at=>{
  const f=await fixture();let count=0;
  await expect(installOperatorModelPolicy(f.input,async()=>({...await f.observe(),fileIdentity:++count===at?'changed':'protected-inode-fixture'})))
    .rejects.toMatchObject({code:'MODEL_POLICY_FILE_CHANGED'});await noEffect();
  expect((await pool.query("SELECT 1 FROM settings WHERE id LIKE 'model-policy-install:%'")).rowCount).toBe(0);
});
it.each(['preparation','planning-preparation','chat','queue'])('refuses additional quiet-window blocker %s',async kind=>{
  const f=await fixture();
  if(kind==='planning-preparation')await pool.query("INSERT INTO settings VALUES('planning-source-preparation:fixture','{\"state\":\"preparing\"}')");
  if(kind==='chat'){
    const id=randomUUID();await pool.query("INSERT INTO chats(id,title) VALUES($1,'Fixture')",[id]);
    await pool.query("INSERT INTO chat_messages(id,chat_id,role,state) VALUES($1,$2,'assistant','running')",[randomUUID(),id]);
  }
  if(kind==='preparation'){
    const goal=randomUUID();await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Fixture','Fixture','fixture',$1::text)",[goal]);
    await pool.query("INSERT INTO workspace_preparations(id,goal_id,workspace,source_workspace,state) VALUES($1,$2,'fixture/copy','fixture/base','failed')",[randomUUID(),goal]);
  }
  if(kind==='queue')await pool.query("INSERT INTO agentos_queue.job(name,state,data) VALUES('runs','active','{}')");
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toMatchObject({code:'MODEL_POLICY_NOT_QUIET'});await noEffect();
});
it.each([migrationReceiptKey,backlogMigrationReceiptKey])('requires installed source-bound schema receipt %s',async key=>{
  const f=await fixture();await pool.query('DELETE FROM settings WHERE id=$1',[key]);
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toThrow();await noEffect();
});
it('refuses previous-policy mismatch and a corrupted completed receipt rather than resetting either',async()=>{
  const f=await fixture();await expect(installOperatorModelPolicy({...f.input,expectedPreviousPolicyDigest:null},f.observe))
    .rejects.toMatchObject({code:'MODEL_POLICY_PREVIOUS_CHANGED'});await noEffect();
  await installOperatorModelPolicy(f.input,f.observe);
  await pool.query("UPDATE settings SET value=value||'{\"model\":\"other\"}' WHERE id=$1",['model-policy-install:'+f.input.token]);
  await expect(installOperatorModelPolicy(f.input,f.observe)).rejects.toMatchObject({code:'MODEL_POLICY_INSTALL_CONFLICT'});
  expect(await current()).toEqual(policy);expect((await pool.query("SELECT 1 FROM events WHERE kind='MODEL_POLICY_INSTALLED'")).rowCount).toBe(1);
});
it('rejects unsafe policy file ownership, modes, links and oversized data',()=>{
  const safe={isFile:()=>true,uid:0n,nlink:1n,mode:0o600n,size:100n};assertProtectedPolicyMetadata(safe);
  for(const change of [{uid:989n},{nlink:2n},{mode:0o644n},{mode:0o660n},{size:4097n},{size:0n},{isFile:()=>false}])
    expect(()=>assertProtectedPolicyMetadata({...safe,...change})).toThrow('MODEL_POLICY_FILE_PROTECTION');
});
it('real CLI entry rejects the unprivileged test UID before filesystem, gate or SQL effects',async()=>{
  expect(process.getuid?.()).not.toBe(0);await expect(modelPolicyInstallMain(['--publication-lock-fd','9']))
    .rejects.toMatchObject({code:'MODEL_POLICY_OPERATOR_UID'});
});

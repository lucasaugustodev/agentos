import {afterAll,afterEach,beforeAll,beforeEach,expect,it} from 'vitest';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {chooseWorkforceRoute,workforceCapacityReason} from '../server/workforce-policy.js';
import {exclusiveModelPolicySql} from '../server/model-policy-sql.js';
import {managerAuthoritySql} from '../server/manager-authority.js';
import {generationTiming} from '../server/generation-timing.js';
import {managementRoutes} from '../server/management.js';
import {resolveSupervisedRoute} from '../server/supervised-route.js';
import {claimPlanning,submitObjective} from '../server/planner.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {chooseChatAccount} from '../server/accounts.js';

const policy={version:2,mode:'exclusive-model',accountId:'agentos-api-11111111-2222-4333-8444-555555555555',
  bridgeId:'agentos:api:11111111-2222-4333-8444-555555555555',model:'deepseek-v4-pro-0813',requestLimit:6,sessionLimit:12};
let dir:string,previous:string|undefined;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
  previous=process.env.AGENTOS_MODEL_POLICY_FILE;dir=await mkdtemp(tmpdir()+'/agentos-deepseek-policy-');
  process.env.AGENTOS_MODEL_POLICY_FILE=dir+'/policy.json';await writeFile(dir+'/policy.json',JSON.stringify(policy));
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='model-policy'");
});
afterEach(async()=>{if(previous===undefined)delete process.env.AGENTOS_MODEL_POLICY_FILE;else process.env.AGENTOS_MODEL_POLICY_FILE=previous;
  await rm(dir,{recursive:true,force:true});await pool.query("DELETE FROM settings WHERE id='model-policy'");});
afterAll(async()=>{await pool.end();});
async function fixture(limit=12){
  const goal=randomUUID(),agent=(await pool.query('SELECT id FROM agents LIMIT 1')).rows[0].id;
  await pool.query("INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,bridge_id) VALUES($1,'Fixture','openai',$2,$3,true,12,$4)",
    [policy.accountId,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Fixture','Immutable goal','fixture',$1::text)",[goal]);
  const workforce={enabled:true,glmAccountId:policy.accountId,glmTarget:limit,coordinatorAccountId:policy.accountId,coordinatorModel:policy.model};
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['workforce:'+goal,JSON.stringify(workforce)]);
  const task=async(stage='work',model=policy.model)=>{
    const id=randomUUID();await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,stage,account_id,model,execution)
      VALUES($1::uuid,$2::uuid,$1::text,'Fixture','Preserve this exact requirement and source',$3,'[{"id":"qa","type":"artifact","name":"qa.json"}]',$4,$5,$6,NULL)`,[id,goal,agent,stage,policy.accountId,model]);return id;
  };
  const run=async(taskId:string,model=policy.model)=>{
    const id=randomUUID();await pool.query("INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,model,state) VALUES($1,$2,1,$3,$4,'harness',1,$5,'running')",
      [id,taskId,randomUUID(),policy.accountId,model]);return id;
  };
  const install=()=>pool.query("INSERT INTO settings(id,value) VALUES('model-policy',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[JSON.stringify(policy)]);
  return {goal,workforce,task,run,install};
}
it('routes every new role to exact v2 policy while keeping explicit restrictions and explicit high planner profile',()=>{
  const account={id:policy.accountId,bridge_id:policy.bridgeId,models:[policy.model],enabled:true,allow_unknown:true,provider:'openai'};
  const workforce:any={enabled:true,glmAccountId:'old-account',glmTarget:12,workerAccounts:[],coordinatorAccountId:'old-account',coordinatorModel:'glm-5.3-flash'};
  for(const stage of ['coordinate','plan','work','integrate','verify'])expect(chooseWorkforceRoute({stage},[account],workforce))
    .toEqual({accountId:policy.accountId,model:policy.model});
  expect(chooseWorkforceRoute({allowed_accounts:['old-account']},[account],workforce)).toBeNull();
  expect(chooseWorkforceRoute({},[{...account,quota:{ordinaryUsageAllowed:false}}],workforce)).toBeNull();
  expect(generationTiming(policy.bridgeId,policy.model)).toEqual({requestTimeoutMs:240000,timeout:240000,reasoning:'high'});
  expect(generationTiming(policy.bridgeId,'other').reasoning).toBe('medium');
  const active=Array.from({length:12},()=>({state:'running',account_id:policy.accountId,model:policy.model}));
  expect(workforceCapacityReason({account_id:policy.accountId,model:policy.model},active,workforce)).toBe('WORKFORCE_GLM_CAPACITY');
});
it('SQL rejects malformed v2 authority and a model rename never grants manager authority',async()=>{
  for(const change of [{version:3},{version:'2'},{mode:'glm-only'},{model:'renamed-privileged'},{requestLimit:7},{sessionLimit:13},{requestLimit:'6'},
    {bridgeId:'agentos:api:other'},{accountId:'agentos:muse:other'},{extra:true}]){
    const result=await pool.query(`SELECT COALESCE(${exclusiveModelPolicySql('$1::jsonb')},false) allowed`,[JSON.stringify({...policy,...change})]);
    expect(result.rows[0].allowed).toBe(false);
  }
  const f=await fixture();await f.install();const task=await f.task('coordinate'),run=await f.run(task);
  const check=()=>pool.query(`SELECT ${managerAuthoritySql()} allowed FROM runs r JOIN tasks t ON t.id=r.task_id JOIN accounts a ON a.id=r.account_id WHERE r.id=$1`,[run]);
  expect((await check()).rows[0].allowed).toBe(true);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{coordinatorModel}','\"renamed-privileged\"') WHERE id=$1",['workforce:'+f.goal]);
  expect((await check()).rows[0].allowed).toBe(false);
});
it('maintenance, manager wake and route suggestions cannot rewrite an existing legacy binding',()=>{
  const account={id:policy.accountId,bridge_id:policy.bridgeId,models:[policy.model],enabled:true,allow_unknown:true,provider:'openai'};
  const workforce:any={enabled:true,glmAccountId:policy.accountId,glmTarget:12,workerAccounts:[policy.accountId],coordinatorAccountId:policy.accountId,coordinatorModel:policy.model};
  for(const old of [{account_id:'old-account',model:'glm-5.3-flash'},
    {account_id:policy.accountId,model:'glm-5.3-flash'},
    {account_id:'old-account',model:policy.model}])for(const allowed_accounts of [[],[policy.accountId]]){
    const task={id:randomUUID(),stage:'coordinate',...old,allowed_accounts,execution:{kind:'harness',workforceLane:'glm'}};
    const frozen=JSON.stringify(task);
    expect(chooseWorkforceRoute(task,[account],workforce)).toBeNull();
    expect(managementRoutes({...task,stage:'work'},[account],workforce)).toEqual([]);
    expect(JSON.stringify(task)).toBe(frozen);
  }
  const current={id:randomUUID(),account_id:policy.accountId,model:policy.model,execution:{kind:'harness'}};
  expect(chooseWorkforceRoute(current,[account],workforce)).toEqual({accountId:policy.accountId,model:policy.model});
  const task={...current,stage:'work',allowed_accounts:[policy.accountId]};
  const route={accountId:policy.accountId,model:policy.model};
  expect(resolveSupervisedRoute(task,route,workforce,[account],[])).toEqual({ok:true,route,lane:'glm'});
  expect(resolveSupervisedRoute({...task,model:'glm-5.3-flash'},route,workforce,[account],[]))
    .toMatchObject({ok:false,code:'SUPERVISION_ROUTE_UNAVAILABLE'});
  expect(resolveSupervisedRoute(task,route,workforce,[{...account,bridge_id:'unverified'}],[])).toMatchObject({ok:false});
});
it('twelve analytical/harness sessions share one SQL lane and concurrent thirteenth admission is refused',async()=>{
  const f=await fixture();await f.install();
  for(let i=0;i<11;i++)await f.run(await f.task());
  const tasks=[await f.task(),await f.task()];
  const results=await Promise.allSettled(tasks.map(task=>f.run(task)));
  expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(1);
  expect(String((results.find(result=>result.status==='rejected') as PromiseRejectedResult).reason)).toContain('WORKFORCE_GLM_CAPACITY');
  expect(Number((await pool.query("SELECT count(*) FROM runs WHERE account_id=$1 AND state='running'",[policy.accountId])).rows[0].count)).toBe(12);
});
it('keeps historical GLM source, criteria and attempt identity; v2 refuses a new legacy call or in-place route mutation',async()=>{
  const f=await fixture();const oldTask=await f.task('verify','glm-5.3-flash'),oldRun=await f.run(oldTask,'glm-5.3-flash');
  const before=(await pool.query('SELECT description,criteria,execution,model,account_id FROM tasks WHERE id=$1',[oldTask])).rows[0];
  const identity=(await pool.query('SELECT fence,attempt,model,account_id FROM runs WHERE id=$1',[oldRun])).rows[0];
  await f.install();
  await pool.query("UPDATE runs SET state='cancelling' WHERE id=$1",[oldRun]);
  expect((await pool.query('SELECT description,criteria,execution,model,account_id FROM tasks WHERE id=$1',[oldTask])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT fence,attempt,model,account_id FROM runs WHERE id=$1',[oldRun])).rows[0]).toEqual(identity);
  await expect(f.run(await f.task('verify','glm-5.3-flash'),'glm-5.3-flash')).rejects.toThrow('MODEL_POLICY_GLM_ONLY');
  await expect(pool.query("UPDATE runs SET model='renamed-privileged' WHERE id=$1",[oldRun])).rejects.toThrow('MODEL_POLICY_GLM_ONLY');
});
it('checks native dependencies for the selected v2 account even on an analytical runtime',async()=>{
  const f=await fixture();await f.install();const parent=await f.task(),child=await f.task();
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[child,parent]);
  await expect(f.run(child)).rejects.toThrow('DEPENDENCIES_PENDING');
});
it('a pending historical planner retains account, model, fence, context and attempt instead of switching providers',async()=>{
  await fixture();
  const request=objectiveSchema.parse({message:'Preserve all original requirements and QA.',project:'fixture',workspaces:['test/base'],mode:'plan',idempotencyKey:randomUUID()});
  const {objectiveId}=await submitObjective(request);
  await pool.query("UPDATE planning_runs SET attempt=2,account_id='old-account',model='glm-5.3-flash',context='{\"preserve\":\"original context\"}' WHERE objective_id=$1",[objectiveId]);
  const fields='id,account_id,model,fence,context,attempt,output';
  const before=(await pool.query('SELECT '+fields+' FROM planning_runs WHERE objective_id=$1',[objectiveId])).rows[0];
  expect(await claimPlanning()).toBeNull();
  expect((await pool.query('SELECT '+fields+' FROM planning_runs WHERE objective_id=$1',[objectiveId])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT state FROM planning_runs WHERE objective_id=$1',[objectiveId])).rows[0].state).toBe('waiting_quota');
  await pool.query("UPDATE planning_runs SET output='{\"persisted\":true}',state='queued',next_attempt_at=now() WHERE objective_id=$1",[objectiveId]);
  const prepared=await claimPlanning();
  expect(prepared).toMatchObject({id:before.id,model:'glm-5.3-flash',account:{id:'old-account'},output:{persisted:true}});
});
it('planner and chat count the shared twelve sessions even if account max_concurrency is larger',async()=>{
  const f=await fixture();await f.install();
  await pool.query('UPDATE accounts SET max_concurrency=99,allow_unknown=true WHERE id=$1',[policy.accountId]);
  const runs=[];for(let i=0;i<12;i++)runs.push(await f.run(await f.task()));
  await expect(chooseChatAccount()).rejects.toMatchObject({code:'ACCOUNT_UNAVAILABLE'});
  const request=objectiveSchema.parse({message:'New isolated DeepSeek occurrence.',project:'fixture',workspaces:['test/base'],mode:'plan',idempotencyKey:randomUUID()});
  const {objectiveId}=await submitObjective(request);
  expect(await claimPlanning()).toBeNull();
  await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[runs[0]]);
  expect((await chooseChatAccount()).model).toBe(policy.model);
  await pool.query('UPDATE planning_runs SET next_attempt_at=now() WHERE objective_id=$1',[objectiveId]);
  expect(await claimPlanning()).toMatchObject({model:policy.model,account:{id:policy.accountId}});
});

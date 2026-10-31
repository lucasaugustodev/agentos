import {afterAll,afterEach,beforeAll,beforeEach,expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installManagement} from '../server/management.js';
import {writeArtifact,completeRun,validateTask,schedule} from '../server/service.js';
import {registerWorkerValidationPolicy,validationDigest,workerValidationGate} from '../server/worker-validation.js';
import {registerCandidateReadinessMap,bindCandidateReadinessPlan,
  verifyCandidateReadinessAdmission,readCandidateReadinessProof} from '../server/candidate-readiness.js';
import {planSchema} from '../server/objective-contracts.js';

// PostgreSQL owns registrations, frozen admission snapshots and consumer locks.
// Synthetic release receipts are isolated fixture inputs, never production proof.
const expected={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
const required=[{file:'tests/readiness-contract-pg.test.ts',name:'candidate keeps every inherited source'}];
const description='Preserve the inherited source and verify the integrated candidate.';
const sha=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const policy={version:1,enabled:true,project:'agentos',profile:'agentos.validate',stages:['integrate'],required:true,requiredDatabaseTests:required};
const originalCriterion={id:'integrated',label:'Original integration check',type:'json_field',name:'delivery.json',field:'passed',equals:true};
let f:any,owner:PoolClient|undefined;
const originalConfig={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installManagement();
});
beforeEach(async()=>{
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','storage:agentos-workspaces') OR id LIKE 'worker-readiness:%' OR id LIKE 'candidate-readiness:%' OR id LIKE 'worker-validation:%' OR id LIKE 'worker-validation-handoff:%' OR id LIKE 'candidate-validation:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  f={goal:randomUUID(),task:randomUUID(),run:randomUUID(),fence:randomUUID(),context:randomUUID(),revision:randomUUID(),planning:randomUUID()};
  f.plan=planSchema.parse({title:'Readiness authority fixture',summary:'A registered finite validation partition.',
    requirements:[{id:'req-a',description}],incremental:{version:1,selectedRequirementId:'req-a',selectedRequirementIds:['req-a'],completedRequirementIds:[],pendingRequirementIds:[]},
    tasks:[{key:'integrate',title:'Integrate candidate',description:'Preserve all source changes and execute the registered controls.',
      agentId:'engineering.integration',stage:'integrate',baseWorkspace:'agentos/base',dependencies:[],covers:['req-a'],criteria:[originalCriterion]}]});
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Readiness authority','Isolated regression','agentos',$1::text)",[f.goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,$2,'fixture','execute','executing',1)",
    [f.goal,JSON.stringify({project:'agentos',planningStrategy:'incremental',requirements:[description]})]);
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence) VALUES($1,$2,0,'fixture','completed',$3)",[f.planning,f.goal,randomUUID()]);
  await pool.query("INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status) VALUES($1,$2,1,$3,$4,'applied')",
    [f.revision,f.goal,f.planning,JSON.stringify(f.plan)]);
});
afterEach(async()=>{
  if(owner){await owner.query('SELECT pg_advisory_unlock_all()');owner.release();owner=undefined;}
  Object.assign(config,originalConfig);
});
afterAll(()=>pool.end());
async function setting(id:string,value:any){
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',[id,JSON.stringify(value)]);
}
async function registered(){
  f.policy=await registerWorkerValidationPolicy(f.goal,policy,{channel:'api-token'});
  f.map={version:1,enabled:true,profile:'agentos.validate',policyRegistrationId:f.policy.registrationId,
    requirements:[{id:'req-a',digest:validationDigest(description)}],obligations:[
      {id:'source-preserved',requirements:['req-a'],kind:'local',name:'local-evidence.json',field:'passed',equals:true},
      {id:'database-control',requirements:['req-a'],kind:'registered_ci',...required[0]}]};
  f.registration=await registerCandidateReadinessMap(f.goal,f.map,{channel:'operator-session'});
  await adoptConsumer();
  f.bound=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,f.plan,{consumerRelease:expected});});
  f.criterion=f.bound.tasks[0].criteria.find((criterion:any)=>criterion.type==='candidate_readiness');
  expect(f.criterion).toBeDefined();
}
async function adoptConsumer(release=expected){
  if(owner){await owner.query('SELECT pg_advisory_unlock_all()');owner.release();owner=undefined;}
  owner=await pool.connect();await owner.query('SELECT pg_advisory_lock(4280027),pg_advisory_lock(4280050)');
  const backendPid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  const readiness={service:'agentos-workers',pid:12345,...release,startupId:randomUUID(),readyAt:new Date(Date.now()-1000).toISOString(),
    locks:[{key:4280027,backendPid},{key:4280050,backendPid}]};
  await setting('worker-readiness:agentos-workers',readiness);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('WORKER_SERVICE_UPDATED',NULL,$1)",[JSON.stringify({
    service:'agentos-workers',candidate:release.candidate,pid:readiness.pid,readiness,
    stopped:{unit:'agentos-workers',previousPid:12344,activeState:'inactive',mainPid:0,oldPidGone:true,cgroupEmpty:true,checkedAt:new Date(Date.now()-2000).toISOString()}})]);
}
async function insertTask(criteria:any[]=[originalCriterion]){
  if(criteria.some(c=>c.type==='candidate_readiness'))
    await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(f.bound)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers,recovery,plan_revision)
    VALUES($1,$2,'r1-integrate','Integrate candidate','Frozen authority fixture','engineering.integration','running','integrate',$3,$4,1,'["req-a"]','{"mode":"automatic"}',$5)`,
    [f.task,f.goal,JSON.stringify(criteria),JSON.stringify({kind:'harness',workspace:'agentos/oreadinessauthority',baseWorkspace:'agentos/base',readOnlySource:false}),1]);
  return (await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
}
async function historicalRun(withPolicy:boolean){
  if(!withPolicy)await pool.query('DELETE FROM settings WHERE id=$1',['worker-validation:'+f.goal]);
  await insertTask();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",
    [f.run,f.task,f.fence,f.context]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Frozen legacy fixture')",[f.context,f.run]);
  const snapshot=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0].instruction_snapshot;
  expect(snapshot.workerValidationContract).toBe(1);
  // Simulate a new criterion attached to an old task. Never rewrite the immutable
  // run snapshot: a current task row is not authority to upgrade its old attempt.
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.task,JSON.stringify([f.criterion])]);
  const local=await writeArtifact(f.run,f.fence,'local-evidence.json',JSON.stringify({passed:true}));
  await writeArtifact(f.run,f.fence,'integration-readiness.json',JSON.stringify({version:1,state:'ready_for_ci',
    localChecks:[{id:'source-preserved',status:'passed',name:'local-evidence.json',digest:local.digest}],
    controllerChecks:{profile:'agentos.validate',status:'pending_controller_ci',obligationsDigest:f.criterion.obligationsDigest,tests:required},
    blockingFailures:[],limitations:[]}));
  await writeArtifact(f.run,f.fence,'candidate.json',JSON.stringify({baseDigest:sha('[]'),candidateDigest:sha('[]'),files:[]}));
  await writeArtifact(f.run,f.fence,'validation-intent.json',JSON.stringify({version:1,profile:'agentos.validate',reason:'Run the exact registered candidate checks.'}));
  await completeRun(f.run,f.fence,'Only local readiness is claimed.');
  return snapshot;
}

it('binds authenticated finite controls without mutating the planner input or accepting a predeclared readiness criterion',async()=>{
  const before=structuredClone(f.plan);await registered();expect(f.plan).toEqual(before);
  expect(f.bound.tasks[0].criteria).toContainEqual(f.criterion);
  await expect(tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,f.bound);})).rejects.toMatchObject({code:'READINESS_PREDECLARED'});
});
it.each([false,true])('does not upgrade a historical admission with policy=%s from a current readiness criterion or ready artifact',async(withPolicy)=>{
  await registered();const before=await historicalRun(withPolicy);
  if(!withPolicy)expect(await workerValidationGate(pool,f.run)).toMatchObject({required:false,state:'legacy'});
  await expect(readCandidateReadinessProof(pool,f.run)).rejects.toMatchObject({code:'READINESS_ADMISSION_UNBOUND'});
  const verdict=await validateTask(f.task);expect(verdict.passed).toBe(false);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.run])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0].instruction_snapshot).toEqual(before);
});
it('does not treat current schema and API as adoption of the actual worker consumer',async()=>{
  await registered();const task=await insertTask(f.bound.tasks[0].criteria);
  await owner!.query('SELECT pg_advisory_unlock_all()');owner!.release();owner=undefined;
  await pool.query("DELETE FROM settings WHERE id='worker-readiness:agentos-workers'");
  await pool.query("DELETE FROM events WHERE kind='WORKER_SERVICE_UPDATED'");
  await expect(tx(async db=>{await lock(db);return verifyCandidateReadinessAdmission(db,task,{consumerRelease:expected});})).rejects.toBeDefined();
  await adoptConsumer({...expected,digest:'b'.repeat(64)});
  await expect(tx(async db=>{await lock(db);return verifyCandidateReadinessAdmission(db,task,{consumerRelease:expected});})).rejects.toBeDefined();
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
});
it('admits the exact adopted worker only while its PostgreSQL owners remain live',async()=>{
  await registered();const task=await insertTask(f.bound.tasks[0].criteria);
  await tx(async db=>{await lock(db);await verifyCandidateReadinessAdmission(db,task,{consumerRelease:expected});});
  await owner!.query('SELECT pg_advisory_unlock(4280050)');
  await expect(tx(async db=>{await lock(db);return verifyCandidateReadinessAdmission(db,task,{consumerRelease:expected});})).rejects.toBeDefined();
});
it('does not trust a copied readiness setting without its authenticated registration event',async()=>{
  await registered();
  await pool.query("DELETE FROM events WHERE payload->>'registrationId'=$1",[f.registration.registrationId]);
  await expect(tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,f.plan);})).rejects.toBeDefined();
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-readiness:'+f.goal])).rows[0].value).toEqual(f.registration);
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(0);
});
it.each(['replace','revoke'])('does not rebind a readiness map after its CI policy is changed by %s',async(mode)=>{
  await registered();
  await registerWorkerValidationPolicy(f.goal,{...policy,enabled:mode!=='revoke'},{channel:'api-token'});
  await expect(tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,f.plan);})).rejects.toBeDefined();
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-readiness:'+f.goal])).rows[0].value).toEqual(f.registration);
});
it.each(['missing-ci','extra-ci','duplicate-ci','foreign-requirement','changed-requirement'])('rejects %s mapping without replacing the authenticated finite controls',async(mode)=>{
  await registered();const bad=structuredClone(f.map);
  if(mode==='missing-ci')bad.obligations=[bad.obligations[0],{...bad.obligations[0],id:'other-local',name:'other-local.json'}];
  if(mode==='extra-ci')bad.obligations.push({...bad.obligations[1],id:'other-database',name:'unregistered database case'});
  if(mode==='duplicate-ci')bad.obligations.push({...bad.obligations[1],id:'duplicate-database'});
  if(mode==='foreign-requirement')bad.obligations[0].requirements=['invented-requirement'];
  if(mode==='changed-requirement')bad.requirements[0].digest=validationDigest('A smaller and different requirement.');
  const before=(await pool.query('SELECT count(*)::int n FROM events')).rows[0].n;
  await expect(registerCandidateReadinessMap(f.goal,bad,{channel:'operator-session'})).rejects.toBeDefined();
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-readiness:'+f.goal])).rows[0].value).toEqual(f.registration);
  expect((await pool.query('SELECT count(*)::int n FROM events')).rows[0].n).toBe(before);
});
it('keeps the registered CI wire digest when the same finite test set is mapped in a different order',async()=>{
  await registered();
  const tests=[required[0],{file:'tests/readiness-contract-pg.test.ts',name:'candidate rejects stale database authority'}];
  const registration=await registerWorkerValidationPolicy(f.goal,{...policy,requiredDatabaseTests:tests},{channel:'api-token'});
  const map={...f.map,policyRegistrationId:registration.registrationId,obligations:[f.map.obligations[0],
    ...[...tests].reverse().map((test,index)=>({id:'database-'+index,requirements:['req-a'],kind:'registered_ci',...test}))]};
  await registerCandidateReadinessMap(f.goal,map,{channel:'operator-session'});
  const bound=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,f.plan,{consumerRelease:expected});});
  const criterion:any=bound.tasks[0].criteria.find((item:any)=>item.type==='candidate_readiness');
  expect(criterion?.obligationsDigest).toBe(validationDigest(tests));
  expect(registration.policy.requiredDatabaseTests).toEqual(tests);
});
it('keeps an unadmitted readiness task waiting while dispatching an independent task in the same scheduler pass',async()=>{
  await registered();await insertTask(f.bound.tasks[0].criteria);
  config.bridgeHost='test-only';config.remoteExecution=true;
  await setting('capacity',{maxRuns:4,browserSlots:1});
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('readiness-fixture','Fixture account','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,16,true,'readiness-fixture')`);
  await pool.query("UPDATE tasks SET state='ready',attempt_count=0,priority=100,account_id='readiness-fixture',model='glm-5.3-flash' WHERE id=$1",[f.task]);
  const sibling=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,priority,account_id,model)
    VALUES($1,$2,'independent','Independent work','Existing available work','qa.backend','ready','work',$3,$4,50,'readiness-fixture','glm-5.3-flash')`,
    [sibling,f.goal,JSON.stringify([{id:'proof',type:'artifact',name:'proof.json',label:'Required proof'}]),
      JSON.stringify({kind:'harness',workspace:'agentos/oindependent',baseWorkspace:'agentos/base',profile:'code',writePaths:['src/']})]);
  await owner!.query('SELECT pg_advisory_unlock_all()');owner!.release();owner=undefined;
  await pool.query("DELETE FROM settings WHERE id='worker-readiness:agentos-workers'");
  const runs=await schedule();
  expect(runs).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[runs[0]])).rows[0].task_id).toBe(sibling);
  expect((await pool.query('SELECT state,attempt_count,criteria FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'waiting_resource',attempt_count:0,criteria:f.bound.tasks[0].criteria});
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
});

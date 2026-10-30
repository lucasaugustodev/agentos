import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate,tx,lock,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {validateTask} from '../server/service.js';
import {sourceContractPlanningFailure} from '../server/source-output-validation.js';
import {routeVerificationPlanningRecovery} from '../server/verification-planning-recovery.js';
import {reconcilePlanning} from '../server/planner.js';
import {recoverStoppedTasks} from '../server/recovery.js';
import {ContinuationSupervisor,resumeFailedHarness} from '../server/continuation.js';
import {detectSupervisionCases} from '../server/supervision.js';

const hash=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const winner={path:'src/winner.ts',sha256:hash('accepted source')},feature={path:'src/new.ts',sha256:hash('new source')};
const digest=(files:any[])=>hash(JSON.stringify(files.map(f=>[f.path,f.sha256]).sort((a,b)=>a[0].localeCompare(b[0]))));
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
  await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'validation-receipt:%' OR id='autonomy:source-planning'");await seedRegistry();});
afterAll(()=>pool.end());
async function artifact(run:string,name:string,value:any){
  const id=randomUUID(),bytes=Buffer.from(JSON.stringify(value));
  await writeFile(path.join(config.dataDir,'artifacts',id),bytes,{flag:'wx'});
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,'application/json',$4,$5)",[id,run,name,hash(bytes),bytes.length]);
  return {id,digest:hash(bytes)};
}
async function fixture(kind='base_missing'){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
  const contract={version:1,required:[{path:feature.path,kind:'file'}],preserve:[kind==='base_missing'
    ?{path:'source-planning/base',kind:'directory'}:kind==='base_hash'?{...winner,kind:'file',sha256:hash('incorrect planned base')}
    :{path:winner.path,kind:'file'}]};
  const execution={kind:'harness',workspace:'source-planning/deliver',sourceWorkspace:'source-planning/base',sourceContract:contract};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Source recovery','Preserve source','source-planning',$1::text)",[goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{}','test','execute','active',1)",[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,max_attempts,stage,execution)
    VALUES($1,$2,'deliver','Source delivery','Implement','qa.backend','submitted','[]',1,3,'work',$3)`,[task,goal,JSON.stringify(execution)]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,summary,finished_at) VALUES($1,$2,1,'submitted',$3,'harness',1,'success claim',now())",[run,task,fence]);
  const files=[kind==='regression'?{...winner,sha256:hash('bad edit')}:winner,...(kind==='required_missing'?[]:[feature])];
  const candidate=await artifact(run,'candidate.json',{baseDigest:digest([winner]),candidateDigest:digest(files),baseFiles:[winner],files});
  const proof={runId:run,workspace:execution.workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
  return {goal,task,run,fence,execution,candidate,proof};
}
const route=(task:string,run:string)=>tx(async db=>{await lock(db);return routeVerificationPlanningRecovery(db,task,run);});
const count=async(sql:string,args:any[]=[])=>Number((await pool.query(sql,args)).rowCount);

it.each(['base_missing','base_hash'])('routes verified %s to one planner across concurrent recovery consumers without replaying the writer',async kind=>{
  const f=await fixture(kind),frozen=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0];
  const result=await validateTask(f.task);expect(result.passed).toBe(false);
  const diagnosis=sourceContractPlanningFailure(result.checks,f.run);
  expect(diagnosis).toMatchObject({runId:f.run,artifactId:f.candidate.id,artifactDigest:f.candidate.digest,baseDigest:digest([winner])});
  expect(diagnosis!.failures[0].code).toBe(kind==='base_missing'?'SOURCE_OUTPUT_PRESERVATION_BASE_MISSING':'SOURCE_OUTPUT_PRESERVATION_BASE_HASH_MISMATCH');
  const accepted=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows;
  expect(await count('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).toBe(1);
  await Promise.all([recoverStoppedTasks(),route(f.task,f.run),reconcilePlanning(),detectSupervisionCases()]);
  await reconcilePlanning();await recoverStoppedTasks();
  const feedback=(await pool.query('SELECT * FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows;
  expect(feedback).toHaveLength(1);expect(feedback[0].trigger_key).toBe('run:'+f.run);
  expect(feedback[0].description).toContain('Correct the preserve contract');expect(feedback[0].description).toContain(digest([winner]));
  expect(await count('SELECT 1 FROM planning_runs WHERE objective_id=$1',[f.goal])).toBe(1);
  expect(await count('SELECT 1 FROM recoveries WHERE source_run_id=$1',[f.run])).toBe(0);
  expect(await count('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).toBe(1);
  expect((await pool.query('SELECT state,attempt_count,execution FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toMatchObject({state:'submitted',attempt_count:1,execution:{continuation:{recoveryOwner:'planner',reasonCode:'SOURCE_CONTRACT_PLANNING_REQUIRED'}}});
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows).toEqual(accepted);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(frozen);
  expect(await count("SELECT 1 FROM events WHERE entity_id=$1 AND kind='SOURCE_CONTRACT_PLANNING_ROUTED'",[f.task])).toBe(1);
});
it('continuation reconciles a missing projection without inspecting or resuming an impossible source contract',async()=>{
  const f=await fixture();await validateTask(f.task);
  await pool.query("UPDATE tasks SET execution=execution-'continuation' WHERE id=$1",[f.task]);
  const inspect=vi.fn();await new ContinuationSupervisor(inspect).scan();
  expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.task])).rows[0].execution.continuation.recoveryOwner).toBe('planner');
  expect(await count('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).toBe(1);
  expect(await count('SELECT 1 FROM recoveries WHERE source_run_id=$1',[f.run])).toBe(0);
});
it('resume admission refuses the same impossible contract even with fresh physical stop proof and still rejects absent stop proof',async()=>{
  const f=await fixture();await validateTask(f.task);
  await expect(resumeFailedHarness(f.run,f.fence,{...f.proof,process:{...f.proof.process,stopped:false}})).rejects.toThrow('STOP_UNCONFIRMED');
  await expect(resumeFailedHarness(f.run,f.fence,f.proof)).rejects.toThrow('SOURCE_CONTRACT_PLANNING_REQUIRED');
  expect(await count('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('submitted');
});
it.each(['required_missing','regression'])('keeps ordinary %s on the existing worker repair path',async kind=>{
  const f=await fixture(kind),result=await validateTask(f.task);expect(result.passed).toBe(false);
  expect(sourceContractPlanningFailure(result.checks,f.run)).toBeNull();
  expect(await recoverStoppedTasks()).toEqual([f.task]);
  expect(await count('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).toBe(0);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('retry_scheduled');
});
it.each(['paused','cancelled','manual','autonomy','lease','cancel_event','user_owner'])('does not replace %s ownership or fall through to retry after a base rejection',async guard=>{
  const f=await fixture();
  if(['paused','cancelled'].includes(guard))await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,guard]);
  if(guard==='manual')await pool.query(`UPDATE tasks SET recovery='{"mode":"manual"}' WHERE id=$1`,[f.task]);
  if(guard==='autonomy')await pool.query(`INSERT INTO settings VALUES('autonomy:source-planning','{"enabled":false}')`);
  if(guard==='lease')await pool.query("INSERT INTO leases(resource,run_id) VALUES('source-planning-held',$1)",[f.run]);
  if(guard==='cancel_event')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(guard==='user_owner')await pool.query(`UPDATE tasks SET execution=execution||'{"continuation":{"owner":"user"}}' WHERE id=$1`,[f.task]);
  expect((await validateTask(f.task)).passed).toBe(false);
  expect(await route(f.task,f.run)).toBe(false);await recoverStoppedTasks();await reconcilePlanning();
  expect(await count('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).toBe(0);
  expect(await count('SELECT 1 FROM planning_runs WHERE objective_id=$1',[f.goal])).toBe(0);
  expect(await count('SELECT 1 FROM recoveries WHERE source_run_id=$1',[f.run])).toBe(0);
  expect(await count('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).toBe(1);
});
it('a newer live attempt remains untouched by reconciliation of the rejected older contract',async()=>{
  const f=await fixture();await validateTask(f.task);
  const feedback=(await pool.query('SELECT * FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows;
  const next=randomUUID();await pool.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1",[f.task]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,2,'running',$3,'harness',1)",[next,f.task,randomUUID()]);
  const frozen=(await pool.query('SELECT * FROM runs WHERE id=$1',[next])).rows;
  expect(await route(f.task,f.run)).toBe(false);await recoverStoppedTasks();await reconcilePlanning();
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[next])).rows).toEqual(frozen);
  expect((await pool.query('SELECT * FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows).toEqual(feedback);
  expect(await count('SELECT 1 FROM planning_runs WHERE objective_id=$1',[f.goal])).toBe(0);
});
it('does not reinterpret historical text or a worker claim as canonical base failure evidence',async()=>{
  const f=await fixture();
  await artifact(f.run,'worker-claim.json',{sourceOutput:{schema:1,code:'SOURCE_OUTPUT_PRESERVATION_BASE_MISSING'}});
  const checks=[{id:'source-output',type:'source_contract',passed:false,detail:'SOURCE_OUTPUT_PRESERVATION_BASE_MISSING'}];
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'historical-validator')",[randomUUID(),f.task,f.run,JSON.stringify(checks)]);
  const before=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows;
  expect(await route(f.task,f.run)).toBe(false);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows).toEqual(before);
  expect(await count('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).toBe(0);
});

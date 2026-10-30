import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,lock,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {routeVerificationPlanningRecovery} from '../server/verification-planning-recovery.js';
import {ContinuationSupervisor,routeVerificationRepair} from '../server/continuation.js';
import {reconcilePlanning} from '../server/planner.js';
import {recoverStoppedTasks} from '../server/recovery.js';
import {detectSupervisionCases} from '../server/supervision.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id='autonomy:verification-owner'");await seedRegistry();});
afterAll(async()=>{await pool.end();});
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),writer=randomUUID(),writerRun=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Material QA','Preserve failed evidence','verification-owner',$1::text)`,[goal]);
  await pool.query(`INSERT INTO objectives(id,request,request_hash,mode,status,revision)
    VALUES($1,'{}','test','execute','active',1)`,[goal]);
  await pool.query(`INSERT INTO settings VALUES($1,'{"enabled":true}')`,['supervision:'+goal]);
  for(const [id,readonly,state] of [[writer,false,'accepted'],[task,true,'submitted']] as const)
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
      VALUES($1::uuid,$2,$1::text,'QA','Preserve the contract','qa.backend',$3,$4,'[]',$5,1)`,[id,goal,state,readonly?'verify':'work',
      JSON.stringify({kind:'harness',readOnlySource:readonly,workspace:'verification-owner/'+id,sourceWorkspace:'verification-owner/'+writer})]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[task,writer]);
  for(const [id,taskId] of [[run,task],[writerRun,writer]])await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,summary,finished_at)
      VALUES($1,$2,1,$3,'harness',1,'submitted','Material test failed',now())`,[id,taskId,randomUUID()]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,false,'[{"type":"json_field","passed":false}]','test')`,[randomUUID(),task,run]);
  return {goal,task,run,writer,writerRun};
}
const route=(task:string,run?:string)=>tx(async db=>{await lock(db);return routeVerificationPlanningRecovery(db,task,run);});
it('concurrent planner and continuation consume one failed QA through the existing feedback owner',async()=>{
  const f=await fixture();
  const before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.writer])).rows[0];
  await Promise.all([routeVerificationRepair(f.task),reconcilePlanning(),recoverStoppedTasks(),detectSupervisionCases()]);
  expect((await pool.query('SELECT * FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
  expect((await pool.query('SELECT * FROM planning_runs WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('replanning');
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.writer])).rows[0]).toEqual(before);
  const qa=(await pool.query('SELECT state,attempt_count,execution FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(qa).toMatchObject({state:'submitted',attempt_count:1,execution:{continuation:{runId:f.run,
    owner:'controller',recoveryOwner:'planner',reasonCode:'VERIFICATION_PLANNING_REQUIRED'}}});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_PLANNING_ROUTED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind IN('VERIFICATION_REPAIR_ROUTED','VERIFICATION_RECHECK_QUEUED')")).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM recoveries WHERE source_run_id=$1',[f.run])).rowCount).toBe(0);
});
it('a replanning scan repairs the stale GOAL_NOT_ACTIVE projection without reopening the goal or executing QA',async()=>{
  const f=await fixture();await pool.query("UPDATE goals SET state='replanning' WHERE id=$1",[f.goal]);
  await pool.query(`UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1`,[f.task,JSON.stringify({continuation:{
    state:'needs_action',owner:'controller',runId:f.run,routingCode:'GOAL_NOT_ACTIVE',nextAt:'2020-01-01'}})]);
  const inspect=vi.fn();await new ContinuationSupervisor(inspect).scan();
  expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('replanning');
  expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.task])).rows[0].execution.continuation)
    .toMatchObject({recoveryOwner:'planner',runId:f.run});
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(1);
});
it.each(['live','lease'])('records evidence but waits for the %s writer before planning',async kind=>{
  const f=await fixture();
  if(kind==='live')await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.writerRun]);
  else await pool.query("INSERT INTO leases(resource,run_id) VALUES('verification-owner-writer',$1)",[f.writerRun]);
  await routeVerificationRepair(f.task);await reconcilePlanning();
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM planning_runs WHERE objective_id=$1',[f.goal])).rowCount).toBe(0);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  if(kind==='lease')expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.writerRun])).rowCount).toBe(1);
});
it.each(['MODEL_IDLE_TIMEOUT','MODEL_TRANSPORT_ERROR','HARNESS_DELIVERY_INVALID_REPORT'])
('preserves planner recovery for a readonly technical %s failure',async code=>{
  const f=await fixture();
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[f.task]);
  await pool.query("UPDATE runs SET state='failed',error_code=$2 WHERE id=$1",[f.run,code]);
  await reconcilePlanning();
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM planning_runs WHERE objective_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_PLANNING_ROUTED' AND entity_id=$1",[f.task])).rowCount).toBe(0);
});
it('does not use the technical fallback to bypass a manual recovery policy',async()=>{
  const f=await fixture();
  await pool.query(`UPDATE tasks SET state='recovery_blocked',recovery='{"mode":"manual"}' WHERE id=$1`,[f.task]);
  await pool.query("UPDATE runs SET state='failed',error_code='MODEL_IDLE_TIMEOUT' WHERE id=$1",[f.run]);
  await reconcilePlanning();
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(0);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
});
it.each(['paused','cancelled','autonomy','manual','workforce','cancel_event','newer_run','qa_lease','review_only','transient','user_owner'])
('does not claim material QA recovery for %s',async kind=>{
  const f=await fixture();
  if(['paused','cancelled'].includes(kind))await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,kind]);
  if(kind==='autonomy'||kind==='manual')await pool.query("INSERT INTO settings VALUES('autonomy:verification-owner',$1) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",[JSON.stringify({enabled:kind==='manual'?undefined:false})]);
  if(kind==='manual')await pool.query(`UPDATE tasks SET recovery='{"mode":"manual"}' WHERE id=$1`,[f.task]);
  if(kind==='workforce')await pool.query(`INSERT INTO settings VALUES($1,'{"enabled":true}')`,['workforce:'+f.goal]);
  if(kind==='cancel_event')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(kind==='newer_run')await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'harness',1,'running')`,[randomUUID(),f.task,randomUUID()]);
  if(kind==='qa_lease')await pool.query("INSERT INTO leases(resource,run_id) VALUES('verification-owner-qa',$1)",[f.run]);
  if(kind==='review_only')await pool.query(`UPDATE acceptances SET checks='[{"type":"review","passed":false}]' WHERE run_id=$1`,[f.run]);
  if(kind==='transient')await pool.query("UPDATE runs SET state='failed',error_code='MODEL_TRANSPORT_ERROR' WHERE id=$1",[f.run]);
  if(kind==='user_owner')await pool.query(`UPDATE tasks SET execution=execution||'{"continuation":{"owner":"user"}}' WHERE id=$1`,[f.task]);
  expect(await route(f.task,f.run)).toBe(false);
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(0);
});

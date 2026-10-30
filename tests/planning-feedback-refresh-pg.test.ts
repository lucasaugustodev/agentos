import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {submitObjective,claimPlanning,feedbackObjective} from '../server/planner.js';
import {refreshQueuedPlanningFeedback} from '../server/planning-feedback-refresh.js';
import {accountBridge} from '../server/bridge.js';

beforeAll(()=>migrate());
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("UPDATE accounts SET enabled=true,provider='codex',model='test-model',models='[\"test-model\"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true");
});
afterEach(()=>vi.restoreAllMocks());afterAll(()=>pool.end());
async function fixture(){
 const {objectiveId}=await submitObjective({message:'Prepare and independently verify the example feature.',project:'test',workspaces:['test/base'],mode:'execute',idempotencyKey:randomUUID()});
 const run=(await pool.query('SELECT * FROM planning_runs WHERE objective_id=$1',[objectiveId])).rows[0];
 return {goal:objectiveId,run,feedback:async()=>(await pool.query('SELECT feedback FROM planning_runs WHERE id=$1',[run.id])).rows[0].feedback};
}
const events=async(goal:string)=>(await pool.query("SELECT payload FROM events WHERE kind='PLANNING_FEEDBACK_REFRESHED' AND entity_id=$1 ORDER BY id",[goal])).rows;
const refresh=async(id:string)=>tx(async db=>{await lock(db);return refreshQueuedPlanningFeedback(db,id);});

it('includes feedback arriving after queueing in the claimed context before any model request',async()=>{
 const f=await fixture(),rpc=vi.spyOn(accountBridge,'request');
 await feedbackObjective(f.goal,'new-proof','The latest candidate has a newly observed defect; preserve its counterexample.');
 const feedback=(await pool.query('SELECT id,description FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows[0];
 const claimed=await claimPlanning();
 expect(claimed.id).toBe(f.run.id);expect(claimed.feedback.feedbackIds).toEqual([feedback.id]);
 expect(claimed.feedback.reports).toEqual([feedback.description]);expect(await f.feedback()).toEqual(claimed.feedback);
 expect(rpc).not.toHaveBeenCalled();expect((await events(f.goal))[0].payload.addedFeedbackIds).toEqual([feedback.id]);
});

it('preserves prior reports, IDs, source and artifact proofs while appending new IDs exactly once in stable order',async()=>{
 const f=await fixture(),oldId=randomUUID(),a='00000000-0000-4000-8000-000000000001',b='00000000-0000-4000-8000-000000000002';
 const prior={feedbackIds:[oldId],reports:['Existing report','Accepted artifacts are inputs, not final proof.'],
  sourceOverrides:{'test/base':'test/accepted'},artifacts:[{id:randomUUID(),digest:'a'.repeat(64),name:'candidate.json'}],
  partialDeliveries:[{taskId:randomUUID()}],replaceTaskIds:[randomUUID()],acceptedCount:2,incrementalAdvance:{proof:'preserved'}};
 await pool.query('UPDATE planning_runs SET feedback=$2 WHERE id=$1',[f.run.id,JSON.stringify(prior)]);
 await pool.query(`INSERT INTO objective_feedback(id,objective_id,trigger_key,description,created_at) VALUES
  ($1,$3,'second','Second canonical report','2026-01-02'),($2,$3,'first','First canonical report','2026-01-02')`,[b,a,f.goal]);
 await refresh(f.run.id);await refresh(f.run.id);
 expect(await f.feedback()).toEqual({...prior,feedbackIds:[oldId,a,b],reports:[...prior.reports,'First canonical report','Second canonical report']});
 expect(await events(f.goal)).toHaveLength(1);
});

it('does not reattach feedback already consumed explicitly or superseded by a later completed plan',async()=>{
 const f=await fixture(),explicit=randomUUID(),older=randomUUID(),fresh=randomUUID(),completed=randomUUID();
 await pool.query(`INSERT INTO objective_feedback(id,objective_id,trigger_key,description,created_at) VALUES
  ($1,$4,'explicit','Explicitly consumed','2026-01-03'),($2,$4,'older','Historical context','2026-01-01'),($3,$4,'fresh','New counterexample','2026-01-04')`,[explicit,older,fresh,f.goal]);
 await pool.query(`INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state,feedback,created_at)
  VALUES($1,$2,0,'historical',$3,'completed',$4,'2026-01-02')`,[completed,f.goal,randomUUID(),JSON.stringify({feedbackIds:[explicit]})]);
 const claimed=await claimPlanning();expect(claimed.feedback.feedbackIds).toEqual([fresh]);
 expect(claimed.feedback.reports).toEqual(['New counterexample']);
});

it('refreshes a quota-waiting context without starting a model and does not duplicate reports across retries',async()=>{
 const f=await fixture(),rpc=vi.spyOn(accountBridge,'request');await pool.query('UPDATE accounts SET enabled=false');
 await feedbackObjective(f.goal,'quota-proof','New evidence arriving while no provider account is eligible.');
 expect(await claimPlanning()).toBeNull();
 const once=await f.feedback();expect(once.feedbackIds).toHaveLength(1);
 await pool.query('UPDATE planning_runs SET next_attempt_at=now() WHERE id=$1',[f.run.id]);
 expect(await claimPlanning()).toBeNull();expect(await f.feedback()).toEqual(once);expect(await events(f.goal)).toHaveLength(1);
 expect((await pool.query('SELECT state,attempt FROM planning_runs WHERE id=$1',[f.run.id])).rows[0]).toEqual({state:'waiting_quota',attempt:0});
 expect(rpc).not.toHaveBeenCalled();
});

it.each(['running','output','reserved','preparing','prepared','uncertain','applied'] as const)
 ('never rewrites feedback or preparation evidence for %s',async kind=>{
  const f=await fixture(),prior={feedbackIds:[],reports:['Frozen prior context'],artifacts:[{name:'retained'}]};
  await pool.query('UPDATE planning_runs SET feedback=$2 WHERE id=$1',[f.run.id,JSON.stringify(prior)]);
  if(kind==='running')await pool.query("UPDATE planning_runs SET state='running' WHERE id=$1",[f.run.id]);
  else if(kind==='output')await pool.query("UPDATE planning_runs SET output='{\"persisted\":true}' WHERE id=$1",[f.run.id]);
  else await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[`planning-source-preparation:${f.run.id}:0`,JSON.stringify({state:kind,proof:'immutable test evidence'})]);
  await feedbackObjective(f.goal,'later','Fresh evidence must wait for the existing preparation recovery gate.');
  const before=(await pool.query('SELECT feedback,output,state FROM planning_runs WHERE id=$1',[f.run.id])).rows[0];
  const settingsBefore=(await pool.query('SELECT value FROM settings WHERE id=$1',[`planning-source-preparation:${f.run.id}:0`])).rows;
  expect(await refresh(f.run.id)).toBeUndefined();
  expect((await pool.query('SELECT feedback,output,state FROM planning_runs WHERE id=$1',[f.run.id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',[`planning-source-preparation:${f.run.id}:0`])).rows).toEqual(settingsBefore);
  expect(await events(f.goal)).toHaveLength(0);
 });

it('does not conflate two distinct feedback identities just because their text is equal',async()=>{
 const f=await fixture();await feedbackObjective(f.goal,'occurrence-a','The same external failure was observed again.');
 await feedbackObjective(f.goal,'occurrence-b','The same external failure was observed again.');
 const claimed=await claimPlanning();expect(new Set(claimed.feedback.feedbackIds).size).toBe(2);
 expect(claimed.feedback.reports).toHaveLength(2);
});

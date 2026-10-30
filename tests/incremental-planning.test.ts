import { beforeAll,beforeEach,afterEach,afterAll,it,expect,vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool,migrate,tx,lock } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import { objectiveSchema,validatePlan } from '../server/objective-contracts.js';
import { compileDelivery,bindDeliveryFeedback,incrementalEligible,verifiedBatch } from '../server/incremental-planning.js';
import { submitObjective,feedbackObjective,claimPlanning,applyPlannedResult,reconcilePlanning,PlanningEngine } from '../server/planner.js';
import { refreshGoal } from '../server/service.js';
import {planningPreparationFixture} from './fixtures/planning-preparation.js';

const request = () => objectiveSchema.parse({message:'Corrigir dois defeitos independentes e verificar a candidata acumulada.',project:'test',
  workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',sourceContractVersion:1,requirements:['O endpoint deve persistir a alteração correta.','A interface deve exibir o progresso correto.'],idempotencyKey:randomUUID()});
const decision = (requirementId='req-1') => ({kind:'delivery' as const,requirementId,title:'Corrigir persistência',action:'Reproduzir a falha de persistência e corrigir sua causa com teste discriminante.',
  verification:'Executar regressão que falha na base e passa na candidata, preservando os demais controles.',agentId:'engineering.backend.node',
  sourceContract:{version:1 as const,required:[],preserve:[],noSourceReason:'Synthetic transition fixture emits diagnostic evidence without claiming a real source implementation.'}});
beforeAll(()=>migrate());
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%'");
  await seedRegistry();
  await pool.query(`UPDATE accounts SET provider='codex',model='test-model',models='["test-model"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true`);
  config.bridgeHost='fixture';config.remoteExecution=true;
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(()=>pool.end());

// Explicitly synthetic acceptance fixtures: these tests validate controller
// transitions, never count toward the externally measured autonomy cohort.
async function acceptBatch(goalId:string,revision:number,candidate=true){
  for(const t of (await pool.query('SELECT id FROM tasks WHERE goal_id=$1 AND plan_revision=$2',[goalId,revision])).rows){
    const id=randomUUID();
    await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)`,[id,t.id,randomUUID()]);
    await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[t.id]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test-fixture')",[randomUUID(),t.id,id]);
    if(candidate)await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,2)",[randomUUID(),id,'a'.repeat(64)]);
  }
}
async function start(){
  const input=request();const {objectiveId}=await submitObjective(input);const run=await claimPlanning();
  const plan=compileDelivery(decision(),input);await applyPlannedResult(run,plan);return {input,objectiveId,plan,run};
}
it('compiles two concrete stages while preserving every pending requirement',async()=>{
  const input=request(),plan=compileDelivery(decision(),input);
  validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
  expect(plan.tasks.map(t=>t.stage)).toEqual(['work','verify']);
  expect(plan.incremental).toEqual({version:1,selectedRequirementId:'req-1',completedRequirementIds:[],pendingRequirementIds:['req-2']});
  expect(plan.requirements.map(r=>r.description)).toEqual(input.requirements);
  expect(()=>compileDelivery({...decision(),requirementId:'invented'},input)).toThrow();
  expect(()=>compileDelivery({...decision(),accountId:'other'},input)).toThrow();
  expect(incrementalEligible({...input,mode:'plan'},undefined,true)).toBe(false);
  expect(incrementalEligible({...input,workspaces:['test/base','test/other']},undefined,true)).toBe(false);
});
it('does not turn an accepted partial lot into a completed objective',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
});
async function addHistoricalCancellation(goalId:string){
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,plan_revision,criteria)
    VALUES($1,$2,'obsolete-cancelled','Old bounded attempt','Historical cancellation','qa.backend','cancelled','work',0,'[]')`,[randomUUID(),goalId]);
}
it('does not derive objective cancellation from a cancelled task in an older revision',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);await addHistoricalCancellation(objectiveId);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
});
it('reconciles the historical cancellation defect without rewriting tasks, acceptances or runs',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);await addHistoricalCancellation(objectiveId);
  await pool.query("UPDATE goals SET state='cancelled' WHERE id=$1",[objectiveId]);
  await pool.query("UPDATE objectives SET status='cancelled' WHERE id=$1",[objectiveId]);
  const runs=(await pool.query('SELECT id,fence,state FROM runs WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1) ORDER BY id',[objectiveId])).rows;
  await reconcilePlanning();
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('replanning');
  expect((await pool.query('SELECT id,fence,state FROM runs WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1) ORDER BY id',[objectiveId])).rows).toEqual(runs);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_STATE_RECONCILED' AND entity_id=$1",[objectiveId])).rowCount).toBe(1);
  expect((await pool.query("SELECT state FROM tasks WHERE goal_id=$1 AND key='obsolete-cancelled'",[objectiveId])).rows[0].state).toBe('cancelled');
});
it('does not reopen an objective whose current revision was actually cancelled',async()=>{
  const {objectiveId}=await start();await addHistoricalCancellation(objectiveId);
  await pool.query("UPDATE tasks SET state='cancelled' WHERE goal_id=$1",[objectiveId]);
  await pool.query("UPDATE goals SET state='cancelled' WHERE id=$1",[objectiveId]);
  await pool.query("UPDATE objectives SET status='cancelled' WHERE id=$1",[objectiveId]);
  await reconcilePlanning();
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('cancelled');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_STATE_RECONCILED' AND entity_id=$1",[objectiveId])).rowCount).toBe(0);
});
it('does not let feedback stranded on a failed planning run block a verified advance forever',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);
  await feedbackObjective(objectiveId,'stranded-counterexample','Counterexample antigo a considerar no proximo plano.');
  await reconcilePlanning();
  const stranded=await claimPlanning();
  await pool.query("UPDATE planning_runs SET state='failed' WHERE id=$1",[stranded.id]);
  await reconcilePlanning();
  // A failed planner alone queues nothing and must not silently advance.
  expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rowCount).toBe(0);
  // Fresh evidence drives a later completed revision, superseding the stranded feedback.
  await feedbackObjective(objectiveId,'fresh-counterexample','Counterexample novo: reproduzir e corrigir na candidata acumulada.');
  await reconcilePlanning();
  const run=await claimPlanning();
  await pool.query("UPDATE planning_runs SET state='running' WHERE id=$1",[run.id]);
  await applyPlannedResult(run,compileDelivery(decision(),input,plan,false,run.feedback.reports));
  await acceptBatch(objectiveId,2);
  await reconcilePlanning();
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);
  expect(queued[0].trigger_key).toMatch(/^incremental:/);
  expect(queued[0].feedback.incrementalAdvance).toBeTruthy();
});
it('keeps the accumulated accepted candidate as replan source when a later work revision fails',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);await reconcilePlanning();
  const run=await claimPlanning();await applyPlannedResult(run,compileDelivery(decision('req-2'),input,plan,true));
  const deliver=(await pool.query("SELECT id,execution FROM tasks WHERE goal_id=$1 AND plan_revision=2 AND stage='work'",[objectiveId])).rows[0];
  expect(deliver.execution.workspace).toContain('r2');
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'failed',$3,'harness',1)",[randomUUID(),deliver.id,randomUUID()]);
  await pool.query("UPDATE tasks SET state='recovery_blocked',attempt_count=1 WHERE id=$1",[deliver.id]);
  await reconcilePlanning();
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);
  expect(queued[0].feedback.sourceOverrides['test/base']).toContain('r1-deliver');
  expect(queued[0].feedback.sourceOverrides['test/base']).not.toContain('r2');
});
it('carries immutable artifacts from a rejected submitted delivery without certifying its partial source',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);await reconcilePlanning();
  const planning=await claimPlanning();await applyPlannedResult(planning,compileDelivery(decision('req-2'),input,plan,true));
  const task=(await pool.query("SELECT id FROM tasks WHERE goal_id=$1 AND plan_revision=2 AND stage='work'",[objectiveId])).rows[0];
  const runId=randomUUID(),artifactId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,summary) VALUES($1,$2,1,'submitted',$3,'harness',1,'Original input exposed a candidate regression.')",[runId,task.id,randomUUID()]);
  await pool.query("UPDATE tasks SET state='submitted',attempt_count=1 WHERE id=$1",[task.id]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[]','test-fixture')",[randomUUID(),task.id,runId]);
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'changes.patch','text/plain',$3,2)",[artifactId,runId,'b'.repeat(64)]);
  await reconcilePlanning();await reconcilePlanning();
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);
  expect(queued[0].feedback.artifacts).toContainEqual({id:artifactId,name:'changes.patch',digest:'b'.repeat(64)});
  expect(queued[0].feedback.sourceOverrides['test/base']).toContain('r1-deliver');
  expect(queued[0].feedback.sourceOverrides['test/base']).not.toContain('r2');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[task.id])).rows[0].state).toBe('submitted');
  expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[runId])).rows[0].passed).toBe(false);
});
it('queues exactly one successor across repeated/concurrent reconciliation and carries its verified source',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);
  await Promise.all(Array.from({length:4},()=>reconcilePlanning()));
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);
  expect(queued[0].feedback.sourceOverrides['test/base']).toContain('r1-deliver');
  expect((await pool.query('SELECT count(*) FROM tasks WHERE goal_id=$1',[objectiveId])).rows[0].count).toBe('2');
});
it('prioritizes new counterexamples over advancement and preserves the accumulated candidate',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);
  const report='Counterexample: 2026-02-30T12:00:00Z is not a valid calendar date; accepted must be 0.';
  await feedbackObjective(objectiveId,'external-calendar-case',report);
  await Promise.all(Array.from({length:4},()=>reconcilePlanning()));
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows;
  expect(queued).toHaveLength(1);expect(queued[0].trigger_key).toMatch(/^feedback:/);
  expect(queued[0].feedback.incrementalAdvance).toBeUndefined();
  expect(queued[0].feedback.sourceOverrides['test/base']).toContain('r1-deliver');
  expect(queued[0].feedback.artifacts).toHaveLength(1);
  const run=await claimPlanning();
  // A failed feedback planner has not addressed the evidence. It must not let
  // the old accepted lot silently advance while awaiting its own recovery.
  await pool.query("UPDATE planning_runs SET state='failed' WHERE id=$1",[run.id]);
  await reconcilePlanning();
  expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rowCount).toBe(0);
  await pool.query("UPDATE planning_runs SET state='running' WHERE id=$1",[run.id]);
  await expect(applyPlannedResult(run,compileDelivery(decision(),input,plan))).rejects.toMatchObject({code:'REQUIRED_CONTEXT'});
  const repaired=compileDelivery(decision(),input,plan,false,run.feedback.reports);
  expect(repaired.incremental?.completedRequirementIds).toEqual([]);
  for(const task of repaired.tasks)expect(task.description).toContain(report);
  expect(bindDeliveryFeedback(repaired,run.feedback.reports)).toEqual(repaired);
  await applyPlannedResult(run,repaired);
  expect((await pool.query('SELECT count(*) FROM acceptances WHERE passed=true')).rows[0].count).toBe('2');
});
it('leaves active and uncertain attempts unchanged while feedback waits for a safe boundary',async()=>{
  const {objectiveId}=await start();
  const task=(await pool.query("SELECT id FROM tasks WHERE goal_id=$1 AND stage='work'",[objectiveId])).rows[0];
  const runId=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'running',$3,'harness',1)",[runId,task.id,fence]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[task.id]);
  await feedbackObjective(objectiveId,'live-counterexample','Reproduce the externally reported invalid-date case before accepting.');
  for(const state of ['running','reconciling']){
    await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[runId,state]);
    await reconcilePlanning();
    expect((await pool.query('SELECT state,fence FROM runs WHERE id=$1',[runId])).rows[0]).toEqual({state,fence});
    expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rowCount).toBe(0);
  }
});
it('cannot complete with unconsumed feedback and reopens a completed objective without erasing evidence',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);await reconcilePlanning();
  const run=await claimPlanning();await applyPlannedResult(run,compileDelivery(decision('req-2'),input,plan,true));
  await acceptBatch(objectiveId,2);
  await feedbackObjective(objectiveId,'before-completion','A concrete regression remains in a prior requirement. Reproduce and repair it.');
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
  await reconcilePlanning();const repair=await claimPlanning();
  for(const state of ['queued','running','waiting_quota','failed']){
    await pool.query('UPDATE planning_runs SET state=$2 WHERE id=$1',[repair.id,state]);
    await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
    expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).not.toBe('completed');
  }
  await pool.query("UPDATE planning_runs SET state='running' WHERE id=$1",[repair.id]);
  const latest=(await pool.query('SELECT plan FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 1',[objectiveId])).rows[0].plan;
  await applyPlannedResult(repair,compileDelivery(decision('req-2'),input,latest,false,repair.feedback.reports));
  await acceptBatch(objectiveId,3);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('completed');
  await feedbackObjective(objectiveId,'after-completion','A new external counterexample invalidates the earlier passing report.');
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
  expect((await pool.query('SELECT count(*) FROM acceptances WHERE passed=true')).rows[0].count).toBe('6');
});
it('completes when the only unconsumed feedback was superseded by a later completed plan',async()=>{
  const {objectiveId,input,plan}=await start();
  // Legacy zombie: old feedback referenced only by a terminally failed run,
  // with completed plans created after it. This mirrors the deployed repair.
  await feedbackObjective(objectiveId,'legacy-zombie','Old counterexample superseded by completed planning revisions.');
  const zombie=(await pool.query("SELECT id FROM objective_feedback WHERE objective_id=$1 AND trigger_key='legacy-zombie'",[objectiveId])).rows[0];
  await pool.query("UPDATE objective_feedback SET created_at=now()-interval '2 days' WHERE id=$1",[zombie.id]);
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,feedback,created_at) VALUES($1,$2,1,'feedback:legacy-zombie','failed',$3,$4,now()-interval '2 days')",[randomUUID(),objectiveId,randomUUID(),JSON.stringify({feedbackIds:[zombie.id],reports:['legacy'],acceptedCount:0})]);
  await acceptBatch(objectiveId,1);await reconcilePlanning();
  const run=await claimPlanning();await applyPlannedResult(run,compileDelivery(decision('req-2'),input,plan,true));
  await acceptBatch(objectiveId,2);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('completed');
});
it('fails visibly instead of silently truncating feedback beyond the existing task context limit',()=>{
  expect(()=>compileDelivery(decision(),request(),undefined,false,['x'.repeat(21000)])).toThrowError(expect.objectContaining({code:'REQUIRED_CONTEXT'}));
});
it('will not advance from a model claim without acceptance or from missing candidate evidence',async()=>{
  const {objectiveId}=await start();
  await pool.query("UPDATE tasks SET state='accepted' WHERE goal_id=$1",[objectiveId]);
  await reconcilePlanning();expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
  await acceptBatch(objectiveId,1,false);await reconcilePlanning();
  expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rowCount).toBe(0);
});
it('rechecks acceptance before applying a successor and rejects forged completed coverage',async()=>{
  const {objectiveId,input,plan}=await start();await acceptBatch(objectiveId,1);await reconcilePlanning();
  const run=await claimPlanning();const next=compileDelivery(decision('req-2'),input,plan,true);
  await pool.query('UPDATE acceptances SET passed=false WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)',[objectiveId]);
  await expect(applyPlannedResult(run,next)).rejects.toMatchObject({code:'STALE_PLAN'});
  await pool.query('UPDATE acceptances SET passed=true WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)',[objectiveId]);
  await applyPlannedResult(run,next);
  const final=(await pool.query('SELECT covers FROM tasks WHERE goal_id=$1 AND plan_revision=2 AND stage=\'verify\'',[objectiveId])).rows[0];
  expect(final.covers).toEqual(['req-1','req-2']);
  await acceptBatch(objectiveId,2);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('completed');
});
it('rejects an initial plan that claims previous deliveries without proof',async()=>{
  const input=request();await submitObjective(input);const run=await claimPlanning();
  const plan=compileDelivery(decision(),input);
  plan.incremental!.pendingRequirementIds=[];plan.incremental!.completedRequirementIds=['req-2'];plan.tasks[1].covers=['req-1','req-2'];
  await expect(applyPlannedResult(run,plan)).rejects.toMatchObject({code:'PLAN_INVALID'});
});
it('resumes a fenced planning claim after executor loss without applying its stale response',async()=>{
  const input=request();await submitObjective(input);const old=await claimPlanning();
  await pool.query("UPDATE planning_runs SET heartbeat_at=now()-interval '2 minutes' WHERE id=$1",[old.id]);
  await reconcilePlanning();const current=await claimPlanning();
  await expect(applyPlannedResult(old,compileDelivery(decision(),input))).rejects.toMatchObject({code:'STALE_PLAN'});
  await applyPlannedResult(current,compileDelivery(decision(),input));
  expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('2');
});
it('recovers a confirmed timeout with the small contract and reuses persisted output during preparation',async()=>{
  let generated=0,prepared=0;const preparation=planningPreparationFixture();
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
    if(method==='memory.search')return {items:[]};
    if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='generate'){
      expect(p.maxOutputTokens).toBe(8192);expect(JSON.parse(p.messages[0].content)).not.toHaveProperty('previousPlan');
      if(++generated===1)throw Object.assign(Error('confirmed'),{code:'PROVIDER_TIMEOUT'});
      return {message:{content:[{type:'text',text:JSON.stringify(decision())}]}};
    }
    if(method==='worker.prepare'&&++prepared===1)throw Object.assign(Error('busy'),{code:'SOURCE_BUSY'});
    const inspection=preparation(method,p);if(inspection!==undefined)return inspection;
    throw Error('ranking fallback');
  });
  await submitObjective(request());const engine=new PlanningEngine();
  for(let i=0;i<3;i++){await engine.execute(await claimPlanning(),new AbortController().signal);await pool.query('UPDATE planning_runs SET next_attempt_at=now()');}
  expect(generated).toBe(2);expect(prepared).toBe(2);expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('2');
});
it('does not dispatch a truncated JSON fragment or retry an uncertain termination',async()=>{
  vi.spyOn(accountBridge,'request').mockImplementation(async(method)=>{
    if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='generate')return {message:{content:[{type:'text',text:'{"requirementId":"req-1"'}]}};
    throw Error('ranking fallback');
  });
  await submitObjective(request());const engine=new PlanningEngine();await engine.execute(await claimPlanning(),new AbortController().signal);
  expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('0');
  await pool.query('UPDATE planning_runs SET next_attempt_at=now()');
  vi.mocked(accountBridge.request).mockRejectedValue(Object.assign(Error('uncertain'),{code:'STOP_UNCONFIRMED'}));
  await engine.execute(await claimPlanning(),new AbortController().signal);
  expect((await pool.query('SELECT state FROM planning_runs')).rows[0].state).toBe('failed');
});
it('honors paused goals between lots',async()=>{
  const {objectiveId}=await start();await acceptBatch(objectiveId,1);await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[objectiveId]);
  await reconcilePlanning();expect(await claimPlanning()).toBeNull();
});

it('repairs the first invalid agent after nineteen transport failures without consuming semantic retries',async()=>{
  let generated=0;const preparation=planningPreparationFixture();
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
    if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];
    const inspection=preparation(method,p);if(inspection!==undefined)return inspection;
    if(method==='generate'){
      const schema=JSON.parse(p.messages[0].content).schema;
      const deliverySchema=schema.anyOf[0];
      expect(deliverySchema.properties.agentId.enum).toContain('engineering.backend.node');
      expect(deliverySchema.properties.agentId.enum).not.toContain('engineering.backend.nodejs');
      return {message:{content:[{type:'text',text:JSON.stringify({...decision(),agentId:++generated===1?'engineering.backend.nodejs':'engineering.backend.node'})}]}};
    }
    throw Error('ranking fallback');
  });
  await submitObjective(request());
  await pool.query(`UPDATE planning_runs SET attempt=19,recovery_counts='{"transient":19}'`);
  const engine=new PlanningEngine();await engine.execute(await claimPlanning(),new AbortController().signal);
  const repaired=(await pool.query('SELECT * FROM planning_runs')).rows[0];
  expect(repaired.state).toBe('queued');expect(repaired.recovery_counts).toEqual({transient:19,invalid_plan:1});
  await pool.query('UPDATE planning_runs SET next_attempt_at=now()');
  await engine.execute(await claimPlanning(),new AbortController().signal);
  expect((await pool.query('SELECT count(*) FROM tasks')).rows[0].count).toBe('2');
});

it('persists a child before its parent atomically and rejects cyclic or absent parents',async()=>{
  const input=request();await submitObjective(input);const run=await claimPlanning();
  const plan=compileDelivery(decision(),input);plan.tasks[0].parentKey='verify';
  const agents=(await pool.query('SELECT * FROM agents')).rows;
  const missing=structuredClone(plan);missing.tasks[0].parentKey='unknown';
  expect(()=>validatePlan(missing,input,agents,[])).toThrow();
  const cyclic=structuredClone(plan);cyclic.tasks[1].parentKey='deliver';
  expect(()=>validatePlan(cyclic,input,agents,[])).toThrow();
  await applyPlannedResult(run,plan);
  const rows=(await pool.query('SELECT id,key,parent_task_id FROM tasks')).rows;
  expect(rows.find(t=>t.key.endsWith('-deliver')).parent_task_id).toBe(rows.find(t=>t.key.endsWith('-verify')).id);
});

it('compiles and advances a parallel incremental window with disjoint writePaths, integration and verification',async()=>{
  const input=objectiveSchema.parse({message:'Corrigir três módulos independentes com verificação acumulada.',project:'test',
    workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',sourceContractVersion:1,
    requirements:['O módulo A deve rejeitar entradas nulas.','O módulo B deve preservar precisão decimal.','O módulo C deve registrar auditoria.'],
    idempotencyKey:randomUUID()});
  const agents=(await pool.query('SELECT * FROM agents')).rows;
  expect(()=>compileDelivery({deliveries:[{...decision('req-1'),writePaths:['src/a.ts']},{...decision('req-2'),title:'Corrigir precisão',writePaths:['src/a.ts']}]},input)).toThrow();
  expect(()=>compileDelivery({deliveries:[{...decision('req-1')},{...decision('req-2'),title:'Corrigir precisão',writePaths:['src/b.ts']}]},input)).toThrow();
  const windowPlan=compileDelivery({deliveries:[
    {...decision('req-1'),writePaths:['src/a.ts']},
    {...decision('req-2'),title:'Corrigir precisão decimal',writePaths:['src/b.ts']},
  ]},input);
  validatePlan(windowPlan,input,agents,[]);
  expect(windowPlan.tasks.map(t=>t.stage)).toEqual(['work','work','integrate','verify']);
  expect(windowPlan.incremental).toEqual({
    version:1,
    selectedRequirementId:'req-1',
    selectedRequirementIds:['req-1','req-2'],
    completedRequirementIds:[],
    pendingRequirementIds:['req-3'],
  });
  const {objectiveId}=await submitObjective(input);
  await applyPlannedResult(await claimPlanning(),windowPlan);
  await acceptBatch(objectiveId,1);
  await reconcilePlanning();
  const nextRun=await claimPlanning();
  expect(nextRun.feedback.sourceOverrides['test/base']).toContain('r1-integrate');
  const finalPlan=compileDelivery(decision('req-3'),input,windowPlan,true);
  expect(finalPlan.incremental?.completedRequirementIds).toEqual(['req-1','req-2']);
  expect(finalPlan.tasks.find(t=>t.stage==='verify')?.covers).toEqual(['req-1','req-2','req-3']);
  await applyPlannedResult(nextRun,finalPlan);
  await acceptBatch(objectiveId,2);
  await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('completed');
});

it('rebinds unaccepted agentos objectives to the current published agentos-source-workspace on feedback',async()=>{
  const staleSource='agentos/src1111111111111111',publishedSource='agentos/src2222222222222222';
  await pool.query(
    `INSERT INTO settings VALUES('agentos-source-workspace',$1::jsonb)
     ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`,
    [JSON.stringify({workspace:publishedSource})],
  );
  const input=objectiveSchema.parse({
    message:'Evoluir o AgentOS preservando a baseline publicada.',
    project:'agentos',
    workspaces:[staleSource],
    mode:'execute',
    planningStrategy:'incremental',
    sourceContractVersion:1,
    requirements:['O módulo A deve rejeitar entradas nulas.','O módulo B deve preservar precisão decimal.'],
    idempotencyKey:randomUUID(),
  });
  const {objectiveId}=await submitObjective(input);
  await applyPlannedResult(await claimPlanning(),compileDelivery(decision('req-1'),input));
  const deliver=(await pool.query("SELECT id FROM tasks WHERE goal_id=$1 AND stage='work'",[objectiveId])).rows[0];
  const runId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'failed',$3,'harness',1)",[runId,deliver.id,randomUUID()]);
  await pool.query("UPDATE tasks SET state='recovery_blocked',attempt_count=1 WHERE id=$1",[deliver.id]);
  await feedbackObjective(objectiveId,'rebind-published-source','Retomar na nova release publicada após correção do runtime.');
  await reconcilePlanning();
  const queued=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[objectiveId])).rows[0];
  expect(queued.feedback.acceptedCount).toBe(0);
  expect(queued.feedback.sourceOverrides[staleSource]).toBe(publishedSource);
});

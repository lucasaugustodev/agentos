import { beforeAll, beforeEach, afterAll, it, expect, vi, afterEach } from 'vitest';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate } from '../server/db.js';
import { actionSchema } from '../server/supervision-actions.js';
import { resolveSupervisedRoute } from '../server/supervised-route.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import {
  installSupervision,
  detectSupervisionCases,
  readSupervisionFailureHistory,
  wakeSupervisors,
  recordSupervisionDecisions,
  applySupervisionDecisions,
  runSupervisorJobs,
  reconcileLostAnalyticalSupervisors,
  closeSupervisorJobs,
  resumeSupervisedWork,
} from '../server/supervision.js';
import { continuationSignature } from '../server/supervision-handoff.js';
import { retryProgressSignature,requestProgressReplan } from '../server/retry-progress.js';
import { repeatedWithoutProgress, routeVerificationRepair } from '../server/continuation.js';
import { supervisionInputs, mergeSupervisorInputs } from '../server/supervision-context.js';
import { supervisionEvidenceFingerprint } from '../server/supervision-wait.js';
import { hash, saveMemory, assembleContext } from '../server/memory.js';
import { repairStoppedManagerContext } from '../server/manager-context-repair.js';
import { managerTextHash,managerContextVersion } from '../server/manager-context.js';
import { tx } from '../server/db.js';
import { requestAdmissionSupervision } from '../server/admission-supervision.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { schedule, writeArtifact, completeRun, startRun, validateTask } from '../server/service.js';
import { WorkerEngine } from '../server/worker.js';
import { readObjectiveFrontier } from '../server/work-frontier.js';
import {
  installManagement,
  applyManagementAction,
  recordManagementReviews,
  recordManagementTransitions,
  managementSnapshot,
} from '../server/management.js';

beforeAll(async () => {
  await migrate();
  await installBacklogPlanner();
  await installSupervision();
  await installManagement();
});
beforeEach(async () => {
  config.bridgeHost = 'test-only';
  config.remoteExecution = true;
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterEach(() => {
  vi.restoreAllMocks();
  config.bridgeHost = '';
  config.remoteExecution = false;
});
afterAll(async () => {
  await closeSupervisorJobs();
  await pool.end();
});
async function enableManager(f: any) {
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'management:' + f.goal,
    JSON.stringify({
      enabled: true,
      intervalSeconds: 120,
      enabledAt: new Date(Date.now() - 1000).toISOString(),
    }),
  ]);
}

it('persists attention only from committed valid management reviews and keeps that controller ledger out of the prompt',async()=>{
 const f=await fixture();await enableManager(f);await detectSupervisionCases();
 const caseId=(await pool.query("SELECT id FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows[0].id;
 const now=Date.now();await wakeSupervisors(now);
 const finish=async(management:any)=>{
  const [id]=await schedule(),run=await startRun(id,'focus-service-history');
  await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management}),'application/json');
  await completeRun(run.id,run.fence,'Manager result for attention history',{});await validateTask(f.coordinator);
  await recordManagementReviews();await recordManagementReviews();return run.id;
 };
 const invalid=await finish({summary:'',actions:[]});
 expect((await pool.query("SELECT receipts FROM management_reviews WHERE run_id=$1",[invalid])).rows[0].receipts)
  .toContainEqual(expect.objectContaining({code:'MANAGEMENT_FORMAT'}));
 await wakeSupervisors(now+120001);
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.managerFocusServiceHistory)
  .toEqual({version:1,lastReviewed:{}});
 const valid=await finish({summary:'Review exact failed delivery evidence.',capacityReason:'No verified replacement exists yet.',actions:[]});
 const eventId=(await pool.query("SELECT id::text id FROM events WHERE kind='MANAGEMENT_REVIEWED' AND payload->>'runId'=$1",[valid])).rows[0].id;
 await wakeSupervisors(now+240002);
 const state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(state.managerFocusServiceHistory).toEqual({version:1,lastReviewed:{[caseId]:eventId}});
 expect((await pool.query('SELECT description FROM tasks WHERE id=$1',[f.coordinator])).rows[0].description)
  .not.toContain('managerFocusServiceHistory');
});

it('refuses a stale prepared wake without overwriting concurrently advanced attention history',async()=>{
 const f=await fixture();await enableManager(f);await detectSupervisionCases();
 const caseId=(await pool.query("SELECT id FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows[0].id;
 const initial={version:1,lastReviewed:{[caseId]:'90071992547409931'}},advanced={version:1,lastReviewed:{[caseId]:'90071992547409932'}};
 await pool.query('INSERT INTO settings VALUES($1,$2)',['management-state:'+f.goal,JSON.stringify({managerFocusServiceHistory:initial})]);
 const before=(await pool.query('SELECT state,description,attempt_count FROM tasks WHERE id=$1',[f.coordinator])).rows[0];
 let enter!:()=>void,release!:()=>void;
 const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
 const wake=wakeSupervisors(Date.now(),async(db,goalId,policy,focus)=>{
  enter();await held;return managementSnapshot(db,goalId,policy,focus);
 });
 try{
  await Promise.race([entered,wake.then(()=>{throw Error('MANAGER_SNAPSHOT_NOT_REACHED');})]);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{managerFocusServiceHistory}',$2::jsonb) WHERE id=$1",
   ['management-state:'+f.goal,JSON.stringify(advanced)]);
 }finally{release();await wake;}
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.managerFocusServiceHistory).toEqual(advanced);
 expect((await pool.query('SELECT state,description,attempt_count FROM tasks WHERE id=$1',[f.coordinator])).rows[0]).toEqual(before);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});

it('coalesces unchanged qualified capacity waits and wakes again for a real new failure fact',async()=>{
  const f=await fixture();await enableManager(f);
  await pool.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1",[f.task]);
  const now=Date.now();await wakeSupervisors(now);
  const runId=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[f.coordinator]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[runId,f.coordinator,fence]);
  await writeArtifact(runId,fence,'coordination.json',JSON.stringify({management:{
    summary:'The consumer has a precise resource wait owned by the existing scheduler.',
    capacityReason:'The existing consumer retains its original required verification.',actions:[],
    capacityAssessment:[{scope:'supervision-test/game',taskIds:[f.task],reason:'resource',
      nextAction:'Admit the existing consumer when its configured resource becomes available.'}],
    nextTransition:{mode:'task',taskId:f.task,expectedAttempt:3,expectedArtifact:'verification.json',
      cause:'The original consumer awaits a configured resource without a new failed execution.',
      hypothesis:'Retain this consumer and verify its exact artifact when the scheduler admits it.'}}}));
  await completeRun(runId,fence,'Qualified wait bound to the existing consumer',{});await validateTask(f.coordinator);
  await recordManagementReviews();
  const reviewed=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
  expect(reviewed.followThrough).toMatchObject({code:'MANAGEMENT_WAIT_CAPACITY',satisfied:true});
  expect(reviewed.receipts.some((r:any)=>r.code==='CAPACITY_ASSESSMENT_REQUIRED')).toBe(false);
  const historical=(await pool.query('SELECT instruction_snapshot,context_id FROM runs WHERE id=$1',[runId])).rows[0];
  await wakeSupervisors(now+120001);
  const quiet=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
  expect(quiet.materialityObservation).toMatchObject({runId,fingerprint:reviewed.wakeMateriality});
  expect(quiet.requestedAt).toBe(reviewed.requestedAt);expect(quiet.reviewedAt).toBe(reviewed.reviewedAt);
  await wakeSupervisors(now+125001);
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.materialityObservation).toEqual(quiet.materialityObservation);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
  await pool.query("UPDATE runs SET error_code='NEW_FUNCTIONAL_FAILURE' WHERE id=$1",[f.run]);
  await pool.query("INSERT INTO events(kind,entity_id,payload,created_at) VALUES('RUN_FAILED',$1,$2,$3)",
    [f.task,JSON.stringify({runId:f.run,code:'NEW_FUNCTIONAL_FAILURE'}),new Date(Date.parse(quiet.materialityObservation.observedAt)+1)]);
  await wakeSupervisors(now+150003);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('ready');
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(2);
  expect((await pool.query('SELECT instruction_snapshot,context_id FROM runs WHERE id=$1',[runId])).rows[0]).toEqual(historical);
});

it('rotates the one manager after three accepted unchanged reviews while preserving both blocked deliveries and all attempts',async()=>{
 const f=await fixture();await enableManager(f);
 const other=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
   SELECT $1,goal_id,'other-delivery','Other delivery',description,agent_id,state,stage,account_id,model,criteria,
     execution||'{"workspace":"supervision-test/other","baseWorkspace":"supervision-test/other-game"}'::jsonb,attempt_count
   FROM tasks WHERE id=$2`,[other,f.task]);
 for(let attempt=1;attempt<=3;attempt++)await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,finished_at)
   VALUES($1,$2,$3,$4,'harness',1,'failed','MODEL_TIMEOUT','Known independent failure',now())`,
   [randomUUID(),other,attempt,randomUUID()]);
 await detectSupervisionCases();
 const before={tasks:(await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[[f.task,other]])).rows,
   runs:(await pool.query('SELECT * FROM runs WHERE task_id=ANY($1::uuid[]) ORDER BY id',[[f.task,other]])).rows,
   cases:(await pool.query('SELECT * FROM supervision_cases WHERE goal_id=$1 ORDER BY id',[f.goal])).rows};
 const started=Date.now();let first:any;
 for(let index=0;index<3;index++){
   await wakeSupervisors(started+index*120001);
   const focus=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.focus;
   first??=focus;expect(focus.caseId).toBe(first.caseId);
   const runs=await schedule();expect(runs).toHaveLength(1);
   expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[runs[0]])).rows[0].task_id).toBe(f.coordinator);
   const run=await startRun(runs[0],'manager-review-quantum');
   await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management:{
     summary:'The delivery gate still requires its stopped repair.',capacityReason:'No independent worker input accepted yet.',actions:[]}}),'application/json');
   await completeRun(run.id,run.fence,'Review without new worker evidence',{});
   await validateTask(f.coordinator);await recordManagementReviews();await recordManagementReviews();
 }
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1",[f.goal])).rows[0].n).toBe(3);
 await wakeSupervisors(started+3*120001);
 const next=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.focus;
 expect(next.caseId).not.toBe(first.caseId);expect(next.scope).not.toBe(first.scope);
 expect(next.selectionReason).toBe('review_quantum');
 expect((await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[[f.task,other]])).rows).toEqual(before.tasks);
 expect((await pool.query('SELECT * FROM runs WHERE task_id=ANY($1::uuid[]) ORDER BY id',[[f.task,other]])).rows).toEqual(before.runs);
 expect((await pool.query('SELECT * FROM supervision_cases WHERE goal_id=$1 ORDER BY id',[f.goal])).rows).toEqual(before.cases);
});

it('detects recurring failures without transferring historical worker input bundles or changing their stored evidence',async()=>{
  const f=await fixture('Preserved original instruction. '.repeat(20000));
  const original=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0].instruction_snapshot;
  expect(JSON.stringify(original).length).toBeGreaterThan(500000);
  const history=await readSupervisionFailureHistory(pool,f.task);
  expect(history.map(run=>run.attempt)).toEqual([3,2,1]);
  expect(history.every(run=>run.error_code==='MODEL_TIMEOUT' && run.summary==='Known failure needing another approach')).toBe(true);
  expect(JSON.stringify(history).length).toBeLessThan(3000);
  expect(history.some(run=>Object.hasOwn(run,'instruction_snapshot') || Object.hasOwn(run,'checkpoint'))).toBe(false);
  await detectSupervisionCases();
  const evidence=(await pool.query('SELECT evidence FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0].evidence;
  expect(evidence.map((entry:any)=>entry.runId)).toEqual(history.map(run=>run.id));
  expect(evidence.every((entry:any)=>entry.code==='MODEL_TIMEOUT')).toBe(true);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0].instruction_snapshot).toEqual(original);
});

it('records a dated manager observation without holding admission during slow frontier reads',async()=>{
  const f=await fixture();await enableManager(f);await detectSupervisionCases();await wakeSupervisors();
  const [runId]=await schedule(),run=await startRun(runId,'readonly-manager-observation');
  await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management:{
    summary:'Current blocked delivery still requires exact accepted support.',capacityReason:'No independent work with accepted input yet.',actions:[]}}));
  await completeRun(run.id,run.fence,'A real accepted manager review',{});await validateTask(f.coordinator);
  const before=(await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[[f.task,f.source]])).rows;
  let enter!:()=>void,release!:()=>void;
  const entered=new Promise<void>(resolve=>enter=resolve),held=new Promise<void>(resolve=>release=resolve);
  const reader:typeof readObjectiveFrontier=async(db,goalId)=>{
    const observation=(await db.query(`SELECT current_setting('transaction_read_only') read_only,
      current_setting('transaction_isolation') isolation,
      (SELECT count(*)::int FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND objid=4280001 AND granted) admission_locks`)).rows[0];
    expect(observation).toEqual({read_only:'on',isolation:'repeatable read',admission_locks:0});
    enter();await held;return readObjectiveFrontier(db,goalId);
  };
  const started=Date.now(),recording=recordManagementReviews(reader);
  try{
    await Promise.race([entered,recording.then(()=>{throw Error('OBSERVATION_NOT_REACHED');})]);
    await tx(async db=>{
      await db.query("SET LOCAL statement_timeout='1000ms'");
      await db.query('SELECT pg_advisory_xact_lock(4280001)');
      expect((await db.query('SELECT count(*)::int n FROM management_reviews WHERE run_id=$1',[run.id])).rows[0].n).toBe(0);
    });
  }finally{release();await recording;}
  await recordManagementReviews();
  const frontier=(await pool.query('SELECT value FROM settings WHERE id=$1',['work-frontier:'+f.goal])).rows[0].value;
  const review=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
  expect(frontier.observedAt).toBe(review.frontierObservedAt);
  expect(Date.parse(frontier.observedAt)).toBeGreaterThanOrEqual(started);
  expect(Date.parse(frontier.observedAt)).toBeLessThanOrEqual(Date.parse(frontier.reviewedAt));
  expect(frontier.complete).toBe(false);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1",[f.goal])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[[f.task,f.source]])).rows).toEqual(before);
});

async function acceptedManagerTransition(f:any,extra:Record<string,unknown>={}) {
  await enableManager(f);await detectSupervisionCases();await wakeSupervisors();
  const [runId]=await schedule(),run=await startRun(runId,'manager-transition-consumption');
  const c=(await pool.query('SELECT id FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0];
  await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({
    management:{summary:'Resume the bounded failed step and preserve its exact proof.',
      capacityReason:'The accepted decision must be consumed before requesting another review.',actions:[],
      nextTransition:{mode:'task',taskId:f.task,expectedAttempt:3,expectedArtifact:'verification.json',
        cause:'Repeated timeout requires the accepted revised execution.',
        hypothesis:'Bound the failed sub-step and rerun the original verification.'}},
    supervision:{actions:[{caseId:c.id,kind:'revise',diagnosis:'A bounded sub-step will distinguish the timeout cause.',
      instruction:'Reproduce the blocked sub-step with a bounded deadline, retain failed output, and execute the original verification.'}]},
    ...extra}));
  await completeRun(run.id,run.fence,'Accepted decision before actual retry',{});
  await validateTask(f.coordinator);
  return run;
}

it('binds an accepted manager transition before its supervision decision resumes the next attempt',async()=>{
  const f=await fixture(),run=await acceptedManagerTransition(f);
  await recordSupervisionDecisions();
  await Promise.all([recordManagementTransitions(),recordManagementTransitions()]);
  const bound=(await pool.query('SELECT * FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rows[0];
  expect(bound).toMatchObject({task_id:f.task,expected_attempt:3,receipt:{code:'TRANSITION_BOUND'}});
  expect((await pool.query('SELECT 1 FROM management_reviews WHERE run_id=$1',[run.id])).rowCount).toBe(0);
  await applySupervisionDecisions(async()=>f.proof());
  expect(await resumeSupervisedWork(async()=>f.proof())).toEqual([f.task]);
  const dispatched=await schedule();
  expect((await pool.query('SELECT task_id FROM runs WHERE id=ANY($1::uuid[])',[dispatched])).rows).toEqual([{task_id:f.task}]);
  expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0].attempt_count).toBe(4);
  await recordManagementReviews();await recordManagementTransitions();
  expect((await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run.id])).rows[0].receipts)
    .toContainEqual(bound.receipt);
  expect((await pool.query('SELECT * FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rows).toEqual([bound]);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_TRANSITION_BOUND' AND payload->>'transitionId'=$1",[bound.id])).rowCount).toBe(1);
  expect((await pool.query('SELECT criteria FROM tasks WHERE id=$1',[f.task])).rows[0].criteria).toEqual(f.criteria);
});

it('binds the exact applied decision even when twelve older reviews occupy the bounded consumer batch',async()=>{
  const f=await fixture(),run=await acceptedManagerTransition(f);
  for(let index=0;index<12;index++) {
    const goal=randomUUID(),task=randomUUID(),olderRun=randomUUID(),fence=randomUUID();
    await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Older objective','Pending review','supervision-test',$2)",[goal,goal]);
    await pool.query('INSERT INTO settings VALUES($1,$2)',[
      'management:'+goal,JSON.stringify({enabled:true,enabledAt:new Date(Date.now()-60000).toISOString()})]);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
      SELECT $1,$2,'coordinate','Older manager','Retained old review',agent_id,'running','coordinate',criteria,1 FROM tasks WHERE id=$3`,[task,goal,f.coordinator]);
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,created_at)
      VALUES($1,$2,1,$3,'harness',1,'running',now()-interval '30 seconds')`,[olderRun,task,fence]);
    await writeArtifact(olderRun,fence,'coordination.json',JSON.stringify({management:{
      summary:'Older accepted review pending consumption.',capacityReason:'No transition proposed in this old response.',actions:[]}}));
    await completeRun(olderRun,fence,'Older review',{});await validateTask(task);
  }
  await recordSupervisionDecisions();await recordManagementTransitions();
  expect((await pool.query('SELECT 1 FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rowCount).toBe(0);
  await applySupervisionDecisions(async()=>f.proof());
  expect((await pool.query('SELECT receipt FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rows[0].receipt)
    .toMatchObject({code:'TRANSITION_BOUND',taskId:f.task});
  expect(await resumeSupervisedWork(async()=>f.proof())).toEqual([f.task]);
  await schedule();
  expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0].attempt_count).toBe(4);
  expect((await pool.query('SELECT count(*)::int n FROM management_reviews')).rows[0].n).toBe(0);
});

it('still rejects genuinely stale accepted transitions without rebinding them to a newer attempt',async()=>{
  const f=await fixture(),run=await acceptedManagerTransition(f);
  await pool.query('UPDATE tasks SET attempt_count=4 WHERE id=$1',[f.task]);
  const before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  await recordManagementTransitions();await recordManagementReviews();
  expect((await pool.query('SELECT expected_attempt,state,receipt FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rows[0])
    .toMatchObject({expected_attempt:3,state:'rejected',receipt:{code:'TRANSITION_STALE',taskId:null}});
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
});

it('persists the transition before a slow analytical observation while ordinary admission remains available',async()=>{
  const f=await fixture(),run=await acceptedManagerTransition(f);
  let enter!:()=>void,release!:()=>void;
  const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
  const recording=recordManagementReviews(async(db,goalId)=>{
    enter();await held;return readObjectiveFrontier(db,goalId);
  });
  try{
    await Promise.race([entered,recording.then(()=>{throw Error('OBSERVATION_NOT_REACHED');})]);
    await tx(async db=>{
      await db.query("SET LOCAL statement_timeout='1000ms'");await db.query('SELECT pg_advisory_xact_lock(4280001)');
      expect((await db.query('SELECT receipt FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rows[0].receipt.code).toBe('TRANSITION_BOUND');
      expect((await db.query('SELECT 1 FROM management_reviews WHERE run_id=$1',[run.id])).rowCount).toBe(0);
    });
    await wakeSupervisors(Date.now()+120001);
    expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
  }finally{release();await recording;}
  await wakeSupervisors(Date.now()+120001);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(2);
  const snapshot=(await pool.query('SELECT description FROM tasks WHERE id=$1',[f.coordinator])).rows[0].description;
  expect(snapshot).toContain('TRANSITION_BOUND');
});

it('consumes an invalid accepted manager response once before permitting the next corrective review',async()=>{
  const f=await fixture(),run=await acceptedManagerTransition(f,{management:{summary:'Missing required fields'}});
  await wakeSupervisors(Date.now()+120001);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
  await recordManagementTransitions();await recordManagementReviews();
  expect((await pool.query('SELECT 1 FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rowCount).toBe(0);
  expect((await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run.id])).rows[0].receipts)
    .toContainEqual(expect.objectContaining({code:'MANAGEMENT_FORMAT'}));
  await wakeSupervisors(Date.now()+120001);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('ready');
});

it('returns the real strategy-key schema error to the next manager without creating a plan or changing worker authority',async()=>{
 const f=await fixture(),run=await acceptedManagerTransition(f,{supervision:{actions:[]},management:{
  summary:'The review proposes an unchanged scoped repair strategy.',capacityReason:'Reuse existing scoped evidence.',actions:[],
  nextTransition:{mode:'plan',scope:'supervision-test/game',sourceRevision:'a'.repeat(64),
   strategyKey:'reutilizar-pacote-aceito-de-preimagem-r167',expectedArtifact:'scope-plan.json',
   cause:'The stopped consumer still requires the same original evidence.',hypothesis:'Inspect existing source proof before requesting a distinct repair.'}}});
 const before=(await pool.query('SELECT state,attempt_count,criteria,execution FROM tasks WHERE id=$1',[f.task])).rows[0];
 await recordManagementTransitions();await recordManagementReviews();
 const receipts=(await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run.id])).rows[0].receipts;
 const error=receipts.find((v:any)=>v.code==='MANAGEMENT_FORMAT');
 expect(error).toMatchObject({issueCount:1,issuesTruncated:false,issues:[{code:'invalid_format',
  path:['nextTransition','strategyKey'],expectedPattern:'^[a-z][a-z_-]{9,99}$'}]});
 expect(JSON.stringify(error)).not.toContain('reutilizar-pacote-aceito-de-preimagem-r167');
 expect((await pool.query('SELECT 1 FROM delivery_transitions WHERE review_run_id=$1',[run.id])).rowCount).toBe(0);
 expect((await pool.query('SELECT state,attempt_count,criteria,execution FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
 await recordManagementReviews();
 expect((await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run.id])).rows[0].receipts).toEqual(receipts);
 await wakeSupervisors(Date.now()+120001);
 const description=(await pool.query('SELECT description FROM tasks WHERE id=$1',[f.coordinator])).rows[0].description;
 const {expandSharedManagerContext}=await import('../server/manager-context-sharing.js');
 const snapshot=expandSharedManagerContext(JSON.parse(description.slice(description.lastIndexOf('\n{"cases":')+1)));
 expect(snapshot.operation.previous.receipts).toContainEqual(error);
});

it('waits for ordinary validation of a submitted manager response instead of replacing its contract',async()=>{
  const f=await fixture();await enableManager(f);await detectSupervisionCases();await wakeSupervisors();
  const [runId]=await schedule(),run=await startRun(runId,'manager-validation');
  await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management:{
    summary:'Await validated review.',capacityReason:'The review is submitted but not accepted yet.',actions:[]}}));
  await completeRun(run.id,run.fence,'Await acceptance',{});
  const before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.coordinator])).rows[0];
  await wakeSupervisors(Date.now()+120001);
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.coordinator])).rows[0]).toEqual(before);
  await validateTask(f.coordinator);await recordManagementReviews();await wakeSupervisors(Date.now()+120001);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('ready');
});

it('rejects a prepared wake when a review was consumed while its analytical snapshot was being assembled',async()=>{
  const f=await fixture();await enableManager(f);
  let enter!:()=>void,release!:()=>void;
  const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
  const wake=wakeSupervisors(Date.now(),async(db,goalId,policy,focus)=>{
    enter();await held;return managementSnapshot(db,goalId,policy,focus);
  });
  const completedRun=randomUUID();
  try{
    await Promise.race([entered,wake.then(()=>{throw Error('MANAGER_SNAPSHOT_NOT_REACHED');})]);
    // A consumer committing an older accepted run does not change the current
    // manager task or requestedAt; the review version must still invalidate this snapshot.
    await pool.query('INSERT INTO settings VALUES($1,$2)',[
      'management-state:'+f.goal,JSON.stringify({runId:completedRun,reviewedAt:new Date().toISOString()})]);
  }finally{release();await wake;}
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
  await wakeSupervisors();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});

it.each([true,false])('routes disk failure by durable preflight proof, leaving no unowned legacy failures: receipt=%s',async(receipt)=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET error_code='DISK_CAPACITY' WHERE task_id=$1",[f.task]);
  if(receipt)await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",[f.task,JSON.stringify({runId:f.run,confirmed:true,code:'DISK_CAPACITY'})]);
  expect(await detectSupervisionCases()).toHaveLength(receipt?0:1);
  expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0].n).toBe(receipt?0:1);
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:receipt?'failed':'recovery_blocked',attempt_count:3});
});

it.each(['oversized','corrupt'])('isolates an %s support handoff and commits other detected cases without partial instructions or repeat events',async(mode)=>{
  const f=await fixture(),caseId=randomUUID(),supportRun=randomUUID(),supportFence=randomUUID(),other=randomUUID();
  // A consumer waiting for delegated support keeps the objective active while
  // its support is genuinely submitted and accepted below.
  await pool.query("UPDATE tasks SET state='recovery_blocked',execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",[f.task]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[f.source]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
    [supportRun,f.source,supportFence]);
  await writeArtifact(supportRun,supportFence,'verification.json',JSON.stringify({passed:true}));
  let corruptId='';
  for(let index=0;index<(mode==='oversized'?4:1);index++){
    const a=await writeArtifact(supportRun,supportFence,'repair-'+index+'.txt',
      mode==='oversized'?Buffer.alloc(7*1024*1024,65+index):Buffer.from('Original verified evidence'));
    corruptId=a.id;
  }
  await completeRun(supportRun,supportFence,'Accepted support with preserved artifacts',{});await validateTask(f.source);
  if(mode==='corrupt')await writeFile(path.join(config.dataDir,'artifacts',corruptId),'Changed bytes must be rejected');
  const decision={actions:[{caseId,kind:'delegate',lane:'glm',sourceTaskId:f.source,
    diagnosis:'A narrow accepted support delivery is ready.',instruction:'Consume the exact accepted support and preserve original verification.'}]};
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,support_task_id,support_task_ids,decision)
    VALUES($1,$2,$3,$4,'delegated','Accepted support is ready','[]',$5,$6,$7)`,
    [caseId,f.goal,f.task,f.run,f.source,JSON.stringify([f.source]),JSON.stringify(decision)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
    SELECT $1,goal_id,'independent-failure','Independent failure',description,agent_id,'failed',stage,account_id,model,criteria,
      execution||'{"workspace":"supervision-test/independent"}'::jsonb,3 FROM tasks WHERE id=$2`,[other,f.task]);
  for(let attempt=1;attempt<=3;attempt++)await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary)
    VALUES($1,$2,$3,$4,'harness',1,'failed','MODEL_TIMEOUT','Independent repeated failure')`,[randomUUID(),other,attempt,randomUUID()]);
  const added=await detectSupervisionCases();
  expect(added).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM supervision_cases WHERE id=$1',[added[0]])).rows[0].task_id).toBe(other);
  const code=mode==='oversized'?'SUPERVISION_INPUTS_TOO_LARGE':'ARTIFACT_CORRUPT';
  const blocked=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
  expect(blocked).toMatchObject({state:'open',decision,support_task_ids:[f.source]});
  expect(blocked.evidence).toContainEqual({kind:'SUPERVISION_HANDOFF_BLOCKED',code,sourceRunId:f.run,supportTaskIds:[f.source]});
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(task.criteria).toEqual(f.criteria);expect(task.attempt_count).toBe(3);
  expect(task.execution.inputs).toBeUndefined();expect(task.execution.supervisionHandoff).toBeUndefined();
  expect(task.instruction_revision_id).toBeNull();
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_HANDOFF_READY' AND entity_id=$1",[f.task])).rows[0].n).toBe(0);
  await detectSupervisionCases();
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_HANDOFF_BLOCKED' AND entity_id=$1",[f.task])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(3);
});

it('allows deterministic admission while a slow manager snapshot is being read, then admits only one fresh manager wake',async()=>{
 const f=await fixture();await enableManager(f);
 let enter!:()=>void,release!:()=>void;
 const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
 const reader:typeof managementSnapshot=async(db,goalId,policy,focus)=>{
   const own=(await db.query(`SELECT count(*)::int n FROM pg_locks WHERE pid=pg_backend_pid()
     AND locktype='advisory' AND objid=4280001 AND granted`)).rows[0].n;
   expect(own).toBe(0);enter();await held;
   return managementSnapshot(db,goalId,policy,focus);
 };
 const now=Date.now(),first=wakeSupervisors(now,reader),second=wakeSupervisors(now,reader);
 const worker=randomUUID();
 try{
 await Promise.race([entered,first.then(()=>{throw Error('MANAGER_SNAPSHOT_NOT_REACHED');})]);
 await pool.query("UPDATE settings SET value='{"+'"maxRuns":4,"browserSlots":4'+"}' WHERE id='capacity'");
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,criteria,resources,execution)
   VALUES($1::uuid,$2,$1::text,'Independent work','Authorized task','qa.backend','muse','muse-spark-1.3','ready','[]','[]',$3)`,
   [worker,f.goal,JSON.stringify({kind:'harness',profile:'code',workspace:'supervision-test/'+worker})]);
   expect(await schedule()).toHaveLength(1);
   expect((await pool.query('SELECT task_id FROM runs WHERE state=\'queued\'')).rows[0].task_id).toBe(worker);
 }finally{release();await Promise.all([first,second]);}
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});

it('rejects a prepared manager wake after the objective pauses during context assembly',async()=>{
 const f=await fixture();await enableManager(f);
 let enter!:()=>void,release!:()=>void;
 const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
 const wake=wakeSupervisors(Date.now(),async(db,goalId,policy,focus)=>{
   enter();await held;return managementSnapshot(db,goalId,policy,focus);
 });
 try{
   await Promise.race([entered,wake.then(()=>{throw Error('MANAGER_SNAPSHOT_NOT_REACHED');})]);
   await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 }finally{release();await wake;}
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});

it('wakes one stopped manager from delivery events before the periodic timer while ignoring model chatter',async()=>{
  const f=await fixture();await enableManager(f);
  const now=Date.now();await wakeSupervisors(now);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.coordinator]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('MODEL_RESPONSE',$1,'{}')",[f.task]);
  await wakeSupervisors(now+50000);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('TASK_ACCEPTED',$1,'{}')",[f.source]);
  await Promise.all([wakeSupervisors(now+50000),wakeSupervisors(now+50000)]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('ready');
  const wakes=(await pool.query("SELECT payload FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1 ORDER BY id",[f.goal])).rows;
  expect(wakes).toHaveLength(2);expect(wakes[1].payload.trigger).toBe('delivery_event');
});

it('supplies accepted repair patches and rejected reports without mixing games or stale acceptances', async () => {
  const f = await fixture();
  const valid = randomUUID();
  const otherGoal = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Other objective','Separate scope','supervision-test',$2)",
    [otherGoal, otherGoal],
  );
  for (const [id, key, base, stale, foreignGoal] of [
    [valid, 'peer', 'supervision-test/game', false, false],
    [randomUUID(), 'other-game', 'other/base', false, false],
    [randomUUID(), 'stale', 'supervision-test/game', true, false],
    [randomUUID(), 'other-objective', 'supervision-test/game', false, true],
  ] as const) {
    const runId = randomUUID(),
      fence = randomUUID();
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,$3,$3,'Peer fixture','qa.backend','running',$4,$5,1)`,
      [
        id,
        foreignGoal ? otherGoal : f.goal,
        key,
        JSON.stringify(f.criteria),
        JSON.stringify({
          kind: 'opencode',
          workKind: 'repair',
          baseWorkspace: base,
          workspace: 'supervision-test/' + key,
        }),
      ],
    );
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
      [runId, id, fence],
    );
    await writeArtifact(runId, fence, 'implementation.patch', 'Real peer patch');
    await writeArtifact(
      runId,
      fence,
      'verification.json',
      JSON.stringify({ passed: true }),
      'application/json',
    );
    await completeRun(runId, fence, 'Test peer', {});
    await validateTask(id);
    if (stale) {
      await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [id]);
      await pool.query(
        "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'submitted')",
        [randomUUID(), id, randomUUID()],
      );
    }
    await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, id]);
  }
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.run]);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.task]);
  await writeArtifact(
    f.run,
    f.fence,
    'verification.json',
    JSON.stringify({ passed: false, defect: 'Actual rejected native probe' }),
    'application/json',
  );
  await writeArtifact(f.run, f.fence, 'changes.patch', 'Preserved consumer delta');
  await writeArtifact(
    f.run,
    f.fence,
    'candidate.json',
    JSON.stringify({ source: 'preserved-consumer', files: [] }),
    'application/json',
  );
  const rejectedRepair = {
    'implementation.patch': 'Partial repair against the preserved consumer',
    'repair-report.json': JSON.stringify({
      fixVerified: false,
      remaining: 'Missing original atlas',
    }),
    'repair-tests.log': '23/24 passed; original atlas unresolved',
    'protocol-report.json': JSON.stringify({ suiteExecuted: true, exitCode: 1 }),
    'protocol-tests.log': 'Original unresolved references remain a failure',
    'test-source.mjs': 'Executable regression for the actual partial repair',
  };
  for (const [name, bytes] of Object.entries(rejectedRepair))
    await writeArtifact(f.run, f.fence, name, bytes);
  await writeArtifact(f.run, f.fence, 'unrelated-debug.log', 'Unrelated execution detail');
  await completeRun(f.run, f.fence, 'Actual rejected report', {});
  await validateTask(f.task);
  const source = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.source])).rows[0];
  const target = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  const context = await supervisionInputs(pool, source, target);
  expect(context.inputs).toHaveLength(10);
  expect(context.provenance.map((p) => ({ task: p.taskId, role: p.role }))).toEqual([
    { task: valid, role: 'accepted_repair' },
    ...Array.from({ length: 9 }, () => ({ task: f.task, role: 'rejected_evidence' })),
  ]);
  const contents = Object.fromEntries(
    context.inputs.map((a) => [a.name, Buffer.from(a.base64, 'base64').toString()]),
  );
  expect(JSON.parse(contents['candidate.json']).source).toBe('preserved-consumer');
  expect(contents['changes.patch']).toBe('Preserved consumer delta');
  for (const [name, bytes] of Object.entries(rejectedRepair)) expect(contents[name]).toBe(bytes);
  expect(contents).not.toHaveProperty('unrelated-debug.log');
  expect(JSON.parse(contents['verification.json']).passed).toBe(false);
  expect(JSON.parse(contents['repair-report.json']).fixVerified).toBe(false);
  expect(
    (await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state,
  ).not.toBe('accepted');
  expect(context.inputs.every((a) => a.digest.length === 64)).toBe(true);
  expect(context.omitted).toEqual([]);
});

it('supplies older and transitive accepted repair dependencies beyond four recent peers', async () => {
  const f = await fixture();
  const peers: string[] = [];
  for (let i = 0; i < 9; i++) {
    const id = randomUUID(),
      runId = randomUUID(),
      fence = randomUUID();
    peers.push(id);
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
       VALUES($1,$2,$3,$3,'Accepted dependency fixture','qa.backend','running',$4,$5,1)`,
      [
        id,
        f.goal,
        'repair-' + i,
        JSON.stringify(f.criteria),
        JSON.stringify({
          kind: 'opencode',
          workKind: 'repair',
          baseWorkspace: 'supervision-test/game',
          workspace: 'supervision-test/repair-' + i,
        }),
      ],
    );
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
      [runId, id, fence],
    );
    await writeArtifact(runId, fence, 'implementation.patch', 'Patch for repair ' + i);
    await writeArtifact(
      runId,
      fence,
      'verification.json',
      JSON.stringify({ passed: true }),
      'application/json',
    );
    await completeRun(runId, fence, 'Verified dependency', {});
    await validateTask(id);
  }
  // Seven direct peers, an older dependency of a peer, and a source-only prerequisite.
  for (const id of peers.slice(2))
    await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, id]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2),($3,$4)', [
    peers[2],
    peers[0],
    f.source,
    peers[1],
  ]);
  // A second path to the oldest module must not duplicate its bytes.
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [peers[3], peers[0]]);
  const source = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.source])).rows[0];
  const target = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  const context = await supervisionInputs(pool, source, target);
  expect(context.inputs).toHaveLength(9);
  expect(new Set(context.provenance.map((p) => p.taskId))).toEqual(new Set(peers));
  expect(context.provenance.every((p) => p.role === 'accepted_repair')).toBe(true);
  expect(new Set(context.inputs.map((a) => Buffer.from(a.base64, 'base64').toString()))).toEqual(
    new Set(peers.map((_, i) => 'Patch for repair ' + i)),
  );
  expect(context.inputs.every((a) => a.digest.length === 64)).toBe(true);
  expect(context.omitted).toEqual([]);
});

it('withdraws legacy supervised consumers so they cannot roll back unrelated dispatches', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='blocked_dependencies' WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Prior unresolved block','[]')",
    [randomUUID(), f.goal, f.task, f.run],
  );
  await detectSupervisionCases();
  await wakeSupervisors();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'recovery_blocked',
  );
  await pool.query("UPDATE tasks SET state='blocked_dependencies' WHERE id=$1", [f.task]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'recovery_blocked',
  );
  expect(await schedule()).toHaveLength(1);
  expect(
    (await pool.query('SELECT state FROM tasks WHERE id=$1', [f.coordinator])).rows[0].state,
  ).toBe('queued');
});

it.each([
  'needs-repair',
  'corrected',
  'repairing',
  'stale-candidate',
  'corrupt-candidate',
  'missing-provenance',
  'rechecking-race',
  'admission-supervision',
  'prepared-admission',
])('routes read-only QA safely, with implementation=%s', async (mode) => {
  const alreadyCorrected = mode === 'corrected';
  const f = await fixture();
  const implementation = randomUUID(),
    implementationRun = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
    SELECT $1,goal_id,'implementation','Implementation',description,agent_id,'accepted','integrate',account_id,model,criteria,jsonb_set(execution,'{workspace}','"supervision-test/implementation"'),1 FROM tasks WHERE id=$2`,
    [implementation, f.source],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'harness',1,'submitted',now())",
    [implementationRun, implementation, randomUUID()],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, implementation]);
  await pool.query(
    "UPDATE tasks SET state='submitted',execution=execution||$2::jsonb WHERE id=$1",
    [
      f.task,
      JSON.stringify({
        readOnlySource: true,
        sourceWorkspace: 'supervision-test/implementation',
        continuation: { state: 'needs_action' },
      }),
    ],
  );
  const oldProof = f.proof;
  f.run = randomUUID();
  f.fence = randomUUID();
  f.proof = () => ({
    ...oldProof(),
    runId: f.run,
    process: { ...oldProof().process, unit: 'agentos-run-' + f.run },
  });
  await pool.query('UPDATE tasks SET attempt_count=4 WHERE id=$1', [f.task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,4,$3,'harness',1,'submitted',now())",
    [f.run, f.task, f.fence],
  );
  const refs = [];
  for (const runId of [implementationRun, f.run]) {
    const id = randomUUID(),
      candidateDigest = hash(
        runId === f.run && ['corrected', 'stale-candidate', 'rechecking-race', 'admission-supervision', 'prepared-admission'].includes(mode)
          ? 'older-candidate'
          : 'current-candidate',
      );
    const bytes = Buffer.from(JSON.stringify({ baseDigest: candidateDigest, candidateDigest }));
    await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
    await writeFile(path.join(config.dataDir, 'artifacts', id), bytes, { flag: 'wx' });
    await pool.query(
      "INSERT INTO artifacts(id,run_id,name,digest,size,mime) VALUES($1,$2,'candidate.json',$3,$4,'application/json')",
      [id, runId, hash(bytes), bytes.length],
    );
    refs.push({ id, name: 'candidate.json', digest: hash(bytes), task_id: implementation });
  }
  const contextId = randomUUID();
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'immutable independent QA')",
    [contextId, f.run, JSON.stringify({ dependencyArtifacts: [refs[0]] })],
  );
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [f.run, contextId]);
  if (mode === 'missing-provenance')
    await pool.query('UPDATE runs SET context_id=NULL WHERE id=$1', [f.run]);
  if (mode === 'corrupt-candidate')
    await writeFile(path.join(config.dataDir, 'artifacts', refs[1].id), '{}');
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test-validator')",
    [
      randomUUID(),
      f.task,
      f.run,
      JSON.stringify([{ type: 'json_field', passed: false, label: 'Real native failure' }]),
    ],
  );
  await detectSupervisionCases();
  const original = await decide(f, {
    kind: 'delegate',
    lane: 'muse',
    sourceTaskId: f.source,
    title: 'Repair native resource resolution',
  });
  if (mode === 'admission-supervision') {
    const revision=(await pool.query('SELECT instruction_revision_id FROM tasks WHERE id=$1',[f.task])).rows[0].instruction_revision_id;
    await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('RUN_ADMISSION_BLOCKED',$1,$2)",
      [f.task,JSON.stringify({goalId:f.goal,instructionRevisionId:revision,code:'INSTRUCTION_CASE_CHANGED'})]);
    expect(await routeVerificationRepair(f.task)).toBeNull();
    expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[original])).rows[0].state).toBe('decided');
    expect((await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount).toBe(0);
  }
  if (alreadyCorrected) {
    await pool.query('UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1', [
      implementation,
      JSON.stringify({ repairFeedback: { sourceRunId: f.run } }),
    ]);
    await pool.query("UPDATE runs SET started_at=now()+interval '1 second' WHERE id=$1", [
      implementationRun,
    ]);
    await pool.query(
      "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test-validator')",
      [randomUUID(), implementation, implementationRun],
    );
  }
  if (mode === 'repairing') {
    await pool.query(
      "UPDATE tasks SET state='running',execution=execution||$2::jsonb WHERE id=$1",
      [implementation, JSON.stringify({ repairFeedback: { sourceRunId: f.run } })],
    );
    await pool.query(
      "UPDATE runs SET state='running',started_at=now(),finished_at=NULL WHERE id=$1",
      [implementationRun],
    );
  }
  if (mode === 'rechecking-race')
    await Promise.all(
      Array.from({ length: 3 }, () => applySupervisionDecisions(async () => f.proof())),
    );
  else await applySupervisionDecisions(async () => f.proof());
  const c = (await pool.query('SELECT * FROM supervision_cases WHERE id=$1', [original])).rows[0];
  if (['stale-candidate', 'rechecking-race', 'admission-supervision', 'prepared-admission'].includes(mode)) {
    expect(c).toMatchObject({ state: 'applied', applied_attempt: 4 });
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [implementation])).rows[0].state,
    ).toBe('accepted');
    expect((await pool.query('SELECT * FROM supervision_cases')).rowCount).toBe(1);
    expect(
      (await pool.query('SELECT state,execution FROM tasks WHERE id=$1', [f.task])).rows[0],
    ).toMatchObject({
      state: 'blocked_dependencies',
      execution: { workspace: 'supervision-test/oqa-' + f.run, readOnlySource: true },
    });
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
    ).toBe(1);
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rowCount,
    ).toBe(0);
    const replacement=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
    expect(replacement.description).toContain('QA_RECHECK_V1');
    let bound=(await pool.query('SELECT * FROM task_instruction_revisions WHERE id=$1',[replacement.instruction_revision_id])).rows[0];
    expect(bound.provenance.caseId).toBe(c.id);
    expect(bound.provenance.decisionRunId).toBe(c.decision_run_id);
    expect(bound.provenance.sourceAttempt).toBe(4);
    if (mode === 'prepared-admission') {
      const previous = bound;
      await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('RUN_ADMISSION_BLOCKED',$1,$2)",
        [f.task, JSON.stringify({ goalId: f.goal, instructionRevisionId: bound.id, code: 'INSTRUCTION_CASE_CHANGED' })]);
      expect(await tx(requestAdmissionSupervision)).toEqual([c.id]);
      await decide(f, { kind: 'revise', sourceTaskId: undefined,
        diagnosis: 'The runtime prepared recheck is valid; only current instruction provenance must be rebound.',
        instruction: 'Run the already prepared read-only verification under the original criteria and current candidate identity.' });
      await applySupervisionDecisions(async () => f.proof());
      const updated = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
      bound = (await pool.query('SELECT * FROM task_instruction_revisions WHERE id=$1', [updated.instruction_revision_id])).rows[0];
      expect(bound.id).not.toBe(previous.id);
      expect(bound.provenance.decisionRunId).not.toBe(previous.provenance.decisionRunId);
      expect(bound.provenance.caseId).toBe(c.id);
      expect(updated.state).toBe('blocked_dependencies');
      expect(updated.execution.readOnlySource).toBe(true);
      expect((await pool.query("SELECT payload->>'reason' reason FROM events WHERE entity_id=$1 AND kind='SUPERVISION_QA_RECHECK_READY' ORDER BY id DESC LIMIT 1", [f.task])).rows[0].reason)
        .toBe('ADMISSION_RECHECK');
    }
    await applySupervisionDecisions(async () => f.proof());
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
    ).toBe(mode === 'prepared-admission' ? 2 : 1);
    await schedule();
    const admitted=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[f.task])).rows[0];
    expect(admitted.state).toBe('queued');
    expect(admitted.instruction_snapshot.revisionId).toBe(bound.id);
    expect(admitted.instruction_snapshot.task.execution.readOnlySource).toBe(true);
    expect((await pool.query('SELECT state,context_id FROM runs WHERE id=$1',[f.run])).rows[0])
      .toEqual({state:'submitted',context_id:contextId});
    return;
  }
  if (['corrupt-candidate', 'missing-provenance'].includes(mode)) {
    expect(c.state).toBe('decided');
    expect(c.reason).toContain(
      mode === 'corrupt-candidate'
        ? 'VERIFICATION_CANDIDATE_INVALID'
        : 'VERIFICATION_CANDIDATE_UNBOUND',
    );
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [implementation])).rows[0].state,
    ).toBe('accepted');
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
    ).toBe(0);
    return;
  }
  if (alreadyCorrected) {
    expect(c.state).toBe('applied');
    expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(1);
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [implementation])).rows[0].state,
    ).toBe('accepted');
    expect(
      (await pool.query('SELECT state,execution FROM tasks WHERE id=$1', [f.task])).rows[0],
    ).toMatchObject({
      state: 'blocked_dependencies',
      execution: { readOnlySource: true, workspace: 'supervision-test/oqa-' + f.run },
    });
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
    ).toBe(1);
    await applySupervisionDecisions(async () => f.proof());
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
    ).toBe(1);
    return;
  }
  expect(c.state).toBe('forwarded');
  expect(c.decision.implementationTaskId).toBe(implementation);
  const qa = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(qa.state).toBe('blocked_dependencies');
  expect(qa.execution.readOnlySource).toBe(true);
  expect(qa.criteria).toEqual(f.criteria);
  const targetCase = (
    await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1', [implementation])
  ).rows[0];
  expect(targetCase.state).toBe('decided');
  expect(targetCase.decision.actions[0].caseId).toBe(targetCase.id);
  if (mode === 'repairing') {
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [implementation])).rows[0].state,
    ).toBe('running');
    const inspect = vi.fn();
    await applySupervisionDecisions(inspect);
    expect(inspect).not.toHaveBeenCalled();
    expect(
      (
        await pool.query('SELECT state,support_task_ids FROM supervision_cases WHERE id=$1', [
          targetCase.id,
        ])
      ).rows[0],
    ).toMatchObject({ state: 'decided', support_task_ids: [expect.any(String)] });
    expect(
      (await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1', [implementation])).rowCount,
    ).toBe(0);
    return;
  }
  await applySupervisionDecisions(async () => ({
    ...f.proof(),
    runId: implementationRun,
    workspace: 'supervision-test/implementation',
    process: { ...f.proof().process, unit: 'agentos-run-' + implementationRun },
  }));
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [targetCase.id])).rows[0]
      .state,
  ).toBe('delegated');
  expect(
    (await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1', [implementation])).rowCount,
  ).toBe(1);
});

it('does not replay older rejected plans over the latest supervisor decision', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  const c = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  const publish = async (attempt: number) => {
    const id = randomUUID(),
      fence = randomUUID();
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,created_at,started_at) VALUES($1,$2,$3,$4,'harness',1,'running',$5,$5)",
      [id, f.coordinator, attempt, fence, new Date(Date.now() + attempt * 1000)],
    );
    await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1", [
      f.coordinator,
      attempt,
    ]);
    await writeArtifact(
      id,
      fence,
      'coordination.json',
      JSON.stringify({
        supervision: {
          actions: [
            {
              caseId: c.id,
              kind: 'delegate',
              lane: 'muse',
              sourceTaskId: f.source,
              title: 'Repair an isolated native issue',
              diagnosis: 'The native resource map does not resolve its captured dependency.',
              instruction:
                'Reproduce a minimal request using the genuine captured files, preserve the failure, and propose a focused repair with its test.',
            },
          ],
        },
      }),
      'application/json',
    );
    await completeRun(id, fence, 'Test supervisor response', {});
    await validateTask(f.coordinator);
    return id;
  };
  await publish(1);
  const latest = await publish(2);
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"other/base\"') WHERE id=$1",
    [f.source],
  );
  await recordSupervisionDecisions();
  await applySupervisionDecisions(async () => f.proof());
  await recordSupervisionDecisions();
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query('SELECT state,decision_run_id FROM supervision_cases')).rows[0]).toEqual(
    { state: 'open', decision_run_id: latest },
  );
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_REPLAN_REQUIRED'")).rowCount,
  ).toBe(1);
});

it('reviews unchanged work every two minutes, once, and protects the prompt from the live legacy coordinator', async () => {
  const f = await fixture();
  await enableManager(f);
  const now = Date.now();
  await Promise.all([wakeSupervisors(now), wakeSupervisors(now)]);
  let t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(t.state).toBe('ready');
  expect(t.description).toContain('MANAGEMENT_V1');
  expect(t.description).toContain('BACKLOG_PLANNER_V1');
  expect(t.model).toBe('gpt-6.1-sol');
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [f.coordinator]);
  await pool.query(
    "UPDATE tasks SET state='ready',description='Legacy priorities only' WHERE id=$1",
    [f.coordinator],
  );
  t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(t.state).toBe('accepted');
  expect(t.description).toContain('MANAGEMENT_V1');
  await wakeSupervisors(now + 119999);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    1,
  );
  await wakeSupervisors(now + 120000);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    2,
  );
});

async function managerQuotaFallback(f: any, primaryRemaining = 0) {
  const quota = (remainingPercent: number) => JSON.stringify({ status: 'ok',
    groups: [{ label: 'codex', windows: [{ remainingPercent }] }] });
  await pool.query('UPDATE accounts SET quota=$1,allow_unknown=false WHERE id=\'sol\'', [quota(primaryRemaining)]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id,quota)
    VALUES('sol2','Second account','codex','gpt-6.1-sol','["gpt-6.1-sol"]',true,false,12,'sol2',$1)`, [quota(99)]);
  await enableManager(f);
}

it('keeps explicitly bound operator evidence across ordinary manager wakes without rewriting previous run contexts', async () => {
  const f = await fixture(); await enableManager(f); await detectSupervisionCases();
  const project = 'manager-memory-' + f.goal;
  await pool.query('UPDATE goals SET project=$2 WHERE id=$1', [f.goal, project]);
  const required = await saveMemory({ project, title: 'Existing authorized boundary',
    content: 'Preserve the current contracts. '.repeat(700), kind: 'decision', verification: 'confirmed', source: 'test:boundary' });
  const evidence = JSON.stringify({ auditId: 'persisted-operator-evidence', historical: true,
    nextAction: 'Recheck exact current candidates and accepted source hashes. '.repeat(160) });
  const memory = await saveMemory({ project, title: 'Historical operator evidence', content: evidence,
    kind: 'evidence', verification: 'confirmed', source: 'test:operator-audit' });
  const manager = async () => (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  const collect = async (run: any) => {
    const bridge = config.bridgeHost, laya = config.layaUrl;
    config.bridgeHost = ''; config.layaUrl = '';
    try { return await assembleContext(run, await manager(), []); }
    finally { config.bridgeHost = bridge; config.layaUrl = laya; }
  };
  const finish = async (run: any) => {
    await writeArtifact(run.id, run.fence, 'coordination.json', JSON.stringify({ management: {
      summary: 'Review exact source evidence.', capacityReason: 'Pending independent acceptance.', actions: [] } }));
    await completeRun(run.id, run.fence, 'Completed evidence review', {}); await validateTask(f.coordinator);
    await recordManagementReviews();
  };
  const now = Date.now();
  await wakeSupervisors(now);
  const [oldId] = await schedule(), oldRun = await startRun(oldId, 'memory-before-binding');
  const oldContext = await collect(oldRun);
  expect(oldContext.manifest.selected.map((m: any) => m.id)).not.toContain(memory.id);
  await finish(oldRun);
  const oldSnapshot = (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [oldId])).rows[0];
  const oldStoredContext = (await pool.query('SELECT * FROM contexts WHERE id=$1', [oldContext.id])).rows[0];
  const before = await manager();
  await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1', [f.coordinator,
    JSON.stringify([...before.required_memories, memory.id])]);
  expect(await manager()).toEqual({ ...before, required_memories: [...before.required_memories, memory.id] });
  for (let review = 1; review <= 2; review++) {
    await wakeSupervisors(now + review * 120001);
    expect((await manager()).required_memories).toContain(memory.id);
    const [id] = await schedule(), run = await startRun(id, 'memory-bound-review');
    expect(run.instruction_snapshot.task.required_memories).toContain(memory.id);
    const context = await collect(run);
    expect(context.manifest.selected.find((m: any) => m.id === memory.id))
      .toMatchObject({ required: true, version: 1, hash: hash(evidence), characters: evidence.length });
    expect(context.manifest.selected.map((m: any) => m.id)).toContain(required.id);
    expect(context.manifest.memorySelection.requiredCharacters).toBeGreaterThan(18000);
    expect(context.content).toContain(evidence);
    await finish(run);
  }
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [oldId])).rows[0]).toEqual(oldSnapshot);
  expect((await pool.query('SELECT * FROM contexts WHERE id=$1', [oldContext.id])).rows[0]).toEqual(oldStoredContext);
  expect((await manager()).criteria).toEqual(before.criteria);
  expect((await pool.query('SELECT kind,verification FROM memories WHERE id=$1', [memory.id])).rows[0])
    .toEqual({ kind: 'evidence', verification: 'confirmed' });
});

it('uses current quota for periodic wake and never repins the exhausted configured account on the next wake', async () => {
  const f = await fixture();
  await managerQuotaFallback(f, 99);
  const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  const now = Date.now();
  await wakeSupervisors(now, async (db, goalId, policy, focus) => {
    const snapshot = await managementSnapshot(db, goalId, policy, focus);
    // Provider quota changes after the analytical read snapshot was acquired.
    await pool.query(`UPDATE accounts SET quota='{"status":"ok","groups":[{"label":"codex","windows":[{"remainingPercent":0}]}]}' WHERE id='sol'`);
    return snapshot;
  });
  let after = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(after).toMatchObject({ state: 'ready', account_id: 'sol2', model: 'gpt-6.1-sol',
    attempt_count: before.attempt_count, criteria: before.criteria, allowed_accounts: before.allowed_accounts });
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [f.coordinator]);
  await wakeSupervisors(now + 120001);
  after = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(after).toMatchObject({ state: 'ready', account_id: 'sol2', model: 'gpt-6.1-sol', attempt_count: before.attempt_count });
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1', [f.coordinator])).rowCount).toBe(0);
});

it.each(['all_exhausted', 'allowlist', 'paused', 'workspace_execution'])(
  'does not wake an ineligible periodic manager when %s', async mode => {
    const f = await fixture();
    await managerQuotaFallback(f);
    if (mode === 'all_exhausted') await pool.query("UPDATE accounts SET quota=(SELECT quota FROM accounts WHERE id='sol') WHERE id='sol2'");
    if (mode === 'allowlist') await pool.query("UPDATE tasks SET allowed_accounts='[\"sol\"]' WHERE id=$1", [f.coordinator]);
    if (mode === 'paused') await pool.query("UPDATE tasks SET state='paused' WHERE id=$1", [f.coordinator]);
    if (mode === 'workspace_execution') await pool.query("UPDATE tasks SET execution='{\"kind\":\"harness\",\"workspace\":\"supervision-test/live-source\"}' WHERE id=$1", [f.coordinator]);
    const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
    await wakeSupervisors();
    expect((await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0]).toEqual(before);
    expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1", [f.goal])).rowCount).toBe(0);
  },
);

it('keeps recurring manager context bounded without dropping task identity or stored evidence', async () => {
  const f = await fixture();
  await enableManager(f);
  const instruction = 'Reproduce the independent native issue with genuine captured files. '.repeat(
    120,
  );
  await pool.query(
    `INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision)
      VALUES($1,$2,$3,$4,'delegated','Repair already delegated','[]',$5)`,
    [
      randomUUID(),
      f.goal,
      f.task,
      f.run,
      JSON.stringify({
        actions: Array.from({ length: 4 }, (_, i) => ({
          kind: 'delegate',
          lane: 'glm',
          title: 'Independent repair ' + i,
          sourceTaskId: f.source,
          diagnosis: 'Distinct captured native issue ' + i,
          instruction,
        })),
      }),
    ],
  );
  for (let i = 0; i < 50; i++) {
    const id = randomUUID();
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count,reason)
       SELECT $1,goal_id,$2,$2,description,agent_id,$3,stage,account_id,model,criteria,execution,1,$4 FROM tasks WHERE id=$5`,
      [
        id,
        'coverage-' + i,
        i < 30 ? 'accepted' : 'failed',
        'Detailed failure. '.repeat(150),
        f.source,
      ],
    );
    await pool.query(
      `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,summary)
       VALUES($1,$2,1,$3,'harness',1,'submitted',$4)`,
      [randomUUID(), id, randomUUID(), 'Detailed execution result "with quotes". '.repeat(200)],
    );
  }
  await wakeSupervisors();
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  const snapshot = JSON.parse(task.description.slice(task.description.indexOf('{"cases":')));
  expect(snapshot.catalog).toBeUndefined();
  expect(snapshot.operation.tasks).toHaveLength(21);
  expect(snapshot.operation.sourceCatalog.rows).toHaveLength(24);
  expect(snapshot.operation.sourceCatalog.selection).toMatchObject({totalRows:31,omittedRows:7,truncated:true});
  expect(snapshot.operation.sourceCatalog.rows.find((t: any[]) => t[0] === f.source)[7]).toBe(
    'supervision-test/source',
  );
  expect(snapshot.alreadyPlanned[0].actions).toHaveLength(4);
  expect(snapshot.alreadyPlanned[0].actions[0].sourceTaskId).toBe(f.source);
  expect(snapshot.alreadyPlanned[0].actions[0].instruction).toBeUndefined();
  expect(
    JSON.stringify({ description: task.description, context: 'M'.repeat(60000) }).length,
  ).toBeLessThan(180000);
  expect(
    (await pool.query('SELECT decision FROM supervision_cases')).rows[0].decision.actions[0]
      .instruction,
  ).toBe(instruction);
  expect(
    (
      await pool.query(
        "SELECT length(summary)::int n FROM runs WHERE summary LIKE 'Detailed execution%' LIMIT 1",
      )
    ).rows[0].n,
  ).toBeGreaterThan(700);
});

it('never overlaps a live review, even if task state looks finished', async () => {
  const f = await fixture();
  await enableManager(f);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
    [randomUUID(), f.coordinator, randomUUID()],
  );
  await wakeSupervisors();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    0,
  );
});

it('supplies intact failed reports from this goal to the manager as bounded planning evidence', async () => {
  const f = await fixture();
  const run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
    [run, f.task, fence],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=4 WHERE id=$1", [f.task]);
  const artifact = await writeArtifact(
    run,
    fence,
    'verification.json',
    JSON.stringify({
      passed: false,
      rootCause: 'Native registration lacks a captured dependency',
      limitations: ['No native cycles observed'],
      headers: { authorization: 'private fixture' },
    }),
    'application/json',
  );
  await completeRun(run, fence, 'Native boot failed honestly', {});
  const policy = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + f.goal])
  ).rows[0].value;
  const snapshot = await managementSnapshot(pool, f.goal, policy);
  const evidence = snapshot.evidence.find((e: any) => e.artifactId === artifact.id);
  expect(evidence.taskId).toBe(f.task);
  expect(evidence.runId).toBe(run);
  expect(evidence.digest).toBe(artifact.digest);
  expect(evidence.acceptancePassed).toBeNull();
  expect(JSON.parse(evidence.text).rootCause).toBe(
    'Native registration lacks a captured dependency',
  );
  expect(evidence.text).not.toContain('private fixture');
  expect(
    snapshot.evidence.reduce((n: number, e: any) => n + (e.text?.length ?? 0), 0),
  ).toBeLessThanOrEqual(18000);
});

it('gives the manager accepted diagnostics for unfinished consumers without treating negative findings as game acceptance', async () => {
  const f = await fixture(),
    diagnostic = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'durability-diagnostic','Durability','Real diagnosis','qa.backend','running','work',$3,$4,1)`,
    [
      diagnostic,
      f.goal,
      JSON.stringify([
        { id: 'report', label: 'Delivered report', type: 'artifact', name: 'protocol-report.json' },
      ]),
      JSON.stringify({
        kind: 'opencode',
        workKind: 'diagnostic',
        baseWorkspace: 'supervision-test/game',
        workspace: 'supervision-test/durability',
      }),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",
    [run, diagnostic, fence],
  );
  const artifact = await writeArtifact(
    run,
    fence,
    'protocol-report.json',
    JSON.stringify({
      suiteExecuted: true,
      exitCode: 1,
      findings: ['Wallet lost after server restart'],
      headers: { authorization: 'PRIVATE_DIAG_SENTINEL' },
    }),
    'application/json',
  );
  await completeRun(run, fence, 'Investigation delivered; durability failed.', {});
  await validateTask(diagnostic);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, diagnostic]);
  const policy = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + f.goal])
  ).rows[0].value;
  const evidence = (await managementSnapshot(pool, f.goal, policy)).evidence.find(
    (e: any) => e.artifactId === artifact.id,
  );
  expect(evidence).toMatchObject({
    taskId: diagnostic,
    runId: run,
    acceptancePassed: true,
    role: 'accepted_diagnostic',
    consumers: [{ taskId: f.task, stage: 'integrate', state: 'failed', live: false }],
  });
  expect(JSON.parse(evidence.text)).toMatchObject({
    exitCode: 1,
    findings: ['Wallet lost after server restart'],
  });
  expect(evidence.text).not.toContain('PRIVATE_DIAG_SENTINEL');
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [diagnostic]);
  expect(
    (await managementSnapshot(pool, f.goal, policy)).evidence.some(
      (e: any) => e.artifactId === artifact.id,
    ),
  ).toBe(false);
  await pool.query('UPDATE tasks SET attempt_count=1 WHERE id=$1', [diagnostic]);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [f.task]);
  expect(
    (await managementSnapshot(pool, f.goal, policy)).evidence.some(
      (e: any) => e.artifactId === artifact.id,
    ),
  ).toBe(false);
});

it('backs off failed manager requests and prevents the legacy retry loop from overriding it', async () => {
  const f = await fixture();
  await enableManager(f);
  const now = Date.now();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'harness',1,'failed',$4)",
    [randomUUID(), f.coordinator, randomUUID(), new Date(now)],
  );
  await pool.query("UPDATE tasks SET state='failed',attempt_count=1 WHERE id=$1", [f.coordinator]);
  await pool.query("UPDATE tasks SET state='retry_scheduled' WHERE id=$1", [f.coordinator]);
  expect(
    (await pool.query('SELECT state FROM tasks WHERE id=$1', [f.coordinator])).rows[0].state,
  ).toBe('failed');
  await wakeSupervisors(now + 119999);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    0,
  );
  await wakeSupervisors(now + 120000);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    1,
  );
});

it('repairs one obsolete oversized manager prompt promptly without removing failure backoff for the new version', async () => {
  const f = await fixture();
  await enableManager(f);
  const now = Date.now();
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'management-state:' + f.goal,
    JSON.stringify({ requestedAt: new Date(now).toISOString(),contextProjection:{state:'blocked',sourceHash:'old-context'} }),
  ]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at) VALUES($1,$2,1,$3,'harness',1,'failed','CONTEXT_TOO_LARGE',$4)",
    [randomUUID(), f.coordinator, randomUUID(), new Date(now)],
  );
  await pool.query("UPDATE tasks SET state='failed',attempt_count=1 WHERE id=$1", [f.coordinator]);
  await wakeSupervisors(now);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(task.state).toBe('ready');
  expect(task.description).toContain(managerContextVersion);
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value.contextProjection)
    .toMatchObject({state:'ready',version:managerContextVersion,sourceHash:hash(task.description)});
  expect(
    (
      await pool.query(
        "SELECT payload->>'trigger' trigger FROM events WHERE kind='MANAGEMENT_WAKEUP'",
      )
    ).rows[0].trigger,
  ).toBe('context_repair');
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.coordinator]);
  await wakeSupervisors(now + 1000);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    1,
  );
});

it('projects only a stopped current manager once and preserves the complete original and mandatory context', async () => {
  const f = await fixture();
  const instruction =
    'Keep RTP90 and original rules. No deployment; reproduce independently.\n' +
    'Observed defect. '.repeat(400);
  const patch = '--- a/game.js\n+++ b/game.js\n' + '+verifiedRepair();\n'.repeat(80);
  const snapshot = {
    cases: [
      { id: 'operator-a', evidence: [{ instruction, patch, gameAcceptance: false }] },
      { id: 'operator-b', evidence: [{ instruction, patch, gameAcceptance: false }] },
    ],
    operation: {
      tasks: Array.from({ length: 200 }, (_, i) => ({
        id: 'old-' + i,
        title: 'Completed source ' + i,
        state: 'accepted',
        stage: 'work',
        workKind: 'repair',
        expectedAttempt: 1,
        baseWorkspace: 'game/base',
        workspace: 'isolated/' + i,
        live: false,
        summary: 'Historical source result. '.repeat(100),
      })),
    },
  };
  const description =
    'MANAGEMENT_V1\nMANAGER_CONTEXT_V3\nLegacy source catalog.\nPreserve all criteria and decisions.\n' +
    JSON.stringify(snapshot);
  const runId = randomUUID(),
    contextId = randomUUID();
  const manifest = {
    selected: [{ id: 'mandatory', required: true, hash: hash(instruction) }],
    dependencyArtifacts: [],
    recoveryArtifacts: [],
  };
  await pool.query("UPDATE tasks SET state='failed',description=$2,attempt_count=1 WHERE id=$1", [
    f.coordinator,
    description,
  ]);
  await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,context_id)
    VALUES($1,$2,1,$3,'harness',1,'failed','CONTEXT_TOO_LARGE',$4)`,
    [runId, f.coordinator, randomUUID(), contextId],
  );
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
    contextId,
    runId,
    JSON.stringify(manifest),
    instruction,
  ]);
  await enableManager(f);
  const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  await expect(
    tx((db) => repairStoppedManagerContext(db, f.coordinator, runId, 'changed-hash')),
  ).rejects.toThrow('MANAGER_DESCRIPTION_CHANGED');
  const results = await Promise.allSettled(
    [0, 1].map(() =>
      tx((db) =>
        repairStoppedManagerContext(db, f.coordinator, runId, managerTextHash(description)),
      ),
    ),
  );
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
  expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
  const after = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(after).toMatchObject({
    state: 'ready',
    criteria: before.criteria,
    execution: before.execution,
    required_memories: before.required_memories,
    attempt_count: before.attempt_count,
  });
  const projection = JSON.parse(after.description.slice(after.description.indexOf('{"cases":')));
  const original = (
    await pool.query('SELECT * FROM contexts WHERE id=$1', [
      projection.originalDescription.contextId,
    ])
  ).rows[0];
  expect(original.content).toBe(description);
  expect(managerTextHash(original.content)).toBe(original.manifest.hash);
  expect(projection.evidenceTexts.map((entry: any) => entry.text)).toEqual([instruction, patch]);
  expect(projection.operation.sourceCatalog.rows).toHaveLength(24);
  expect(projection.operation.sourceCatalog.selection).toMatchObject({totalRows:200,omittedRows:176,truncated:true});
  expect(original.content).toBe(description);
  expect(
    (await pool.query('SELECT manifest,content FROM contexts WHERE id=$1', [contextId])).rows[0],
  ).toEqual({ manifest, content: instruction });
  expect(
    (await pool.query('SELECT context_id,state FROM runs WHERE id=$1', [runId])).rows[0],
  ).toEqual({ context_id: contextId, state: 'failed' });
  expect(
    (
      await pool.query(
        "SELECT 1 FROM events WHERE kind='MANAGER_CONTEXT_PROJECTED' AND entity_id=$1",
        [f.coordinator],
      )
    ).rowCount,
  ).toBe(1);
  const liveId = randomUUID();
  await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'harness',1,'running')`,
    [liveId, f.coordinator, randomUUID()],
  );
  await expect(
    tx((db) =>
      repairStoppedManagerContext(db, f.coordinator, liveId, managerTextHash(after.description)),
    ),
  ).rejects.toThrow('MANAGER_NOT_STOPPED_CONTEXT_FAILURE');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [liveId])).rows[0].state).toBe(
    'running',
  );
});

it.each([
  ['paused', 'MANAGER_OPERATION_DISABLED'],
  ['management-disabled', 'MANAGER_OPERATION_DISABLED'],
  ['autonomy-disabled', 'MANAGER_OPERATION_DISABLED'],
  ['manual', 'MANAGER_MANUAL_RECOVERY'],
  ['cancel-requested', 'MANAGER_CANCEL_REQUESTED'],
  ['latest-run-changed', 'MANAGER_NOT_STOPPED_CONTEXT_FAILURE'],
  ['older-live-run', 'MANAGER_HAS_LIVE_RUN'],
  ['oversized', 'MANAGER_PROJECTION_STILL_TOO_LARGE'],
  ['dependency-artifacts', 'MANAGER_INPUTS_REQUIRE_REVIEW'],
  ['recovery-artifacts', 'MANAGER_INPUTS_REQUIRE_REVIEW'],
])(
  'refuses stopped manager projection for %s without changing any persisted evidence',
  async (condition, error) => {
    const f = await fixture();
    const description =
      'MANAGEMENT_V1\nMANAGER_CONTEXT_V3\nLegacy source catalog.\nPreserve all criteria and decisions.\n' +
      JSON.stringify({ cases: [], operation: { tasks: [] } });
    const runId = randomUUID(),
      contextId = randomUUID();
    await pool.query("DELETE FROM settings WHERE id='autonomy:supervision-test'");
    await pool.query("UPDATE tasks SET state='failed',description=$2,attempt_count=1 WHERE id=$1", [
      f.coordinator,
      description,
    ]);
    await pool.query(
      `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,context_id)
    VALUES($1,$2,1,$3,'harness',1,'failed','CONTEXT_TOO_LARGE',$4)`,
      [runId, f.coordinator, randomUUID(), contextId],
    );
    const manifest = { dependencyArtifacts: [] as any[], recoveryArtifacts: [] as any[] };
    if (condition === 'dependency-artifacts')
      manifest.dependencyArtifacts.push({ id: randomUUID() });
    if (condition === 'recovery-artifacts') manifest.recoveryArtifacts.push({ id: randomUUID() });
    await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
      contextId,
      runId,
      JSON.stringify(manifest),
      condition === 'oversized' ? 'M'.repeat(180000) : 'Required decision.',
    ]);
    await enableManager(f);
    if (condition === 'paused')
      await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (condition === 'management-disabled')
      await pool.query('UPDATE settings SET value=\'{"enabled":false}\' WHERE id=$1', [
        'management:' + f.goal,
      ]);
    if (condition === 'autonomy-disabled')
      await pool.query(
        "INSERT INTO settings VALUES('autonomy:supervision-test','{\"enabled\":false}')",
      );
    if (condition === 'manual')
      await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [
        f.coordinator,
      ]);
    if (condition === 'cancel-requested')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [f.coordinator, JSON.stringify({ runId })],
      );
    if (condition === 'latest-run-changed' || condition === 'older-live-run')
      await pool.query(
        `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
      VALUES($1,$2,$3,$4,'harness',1,$5)`,
        [
          randomUUID(),
          f.coordinator,
          condition === 'latest-run-changed' ? 2 : 0,
          randomUUID(),
          condition === 'latest-run-changed' ? 'failed' : 'running',
        ],
      );
    const before = {
      task: (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0],
      runs: (
        await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt', [f.coordinator])
      ).rows,
      contexts: (await pool.query('SELECT * FROM contexts WHERE run_id=$1', [runId])).rows,
    };
    try {
      await expect(
        tx((db) =>
          repairStoppedManagerContext(db, f.coordinator, runId, managerTextHash(description)),
        ),
      ).rejects.toThrow(error);
      expect(
        (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0],
      ).toEqual(before.task);
      expect(
        (await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt', [f.coordinator]))
          .rows,
      ).toEqual(before.runs);
      expect((await pool.query('SELECT * FROM contexts WHERE run_id=$1', [runId])).rows).toEqual(
        before.contexts,
      );
      expect(
        (
          await pool.query(
            "SELECT 1 FROM events WHERE kind='MANAGER_CONTEXT_PROJECTED' AND entity_id=$1",
            [f.coordinator],
          )
        ).rowCount,
      ).toBe(0);
    } finally {
      await pool.query("DELETE FROM settings WHERE id='autonomy:supervision-test'");
    }
  },
);

it('respects paused, manual and explicitly cancelled operation state', async () => {
  const f = await fixture();
  await enableManager(f);
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  await wakeSupervisors();
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query("UPDATE tasks SET recovery='{" + '"mode":"manual"' + "}' WHERE id=$1", [
    f.coordinator,
  ]);
  await wakeSupervisors();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    0,
  );
  await pool.query('UPDATE tasks SET recovery=NULL WHERE id=$1', [f.coordinator]);
  const r = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'failed')",
    [r, f.coordinator, randomUUID()],
  );
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [
    f.coordinator,
    JSON.stringify({ runId: r }),
  ]);
  await wakeSupervisors();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP'")).rowCount).toBe(
    0,
  );
});

it('applies priorities once and rejects stale or live management actions', async () => {
  const f = await fixture();
  await enableManager(f);
  const a = {
    kind: 'prioritize',
    taskId: f.task,
    expectedAttempt: 3,
    priority: 77,
    reason: 'Unlock the actual integration path first.',
  };
  const results = await Promise.all([
    applyManagementAction(f.goal, f.run, 0, a),
    applyManagementAction(f.goal, f.run, 0, a),
  ]);
  expect(results.map((r) => r.code)).toEqual(['APPLIED', 'APPLIED']);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_ACTION'")).rowCount).toBe(
    1,
  );
  expect((await applyManagementAction(f.goal, f.run, 1, { ...a, expectedAttempt: 2 })).code).toBe(
    'TASK_CHANGED',
  );
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.task]);
  expect((await applyManagementAction(f.goal, f.run, 2, a)).code).toBe('TASK_LIVE');
  expect(
    (await pool.query('SELECT priority,criteria FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toEqual({ priority: 77, criteria: f.criteria });
});

it('routes only authorized capacity after fresh stop proof, keeping acceptance and retry controls intact', async () => {
  const f = await fixture();
  await enableManager(f);
  await pool.query('UPDATE accounts SET models=\'["gpt-6.1-sol","gpt-6-luna"]\' WHERE id=\'sol\'');
  await pool.query('UPDATE settings SET value=value||$2 WHERE id=$1', [
    'workforce:' + f.goal,
    JSON.stringify({ lunaTarget: 2, lunaAccounts: ['sol'] }),
  ]);
  const a = {
    kind: 'route',
    taskId: f.task,
    expectedAttempt: 3,
    accountId: 'sol',
    model: 'gpt-6-luna',
    lane: 'luna',
    reason: 'Use the idle authorized Luna to unblock integration.',
  };
  expect(
    (
      await applyManagementAction(f.goal, f.run, 0, a, async () => ({
        ...f.proof(),
        checkedAt: '2020-01-01',
      }))
    ).code,
  ).toBe('STOP_UNCONFIRMED');
  expect(
    (
      await applyManagementAction(f.goal, f.run, 1, { ...a, model: 'gpt-6.1-sol' }, async () =>
        f.proof(),
      )
    ).code,
  ).toBe('ROUTE_UNAVAILABLE');
  await pool.query('UPDATE tasks SET allowed_accounts=\'["muse"]\' WHERE id=$1', [f.task]);
  expect((await applyManagementAction(f.goal, f.run, 2, a, async () => f.proof())).code).toBe(
    'ROUTE_UNAVAILABLE',
  );
  await pool.query("UPDATE tasks SET allowed_accounts='[]' WHERE id=$1", [f.task]);
  expect((await applyManagementAction(f.goal, f.run, 3, a, async () => f.proof())).code).toBe(
    'APPLIED',
  );
  const t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(t.account_id).toBe('sol');
  expect(t.model).toBe('gpt-6-luna');
  expect(t.execution.workforceLane).toBe('luna');
  expect(t.state).toBe('failed');
  expect(t.criteria).toEqual(f.criteria);
  await pool.query("UPDATE accounts SET quota='{\"ordinaryUsageAllowed\":false}' WHERE id='sol'");
  expect((await applyManagementAction(f.goal, f.run, 4, a, async () => f.proof())).code).toBe(
    'ROUTE_UNAVAILABLE',
  );
});

it('escalates technical blockers once without treating account auth as a repairable code defect', async () => {
  const f = await fixture();
  await enableManager(f);
  const a = {
    kind: 'escalate',
    taskId: f.task,
    expectedAttempt: 3,
    reason: 'The delivery has a repeatable technical defect needing a distinct approach.',
  };
  await pool.query("UPDATE runs SET error_code='AUTH_REQUIRED' WHERE id=$1", [f.run]);
  expect((await applyManagementAction(f.goal, f.run, 0, a)).code).toBe('NO_TECHNICAL_FAILURE');
  await pool.query("UPDATE runs SET error_code='RESULT_FORMAT' WHERE id=$1", [f.run]);
  expect((await applyManagementAction(f.goal, f.run, 1, a)).code).toBe('APPLIED');
  expect((await applyManagementAction(f.goal, f.run, 2, a)).code).toBe('ALREADY_SUPERVISED');
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(1);
});

it('persists the real review and its action receipts before the next unchanged periodic wake', async () => {
  const f = await fixture();
  await enableManager(f);
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1", [f.task]);
  await wakeSupervisors();
  await schedule();
  const r = (await pool.query('SELECT * FROM runs WHERE task_id=$1', [f.coordinator])).rows[0];
  const started = await startRun(r.id, 'management-test');
  const management = {
    summary: 'Prioritize the critical integration.',
    capacityReason: 'Remaining work depends on one isolated repair.',
    actions: [
      {
        kind: 'prioritize',
        taskId: f.task,
        expectedAttempt: 3,
        priority: 76,
        reason: 'Unblock the final independent validation.',
      },
    ],
  };
  await writeArtifact(
    r.id,
    started.fence,
    'coordination.json',
    JSON.stringify({ management }),
    'application/json',
  );
  await completeRun(r.id, started.fence, management.summary, {});
  await validateTask(f.coordinator);
  await recordManagementReviews();
  await recordManagementReviews();
  const review = (await pool.query('SELECT * FROM management_reviews')).rows[0];
  expect(review.receipts[0].code).toBe('APPLIED');
  expect((await pool.query('SELECT 1 FROM management_reviews')).rowCount).toBe(1);
  const state = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['management-state:' + f.goal])
  ).rows[0].value;
  expect(state.summary).toBe(management.summary);
  expect(state.nextReviewAt).toBeTruthy();
  const policy = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + f.goal])
  ).rows[0].value;
  expect((await managementSnapshot(pool, f.goal, policy)).previous?.receipts[0].code).toBe(
    'APPLIED',
  );
  await pool.query(
    "INSERT INTO settings VALUES('capacity','{\"maxRuns\":20,\"browserSlots\":10}'),('remote-capacity','{\"maxRuns\":18}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
  const snapshot = await managementSnapshot(pool, f.goal, policy);
  expect(snapshot.resources.global).toEqual({ maxRuns: 20, browserSlots: 10 });
  expect(snapshot.resources.remote).toEqual({ maxRuns: 18 });
});
it('delegates a GLM implementation patch without expanding its source-write mount', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await decide(f, {
    kind: 'delegate',
    lane: 'glm',
    workKind: 'repair',
    sourceTaskId: f.source,
    title: 'Repair request replay identity',
  });
  await applySupervisionDecisions(async () => f.proof());
  const c = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  const t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [c.support_task_id])).rows[0];
  expect(t.execution).toMatchObject({
    kind: 'opencode',
    workKind: 'repair',
    writePaths: ['qa-protocol/', 'evidence/'],
  });
  expect(t.description).toContain('GLM_REPAIR_V1');
  expect(t.criteria.some((criterion: any) => criterion.name === 'implementation.patch')).toBe(true);
});
async function fixture(taskDescription='Authorized task') {
  const goal = randomUUID(),
    task = randomUUID(),
    coordinator = randomUUID(),
    source = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Supervise','Complete real work','supervision-test',$2)",
    [goal, goal],
  );
  const policy = {
    enabled: true,
    glmAccountId: 'glm',
    glmTarget: 12,
    museTarget: 6,
    workerAccounts: ['muse'],
    coordinatorAccountId: 'sol',
    coordinatorModel: 'gpt-6.1-sol',
  };
  await pool.query('INSERT INTO settings VALUES($1,$2),($3,\'{"enabled":true}\')', [
    'workforce:' + goal,
    JSON.stringify(policy),
    'supervision:' + goal,
  ]);
  for (const [id, provider, model] of [
    ['sol', 'codex', 'gpt-6.1-sol'],
    ['muse', 'muse', 'muse-spark-1.3'],
    ['glm', 'openai', 'glm-5.3-flash'],
  ])
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,12,$1)',
      [id, provider, model, JSON.stringify([model])],
    );
  const criteria = [
    {
      id: 'test',
      label: 'Actual verification',
      type: 'json_field',
      name: 'verification.json',
      field: 'passed',
      equals: true,
    },
  ];
  for (const [id, key, state, stage] of [
    [task, 'fix', 'failed', 'integrate'],
    [source, 'source', 'accepted', 'work'],
    [coordinator, 'coordinate', 'accepted', 'coordinate'],
  ])
    await pool.query(
      "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count) VALUES($1,$2,$3,$3,$11,'qa.backend',$4,$5,$6,$7,$8,$9,$10)",
      [
        id,
        goal,
        key,
        state,
        stage,
        id === coordinator ? 'sol' : 'muse',
        id === coordinator ? 'gpt-6.1-sol' : 'muse-spark-1.3',
        JSON.stringify(
          id === coordinator
            ? [{ id: 'coord', label: 'Plan', type: 'artifact', name: 'coordination.json' }]
            : criteria,
        ),
        id === coordinator
          ? null
          : JSON.stringify({
              kind: 'harness',
              profile: 'code',
              workspace: 'supervision-test/' + key,
              baseWorkspace: 'supervision-test/game',
            }),
        id === task ? 3 : 0,
        id === task ? taskDescription : 'Authorized task',
      ],
    );
  let run = '',
    fence = '';
  for (let attempt = 1; attempt <= 3; attempt++) {
    run = randomUUID();
    fence = randomUUID();
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,finished_at) VALUES($1,$2,$3,$4,'harness',1,'failed','MODEL_TIMEOUT','Known failure needing another approach',now())",
      [run, task, attempt, fence],
    );
  }
  const proof = () => ({
    runId: run,
    workspace: 'supervision-test/fix',
    checkedAt: new Date().toISOString(),
    process: {
      unit: 'agentos-run-' + run,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
  });
  return { goal, task, coordinator, source, run, fence, criteria, proof };
}
async function decide(f: any, action: any, recordThroughRuntime = false) {
  const c = (await pool.query(`SELECT * FROM supervision_cases WHERE task_id=$1
    ORDER BY (state IN('open','decided','delegated','waiting_evidence')) DESC,created_at DESC,id DESC LIMIT 1`, [f.task]))
    .rows[0];
  const decision = {
    caseId: c.id,
    kind: 'revise',
    diagnosis: 'The same timeout hides an unbounded sub-step.',
    instruction:
      'Reproduce only the blocked sub-step, bound its execution, and preserve actual failures before running the final validation.',
    ...action,
  };
  const runId = randomUUID(),
    fence = randomUUID();
  const attempt =
    (
      await pool.query('SELECT COALESCE(max(attempt),0)::int n FROM runs WHERE task_id=$1', [
        f.coordinator,
      ])
    ).rows[0].n + 1;
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,'running')",
    [runId, f.coordinator, attempt, fence],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1", [
    f.coordinator,
    attempt,
  ]);
  await writeArtifact(
    runId,
    fence,
    'coordination.json',
    JSON.stringify({ supervision: { actions: [decision] } }),
    'application/json',
  );
  await completeRun(runId, fence, 'Scoped supervisor decision fixture', {});
  await validateTask(f.coordinator);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'", [f.goal]);
  if (recordThroughRuntime) await recordSupervisionDecisions();
  else
    await pool.query(
      "UPDATE supervision_cases SET state='decided',decision=$2,decision_run_id=$3 WHERE id=$1",
      [c.id, JSON.stringify(decision), runId],
    );
  return c.id;
}
async function acceptedRepairFixture(
  f: any,
  verified = true,
  patch = '--- a/compat/module.mjs\n+++ b/compat/module.mjs\n@@ -1 +1 @@\n-export const repaired=false;\n+export const repaired=true;\n',
) {
  const run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    'UPDATE tasks SET state=\'running\',attempt_count=1,execution=execution||\'{"workKind":"repair"}\'::jsonb WHERE id=$1',
    [f.source],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",
    [run, f.source, fence],
  );
  const artifact = await writeArtifact(run, fence, 'implementation.patch', patch);
  await writeArtifact(
    run,
    fence,
    'repair-report.json',
    JSON.stringify({ fixVerified: verified, beforeExitCode: 1, afterExitCode: 0 }),
  );
  await writeArtifact(run, fence, 'verification.json', JSON.stringify({ passed: true }));
  await completeRun(run, fence, 'Accepted focal patch fixture', {});
  await validateTask(f.source);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  return { run, artifact, patch };
}
it('shows accepted repair reports and missing upstream handoffs without altering a live integrator or its DAG', async () => {
  const f = await fixture();
  const repair = await acceptedRepairFixture(f);
  const downstream = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,'later-integrator','Later integrator','Consume after main integration','engineering.backend.node','blocked_dependencies','integrate',$3,$4)`,
    [
      downstream,
      f.goal,
      JSON.stringify(f.criteria),
      JSON.stringify({
        kind: 'harness',
        workspace: 'supervision-test/later',
        baseWorkspace: 'supervision-test/game',
      }),
    ],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2),($1,$3)', [
    downstream,
    f.task,
    f.source,
  ]);
  const policy = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + f.goal])
  ).rows[0].value;
  const report = (
    await pool.query(
      "SELECT id,digest FROM artifacts WHERE run_id=$1 AND name='repair-report.json'",
      [repair.run],
    )
  ).rows[0];
  const original = JSON.stringify(
    (await pool.query('SELECT execution,criteria FROM tasks WHERE id=$1', [f.task])).rows[0],
  );
  const evidence = (await managementSnapshot(pool, f.goal, policy)).evidence.find(
    (e: any) => e.artifactId === report.id,
  );
  expect(evidence).toMatchObject({
    taskId: f.source,
    runId: repair.run,
    digest: report.digest,
    role: 'accepted_repair',
    acceptancePassed: true,
    repairDeliveryTargets: [
      { taskId: f.task, expectedAttempt: 3, stage: 'integrate', state: 'failed', live: false },
    ],
  });
  expect(JSON.parse(evidence.text).fixVerified).toBe(true);
  const running = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
    [running, f.task, randomUUID()],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=4 WHERE id=$1", [f.task]);
  expect(
    (await managementSnapshot(pool, f.goal, policy)).evidence.find(
      (e: any) => e.artifactId === report.id,
    ).repairDeliveryTargets,
  ).toMatchObject([{ taskId: f.task, expectedAttempt: 4, live: true }]);
  expect(
    JSON.stringify(
      (await pool.query('SELECT execution,criteria FROM tasks WHERE id=$1', [f.task])).rows[0],
    ),
  ).toBe(original);
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1', [f.task])).rowCount).toBe(
    0,
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.task, f.source]);
  const received = (await managementSnapshot(pool, f.goal, policy)).evidence.find(
    (e: any) => e.artifactId === report.id,
  );
  expect(received.repairDeliveryTargets).toEqual([]);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [running])).rows[0].state).toBe(
    'running',
  );
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.source]);
  expect(
    (await managementSnapshot(pool, f.goal, policy)).evidence.some(
      (e: any) => e.artifactId === report.id,
    ),
  ).toBe(false);
});
it('excludes another game, read-only QA and cyclic source relationships from repair delivery targets', async () => {
  const f = await fixture();
  await acceptedRepairFixture(f);
  const downstream = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,'later-integrator','Later integrator','Consume after main integration','engineering.backend.node','blocked_dependencies','integrate',$3,$4)`,
    [
      downstream,
      f.goal,
      JSON.stringify(f.criteria),
      JSON.stringify({
        kind: 'harness',
        workspace: 'supervision-test/later',
        baseWorkspace: 'supervision-test/game',
      }),
    ],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2),($1,$3)', [
    downstream,
    f.task,
    f.source,
  ]);
  const policy = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + f.goal])
  ).rows[0].value;
  const targets = async () =>
    (await managementSnapshot(pool, f.goal, policy)).evidence.find(
      (e: any) => e.taskId === f.source && e.name === 'repair-report.json',
    ).repairDeliveryTargets;
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb('other/game'::text)) WHERE id=$1",
    [f.task],
  );
  expect(await targets()).toEqual([]);
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}',to_jsonb('supervision-test/game'::text))||'{\"readOnlySource\":true}'::jsonb WHERE id=$1",
    [f.task],
  );
  expect(await targets()).toEqual([]);
  await pool.query(
    "UPDATE tasks SET execution=execution- 'readOnlySource',stage='verify' WHERE id=$1",
    [f.task],
  );
  expect(await targets()).toEqual([]);
  await pool.query("UPDATE tasks SET stage='integrate' WHERE id=$1", [f.task]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.source, f.task]);
  expect(await targets()).toEqual([]);
});
it('links an accepted repair directly to a stopped integrator and its next context exactly once', async () => {
  const f = await fixture();
  const repair = await acceptedRepairFixture(f);
  await detectSupervisionCases();
  await decide(f, { sourceTaskId: f.source });
  await Promise.all([
    applySupervisionDecisions(async () => f.proof()),
    applySupervisionDecisions(async () => f.proof()),
  ]);
  expect(
    (
      await pool.query('SELECT * FROM dependencies WHERE task_id=$1 AND depends_on=$2', [
        f.task,
        f.source,
      ])
    ).rowCount,
  ).toBe(1);
  const refs = await (await import('../server/recovery.js')).acceptedDependencyArtifacts(f.task);
  expect(refs.find((ref: any) => ref.id === repair.artifact.id)).toMatchObject({
    task_id: f.source,
    digest: repair.artifact.digest,
  });
  expect(
    (await pool.query('SELECT criteria FROM tasks WHERE id=$1', [f.task])).rows[0].criteria,
  ).toEqual(f.criteria);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'failed',
  );
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_REPAIR_ATTACHED'")).rowCount,
  ).toBe(1);
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(3);
});
it('merges the directly delivered repair into native inputs while preserving original input bytes', async () => {
  const f = await fixture();
  const repair = await acceptedRepairFixture(f);
  const original = {
    id: randomUUID(),
    name: 'original-contract.json',
    digest: hash('Original scope'),
    base64: Buffer.from('Original scope').toString('base64'),
  };
  await pool.query('UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1', [
    f.task,
    JSON.stringify({ kind: 'opencode', inputs: [original] }),
  ]);
  await detectSupervisionCases();
  await decide(f, { sourceTaskId: f.source });
  await applySupervisionDecisions(async () => f.proof());
  const execution = (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0]
    .execution;
  expect(execution.inputs[0]).toEqual(original);
  const delivered = execution.inputs.find((input: any) => input.id === repair.artifact.id);
  expect(Buffer.from(delivered.base64, 'base64').toString()).toBe(repair.patch);
  expect(execution.supervision.repairDelivery.runId).toBe(repair.run);
  expect(
    execution.repairDeliveryContext.provenance.some(
      (ref: any) => ref.artifactId === repair.artifact.id && ref.role === 'accepted_source',
    ),
  ).toBe(true);
});
it.each([
  'wrong-base',
  'stale-acceptance',
  'diagnostic',
  'unverified',
  'empty-patch',
  'cycle',
  'read-only',
])('does not link an unusable repair: %s', async (mode) => {
  const f = await fixture();
  await acceptedRepairFixture(f, mode !== 'unverified', mode === 'empty-patch' ? '' : undefined);
  if (mode === 'wrong-base')
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"other/game\"') WHERE id=$1",
      [f.source],
    );
  if (mode === 'diagnostic')
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{workKind}','\"diagnostic\"') WHERE id=$1",
      [f.source],
    );
  if (mode === 'read-only')
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{readOnlySource}','true') WHERE id=$1",
      [f.task],
    );
  if (mode === 'cycle')
    await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.source, f.task]);
  if (mode === 'stale-acceptance') {
    await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.source]);
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'submitted')",
      [randomUUID(), f.source, randomUUID()],
    );
  }
  await detectSupervisionCases();
  await decide(f, { sourceTaskId: f.source });
  await applySupervisionDecisions(async () => f.proof());
  expect(
    (
      await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2', [
        f.task,
        f.source,
      ])
    ).rowCount,
  ).toBe(0);
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_REPAIR_ATTACHED'")).rowCount,
  ).toBe(0);
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE task_id=$1', [f.task])).rows[0]
      .state,
  ).toBe('open');
  expect(
    (await pool.query('SELECT criteria FROM tasks WHERE id=$1', [f.task])).rows[0].criteria,
  ).toEqual(f.criteria);
});
it('keeps a live integrator untouched and delivers the accepted patch only after its current attempt stops', async () => {
  const f = await fixture();
  await acceptedRepairFixture(f);
  await detectSupervisionCases();
  await decide(f, { sourceTaskId: f.source });
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.run]);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.task]);
  const inspect = vi.fn(async () => f.proof());
  await applySupervisionDecisions(inspect);
  expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1', [f.task])).rowCount).toBe(
    0,
  );
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'running',
  );
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [f.run]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  await applySupervisionDecisions(inspect);
  expect(
    (
      await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2', [
        f.task,
        f.source,
      ])
    ).rowCount,
  ).toBe(1);
});
it('gives the supervisor the exact assigned suite for recurring report rejection without accepting the task', async () => {
  const f = await fixture();
  const testFile = 'qa-protocol/assigned-supervision.test.mjs';
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{testFile}',$2::jsonb) WHERE id=$1",
    [f.task, JSON.stringify(testFile)],
  );
  await pool.query(
    "UPDATE runs SET error_code='OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE' WHERE task_id=$1",
    [f.task],
  );
  await detectSupervisionCases();
  const c = (
    await pool.query('SELECT state,evidence FROM supervision_cases WHERE task_id=$1', [f.task])
  ).rows[0];
  expect(c.state).toBe('open');
  expect(c.evidence).toHaveLength(3);
  for (const evidence of c.evidence) {
    expect(evidence.diagnostic).toMatchObject({
      artifact: 'protocol-report.json',
      field: 'TEST_FILE',
      expectedTestFile: testFile,
    });
    expect(evidence.diagnostic.instruction).toContain('execução real desse arquivo exato');
  }
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'recovery_blocked',
  );
  expect(
    (await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1', [f.task])).rows[0]
      .n,
  ).toBe(0);
});

it('shows fresh artifact references after the case source attempt without injecting artifact bytes or rerunning the worker', async () => {
  const f = await fixture();
  await enableManager(f);
  await detectSupervisionCases();
  const currentRun = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'opencode',1,'running')",
    [currentRun, f.task, fence],
  );
  await pool.query(
    "UPDATE tasks SET attempt_count=4,execution=jsonb_set(execution,'{testFile}','\"qa-protocol/assigned-current.test.mjs\"') WHERE id=$1",
    [f.task],
  );
  const artifact = await writeArtifact(
    currentRun,
    fence,
    'repair-report.json',
    JSON.stringify({ missingInputs: ['Original captured bytes'], credential: 'PRIVATE_SENTINEL' }),
  );
  await pool.query(
    "UPDATE runs SET state='failed',error_code='OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE',summary='Genuine captured bytes absent',finished_at=now() WHERE id=$1",
    [currentRun],
  );
  await wakeSupervisors();
  const coordinator = (
    await pool.query('SELECT description FROM tasks WHERE id=$1', [f.coordinator])
  ).rows[0];
  const context = JSON.parse(
    coordinator.description.slice(coordinator.description.indexOf('{"cases":')),
  );
  const current = context.cases[0];
  expect(current.currentRun).toMatchObject({ id: currentRun, attempt: 4, state: 'failed' });
  expect(current.currentRun.artifacts).toHaveLength(1);
  expect(current.currentRun.artifacts[0]).toMatchObject({
    id: artifact.id,
    name: 'repair-report.json',
    digest: artifact.digest,
  });
  expect(current.currentRun.artifacts[0].size).toBeGreaterThan(0);
  expect(current.evidence[0].runId).toBe(f.run);
  expect(current.deliveryDiagnostic.expectedTestFile).toBe('qa-protocol/assigned-current.test.mjs');
  expect(coordinator.description).not.toContain('PRIVATE_SENTINEL');
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(4);
  expect(
    (await pool.query('SELECT source_run_id FROM supervision_cases WHERE task_id=$1', [f.task]))
      .rows[0].source_run_id,
  ).toBe(f.run);
});

it('keeps a large completed source catalog without sending obsolete routing and dependency detail to the manager', async () => {
  const f = await fixture();
  const ids = Array.from({ length: 200 }, () => randomUUID());
  for (const [i, id] of ids.entries()) {
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count,reason)
      VALUES($1,$2,$3,$4,'Preserved completed work','qa.backend','accepted','work','glm','glm-5.3-flash','[]',$5,1,$6)`,
      [
        id,
        f.goal,
        'archive-' + i,
        'Accepted repair ' + i,
        JSON.stringify({
          kind: 'opencode',
          workspace: 'supervision-test/archive-' + i,
          baseWorkspace: 'supervision-test/game',
        }),
        'Old routing detail '.repeat(100),
      ],
    );
    await pool.query(
      `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,summary,finished_at)
      VALUES($1,$2,1,$3,'opencode',1,'submitted',$4,now())`,
      [randomUUID(), id, randomUUID(), 'Actual historical result '.repeat(100)],
    );
  }
  const snapshot = await managementSnapshot(pool, f.goal, {
    glmAccountId: 'glm',
    coordinatorAccountId: 'sol',
    workerAccounts: ['muse'],
    glmTarget: 12,
    museTarget: 6,
  });
  const sources = snapshot.sourceCatalog.rows
    .filter((row: any[]) => ids.includes(row[0]))
    .map((row: any[]) => ({
      id: row[0],
      expectedAttempt: row[5],
      baseWorkspace: snapshot.sourceCatalog.bases[row[6]],
      workspace: row[7],
    }));
  expect(sources).toHaveLength(200);
  for (const source of sources) {
    expect(source.expectedAttempt).toBe(1);
    expect(source.workspace).toMatch(/^supervision-test\/archive-/);
    expect(source.baseWorkspace).toBe('supervision-test/game');
    expect(source).not.toHaveProperty('reason');
    expect(source).not.toHaveProperty('routeOptions');
    expect(source).not.toHaveProperty('accountId');
  }
  const pending = snapshot.tasks.find((t: any) => t.id === f.task)!;
  expect(pending).toHaveProperty('dependencies');
  expect(pending).toHaveProperty('routeOptions');
  expect(pending.errorCode).toBe('MODEL_TIMEOUT');
  expect(JSON.stringify(snapshot).length).toBeLessThan(45000);
  expect(
    (await pool.query('SELECT length(reason)::int n FROM tasks WHERE id=$1', [ids[0]])).rows[0].n,
  ).toBeGreaterThan(1000);
});
async function waitFixture() {
  const f = await fixture();
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.run]);
  const artifact = await writeArtifact(
    f.run,
    f.fence,
    'missing-inputs.json',
    JSON.stringify({ missing: ['Original captured atlas bytes'], verified: false }),
  );
  await pool.query("UPDATE runs SET state='failed' WHERE id=$1", [f.run]);
  await detectSupervisionCases();
  const caseId = await decide(f, {
    kind: 'wait_for_evidence',
    waitingFor: ['Original captured atlas bytes'],
    evidenceArtifactIds: [artifact.id],
  });
  return { ...f, caseId, artifact };
}
it('waits for new evidence without rerunning a stopped worker or losing its artifacts', async () => {
  const f = await waitFixture();
  await Promise.all([
    applySupervisionDecisions(async () => f.proof()),
    applySupervisionDecisions(async () => f.proof()),
  ]);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(task).toMatchObject({
    state: 'recovery_blocked',
    criteria: f.criteria,
    attempt_count: 3,
    execution: { continuation: { state: 'waiting_evidence', runId: f.run, caseId: f.caseId } },
  });
  expect(
    (
      await pool.query('SELECT state,wait_fingerprint FROM supervision_cases WHERE id=$1', [
        f.caseId,
      ])
    ).rows[0],
  ).toMatchObject({
    state: 'waiting_evidence',
    wait_fingerprint: await supervisionEvidenceFingerprint(pool, task),
  });
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_WAITING_EVIDENCE'")).rowCount,
  ).toBe(1);
  await expect(pool.query("UPDATE tasks SET state='queued' WHERE id=$1", [f.task])).rejects.toThrow(
    'SUPERVISION_PENDING',
  );
  const inspect = vi.fn(async () => f.proof());
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
  expect(inspect).not.toHaveBeenCalled();
  await pool.query('UPDATE tasks SET updated_at=now(),description=description||$2 WHERE id=$1', [
    f.task,
    '\nNew clock sample, same evidence',
  ]);
  await pool.query('UPDATE runs SET heartbeat_at=now() WHERE id=$1', [f.run]);
  await detectSupervisionCases();
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0].state,
  ).toBe('waiting_evidence');
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(3);
  expect(
    (await pool.query('SELECT digest FROM artifacts WHERE id=$1', [f.artifact.id])).rows[0].digest,
  ).toBe(f.artifact.digest);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1', [f.goal])).rows[0].state).toBe(
    'active',
  );
});
it.each(['inputs', 'dependency', 'memory'])(
  'reopens an evidence wait only after a new relevant %s arrives',
  async (mode) => {
    const f = await waitFixture();
    await applySupervisionDecisions(async () => f.proof());
    if (mode === 'inputs') {
      const bytes = 'Authentic new source bytes';
      await pool.query(
        "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
        [
          f.task,
          JSON.stringify([
            {
              id: randomUUID(),
              name: 'atlas-provenance.json',
              digest: hash(bytes),
              base64: Buffer.from(bytes).toString('base64'),
            },
          ]),
        ],
      );
    } else if (mode === 'memory') {
      await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1', [
        f.task,
        JSON.stringify([randomUUID()]),
      ]);
    } else {
      const runId = randomUUID(),
        fence = randomUUID();
      await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [
        f.task,
        f.source,
      ]);
      await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1", [f.source]);
      await pool.query(
        "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
        [runId, f.source, fence],
      );
      await detectSupervisionCases();
      expect(
        (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0]
          .state,
      ).toBe('waiting_evidence');
      await writeArtifact(
        runId,
        fence,
        'verification.json',
        JSON.stringify({ passed: true }),
        'application/json',
      );
      await completeRun(runId, fence, 'Genuine accepted new evidence', {});
      await validateTask(f.source);
    }
    await Promise.all([detectSupervisionCases(), detectSupervisionCases()]);
    expect(
      (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0]
        .state,
    ).toBe('open');
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_EVIDENCE_AVAILABLE'"))
        .rowCount,
    ).toBe(1);
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(3);
    await wakeSupervisors();
    expect(
      (await pool.query('SELECT state FROM tasks WHERE id=$1', [f.coordinator])).rows[0].state,
    ).toBe('ready');
  },
);
it.each(['foreign-artifact', 'live', 'unconfirmed-stop', 'paused', 'cancelled'])(
  'refuses an evidence wait with %s',
  async (mode) => {
    const f = await waitFixture();
    if (mode === 'foreign-artifact')
      await decide(f, {
        kind: 'wait_for_evidence',
        waitingFor: ['Original captured atlas bytes'],
        evidenceArtifactIds: [randomUUID()],
      });
    if (mode === 'live') await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.run]);
    if (mode === 'paused')
      await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (mode === 'cancelled')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [f.task, JSON.stringify({ runId: f.run })],
      );
    await applySupervisionDecisions(async () => ({
      ...f.proof(),
      ...(mode === 'unconfirmed-stop' ? { process: { ...f.proof().process, stopped: false } } : {}),
    }));
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_WAITING_EVIDENCE'")).rowCount,
    ).toBe(0);
    expect(
      (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0]
        .state,
    ).toBe(mode === 'foreign-artifact' ? 'open' : 'decided');
    expect(
      (await pool.query('SELECT criteria,attempt_count FROM tasks WHERE id=$1', [f.task])).rows[0],
    ).toEqual({ criteria: f.criteria, attempt_count: 3 });
  },
);
it.each(['pause', 'disable', 'cancel'])(
  'does not reopen an evidence wait after explicit %s',
  async (mode) => {
    const f = await waitFixture();
    await applySupervisionDecisions(async () => f.proof());
    if (mode === 'pause') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    if (mode === 'disable')
      await pool.query('UPDATE settings SET value=\'{"enabled":false}\' WHERE id=$1', [
        'supervision:' + f.goal,
      ]);
    if (mode === 'cancel')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [f.task, JSON.stringify({ runId: f.run })],
      );
    await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1', [
      f.task,
      JSON.stringify([randomUUID()]),
    ]);
    await detectSupervisionCases();
    expect(
      (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [f.caseId])).rows[0]
        .state,
    ).toBe('waiting_evidence');
  },
);
it('deduplicates repeated failures, wakes exact Sol and holds further blind retries', async () => {
  const f = await fixture();
  const added = await Promise.all([detectSupervisionCases(), detectSupervisionCases()]);
  expect(added.flat()).toHaveLength(1);
  await pool.query("UPDATE tasks SET state='retry_scheduled' WHERE id=$1", [f.task]);
  expect(
    (await pool.query('SELECT state,execution FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toMatchObject({
    state: 'recovery_blocked',
    execution: { continuation: { state: 'supervising' } },
  });
  await expect(pool.query("UPDATE tasks SET state='queued' WHERE id=$1", [f.task])).rejects.toThrow(
    'SUPERVISION_PENDING',
  );
  await wakeSupervisors();
  await wakeSupervisors();
  const coord = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.coordinator])).rows[0];
  expect(coord.model).toBe('gpt-6.1-sol');
  expect(coord.description).toContain('SUPERVISION_V1');
  expect(coord.description).not.toContain('BACKLOG_PLANNER_V1');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISOR_WAKEUP'")).rowCount).toBe(
    1,
  );
  const runs = await schedule();
  expect(runs).toHaveLength(1);
  expect((await pool.query('SELECT * FROM supervisor_jobs')).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM outbox')).rowCount).toBe(0);
});
it('never interrupts a running attempt to apply a supervisor decision', async () => {
  const f = await fixture();
  const running = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
    [running, f.task, randomUUID()],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=4 WHERE id=$1", [f.task]);
  await detectSupervisionCases();
  await decide(f, {});
  const inspect = vi.fn();
  await applySupervisionDecisions(inspect);
  expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'running',
  );
});
it('starts independent delegates while a consumer runs, then joins only after stop confirmation', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  const running = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
    [running, f.task, randomUUID()],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=4 WHERE id=$1", [f.task]);
  await decide(f, {
    kind: 'delegate',
    lane: 'muse',
    sourceTaskId: f.source,
    title: 'Repair isolated native bootstrap',
  });
  const inspect = vi.fn();
  await Promise.all([applySupervisionDecisions(inspect), applySupervisionDecisions(inspect)]);
  expect(inspect).not.toHaveBeenCalled();
  const c = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  expect(c.state).toBe('decided');
  expect(c.support_task_ids).toHaveLength(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'running',
  );
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1', [f.task])).rowCount).toBe(
    0,
  );
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [running]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  const proof = {
    ...f.proof(),
    runId: running,
    process: { ...f.proof().process, unit: 'agentos-run-' + running },
  };
  await applySupervisionDecisions(async () => proof);
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('delegated');
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1', [f.task])).rowCount).toBe(
    1,
  );
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_DELEGATED'")).rowCount,
  ).toBe(1);
});
it('requires current stop proof and applies a new instruction once without changing acceptance', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await decide(f, {});
  await applySupervisionDecisions(async () => ({ ...f.proof(), checkedAt: '2020-01-01' }));
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('decided');
  await Promise.all([
    applySupervisionDecisions(async () => f.proof()),
    applySupervisionDecisions(async () => f.proof()),
  ]);
  const t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(t.state).toBe('failed');
  expect(t.criteria).toEqual(f.criteria);
  expect(t.description).toContain('Reproduce only');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_APPLIED'")).rowCount).toBe(
    1,
  );
  expect(await detectSupervisionCases()).toEqual([]);
});
it('does not let old invalid supervisor bindings consume every resume slot', async () => {
  const f=await fixture();
  await detectSupervisionCases();
  await decide(f,{});
  await applySupervisionDecisions(async()=>f.proof());
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  for(let n=0;n<5;n++){
    const id=randomUUID(),runId=randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,updated_at)
      VALUES($1,$2,$3,'Old binding','Old fixture','qa.backend','failed','work','[]',$4,3,'2020-01-01')`,
      [id,f.goal,'old-'+n,JSON.stringify({...task.execution,supervision:{...task.execution.supervision,caseId:randomUUID()}})]);
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code)
      VALUES($1,$2,3,$3,'harness',1,'failed','MODEL_TIMEOUT')`,[runId,id,randomUUID()]);
  }
  const inspected:string[]=[];
  expect(await resumeSupervisedWork(async runId=>{
    inspected.push(runId);return {...f.proof(),candidate:{candidateDigest:'changed',patch:'real fixture patch'}};
  })).toEqual([f.task]);
  expect(inspected).toEqual([f.run]);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE key LIKE 'old-%' AND state='failed'")).rows[0].n).toBe(5);
});

it('resumes a newly applied diagnosis exactly once after six unchanged failures', async () => {
  const f = await fixture();
  const before = await continuationSignature(
    pool,
    (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0],
    'MODEL_TIMEOUT',
    'same',
  );
  for (let i = 0; i < 6; i++)
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('CONTINUATION_QUEUED',$1,$2)",
      [f.task, JSON.stringify({ signature: before })],
    );
  await detectSupervisionCases();
  await decide(f, {});
  await applySupervisionDecisions(async () => f.proof());
  const t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(t.execution.supervision.sourceRunId).toBe(f.run);
  const after = await continuationSignature(pool, t, 'MODEL_TIMEOUT', 'same');
  expect(after).not.toBe(before);
  expect(
    repeatedWithoutProgress(
      Array.from({ length: 6 }, () => ({ signature: before })),
      after,
    ),
  ).toBe(false);
  expect(
    repeatedWithoutProgress(
      Array.from({ length: 6 }, () => ({ signature: after })),
      after,
    ),
  ).toBe(true);
  const inspect = async () => ({
    ...f.proof(),
    candidate: { candidateDigest: 'same', patch: 'real fixture patch' },
  });
  const resumed = await Promise.all([resumeSupervisedWork(inspect), resumeSupervisedWork(inspect)]);
  expect(resumed.flat()).toEqual([f.task]);
  expect(
    (await pool.query('SELECT state,criteria FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toEqual({ state: 'retry_scheduled', criteria: f.criteria });
  expect(
    (
      await pool.query(
        "SELECT payload FROM events WHERE kind='CONTINUATION_QUEUED' ORDER BY id DESC LIMIT 1",
      )
    ).rows[0].payload.signature,
  ).toBe(after);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_RESUMED'")).rowCount).toBe(
    1,
  );
  await pool.query("UPDATE tasks SET attempt_count=4,state='failed' WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'failed')",
    [randomUUID(), f.task, randomUUID()],
  );
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
});

it('replans unchanged retries across supervisor UUIDs and resumes only after real input changes', async () => {
  const f = await fixture();
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  const run = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  const signature = await retryProgressSignature(pool, task, run, 'same');
  for (let n = 0; n < 6; n++)
    await pool.query(
      `INSERT INTO events(kind,entity_id,payload)
    VALUES('EXECUTION_RETRY_ADMITTED',$1,$2)`,
      [f.task, JSON.stringify({ signature, candidateDigest: 'same', sourceRunId: randomUUID() })],
    );
  await detectSupervisionCases();
  await decide(f, {}, true);
  await applySupervisionDecisions(async () => f.proof());
  const revised = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(await retryProgressSignature(pool, revised, run, 'same')).toBe(signature);
  const predecessor=(await pool.query("SELECT * FROM supervision_cases WHERE task_id=$1 AND state='applied'",[f.task])).rows[0];
  expect(predecessor).toBeTruthy();
  expect(await requestProgressReplan(f.task,f.run,signature)).toBe(false);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[predecessor.id])).rows[0]).toEqual(predecessor);
  const inspect = async () => ({
    ...f.proof(),
    candidate: { candidateDigest: 'same', patch: 'existing patch' },
  });
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
  expect(
    (await pool.query("SELECT state,reason FROM supervision_cases WHERE task_id=$1 AND state='open'", [f.task]))
      .rows[0],
  ).toMatchObject({ state: 'open', reason: expect.stringContaining('REPEATED_NO_PROGRESS') });
  const successor=(await pool.query("SELECT id,parent_case_id,request_key FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows[0];
  expect(successor.parent_case_id).toBe(predecessor.id);
  expect(successor.request_key).toBe('retry-progress:'+signature);
  expect(await requestProgressReplan(f.task,f.run,signature,{stopProof:await inspect()})).toBe(false);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[predecessor.id])).rows[0]).toEqual(predecessor);
  expect(
    (
      await pool.query("SELECT 1 FROM events WHERE kind='CONTINUATION_QUEUED' AND entity_id=$1", [
        f.task,
      ])
    ).rowCount,
  ).toBe(0);
  await decide(f, {}, true);
  expect(
    (await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [successor.id])).rows[0]
      .state,
  ).toBe('open');
  const rejected = (
    await pool.query(
      "SELECT payload FROM events WHERE kind='SUPERVISION_PLAN_REJECTED' AND entity_id=$1",
      [f.task],
    )
  ).rows;
  expect(rejected.length).toBeGreaterThan(0);
  expect(rejected.every((event) => event.payload.code === 'NEW_EVIDENCE_REQUIRED')).toBe(true);
  expect(new Set(rejected.map((event) => event.payload.runId)).size).toBe(rejected.length);
  await recordSupervisionDecisions();
  expect(
    (
      await pool.query(
        "SELECT 1 FROM events WHERE kind='SUPERVISION_PLAN_REJECTED' AND entity_id=$1",
        [f.task],
      )
    ).rowCount,
  ).toBe(rejected.length);
  const content = Buffer.from('New genuine input needed for the repair.');
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
    [
      f.task,
      JSON.stringify([
        {
          id: randomUUID(),
          name: 'captured-input.txt',
          mime: 'text/plain',
          size: content.length,
          digest: hash(content),
          base64: content.toString('base64'),
        },
      ]),
    ],
  );
  expect(
    await retryProgressSignature(
      pool,
      (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0],
      run,
      'same',
    ),
  ).not.toBe(signature);
  await decide(f, {}, true);
  await applySupervisionDecisions(inspect);
  expect(await resumeSupervisedWork(inspect)).toEqual([f.task]);
  expect(
    (await pool.query('SELECT criteria FROM tasks WHERE id=$1', [f.task])).rows[0].criteria,
  ).toEqual(f.criteria);
});

it('queues supervised native GLM recovery once through the existing request-gated executor', async () => {
  const f = await fixture();
  await pool.query(
    "UPDATE tasks SET account_id='glm',model='glm-5.3-flash',execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",
    [f.task],
  );
  await pool.query(
    "UPDATE runs SET runtime='opencode',account_id='glm',model='glm-5.3-flash' WHERE id=$1",
    [f.run],
  );
  await detectSupervisionCases();
  await decide(f, {});
  await applySupervisionDecisions(async () => f.proof());
  const proof = async () => ({
    ...f.proof(),
    runtime: 'opencode',
    state: { code: 'MODEL_TIMEOUT' },
    candidate: { patch: '', candidateDigest: 'same' },
  });
  const resumed = await Promise.all([resumeSupervisedWork(proof), resumeSupervisedWork(proof)]);
  expect(resumed.flat()).toEqual([f.task]);
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'retry_scheduled',attempt_count:3});
  expect((await pool.query('SELECT 1 FROM opencode_jobs')).rowCount).toBe(0);
  expect((await Promise.all([schedule('agentos-opencode',proof),schedule(undefined,proof)])).flat()).toHaveLength(1);
  expect(
    (await pool.query('SELECT state,attempt_count,model FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toEqual({ state: 'queued', attempt_count: 4, model: 'glm-5.3-flash' });
  expect((await pool.query('SELECT 1 FROM opencode_jobs')).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='CONTINUATION_QUEUED'")).rowCount).toBe(
    1,
  );
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'failed',
  );
});

it.each([
  'pause',
  'manual',
  'cancel',
  'live',
  'stale-proof',
  'cancel-during-inspection',
  'pause-during-inspection',
  'untrusted',
])('preserves %s while delivering supervisor revisions', async (mode) => {
  const f = await fixture();
  await detectSupervisionCases();
  await decide(f, {});
  await applySupervisionDecisions(async () => f.proof());
  if (mode === 'pause') await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  if (mode === 'manual')
    await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [f.task]);
  if (mode === 'cancel')
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
      [f.task, JSON.stringify({ runId: f.run })],
    );
  if (mode === 'live')
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
      [randomUUID(), f.task, randomUUID()],
    );
  if (mode === 'untrusted')
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{supervision,caseId}',to_jsonb($2::text)) WHERE id=$1",
      [f.task, randomUUID()],
    );
  const inspect = vi.fn(async () => {
    if (mode === 'cancel-during-inspection')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",
        [f.task, JSON.stringify({ runId: f.run })],
      );
    if (mode === 'pause-during-inspection')
      await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
    return { ...f.proof(), ...(mode === 'stale-proof' ? { checkedAt: '2020-01-01' } : {}) };
  });
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_RESUMED'")).rowCount).toBe(
    0,
  );
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'failed',
  );
  if (!['stale-proof', 'cancel-during-inspection', 'pause-during-inspection'].includes(mode))
    expect(inspect).not.toHaveBeenCalled();
});
it('delegates useful Muse work from an accepted source and preserves the original criteria', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await decide(f, {
    kind: 'delegate',
    lane: 'muse',
    sourceTaskId: f.source,
    title: 'Repair the isolated timeout',
  });
  await applySupervisionDecisions(async () => f.proof());
  const c = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  expect(c.state).toBe('delegated');
  const support = (await pool.query('SELECT * FROM tasks WHERE id=$1', [c.support_task_id]))
    .rows[0];
  expect(support.execution).toMatchObject({
    workforceLane: 'muse',
    sourceWorkspace: 'supervision-test/source',
    readOnlySource: false,
  });
  const original = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(original.state).toBe('recovery_blocked');
  expect(original.criteria).toEqual(f.criteria);
  expect(
    (await pool.query('SELECT * FROM dependencies WHERE task_id=$1', [f.task])).rows[0].depends_on,
  ).toBe(support.id);
});
it('rejects cross-project sources and dependency cycles', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await decide(f, {
    kind: 'delegate',
    lane: 'muse',
    sourceTaskId: f.source,
    title: 'Repair the isolated timeout',
  });
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"other/game\"') WHERE id=$1",
    [f.source],
  );
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query('SELECT state,reason FROM supervision_cases')).rows[0].reason).toContain(
    'SOURCE_INVALID',
  );
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"supervision-test/game\"') WHERE id=$1",
    [f.source],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.source, f.task]);
  await decide(f, {
    kind: 'delegate',
    lane: 'muse',
    sourceTaskId: f.source,
    title: 'Repair the isolated timeout',
  });
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query('SELECT state,reason FROM supervision_cases')).rows[0].reason).toContain(
    'DEPENDS_ON_TARGET',
  );
});
it('does not revive a paused goal, manually handled task or explicit cancellation', async () => {
  const f = await fixture();
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  expect(await detectSupervisionCases()).toEqual([]);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [f.task]);
  expect(await detectSupervisionCases()).toEqual([]);
  await pool.query('UPDATE tasks SET recovery=\'{"mode":"automatic"}\' WHERE id=$1', [f.task]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [
    f.task,
    JSON.stringify({ runId: f.run }),
  ]);
  expect(await detectSupervisionCases()).toEqual([]);
});
it('consumes one persisted supervisor job, including concurrent polling, without a second scheduler', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await wakeSupervisors();
  await schedule();
  const execute = vi.spyOn(WorkerEngine.prototype, 'execute').mockImplementation(async (id) => {
    const r = await startRun(id, 'supervisor-test');
    expect(r).toBeTruthy();
    await writeArtifact(
      id,
      r.fence,
      'coordination.json',
      JSON.stringify({
        summary: 'Repair',
        priorities: [],
        recommendations: [],
        supervision: { actions: [] },
      }),
      'application/json',
    );
    await completeRun(id, r.fence, 'Supervision completed', {});
    await validateTask(f.coordinator);
  });
  await Promise.all([runSupervisorJobs(), runSupervisorJobs()]);
  await closeSupervisorJobs();
  expect(execute).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state FROM supervisor_jobs')).rows[0].state).toBe('completed');
});
it('keeps an existing case inactive after pausing the goal or disabling automatic supervision', async () => {
  const f = await fixture();
  await detectSupervisionCases();
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  await wakeSupervisors();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISOR_WAKEUP'")).rowCount).toBe(
    0,
  );
  await decide(f, {});
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('decided');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  await pool.query('UPDATE settings SET value=\'{"enabled":false}\' WHERE id=$1', [
    'supervision:' + f.goal,
  ]);
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('decided');
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [f.task]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'failed',
  );
});
it('preserves an explicitly verified model when a running legacy bridge syncs an older catalog', async () => {
  await fixture();
  await pool.query(
    "INSERT INTO settings VALUES('verified-models:sol','[\"gpt-6.1-sol\"]') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
  try {
    await pool.query("UPDATE accounts SET models='[\"gpt-6-sol\"]' WHERE id='sol'");
    expect(
      (await pool.query("SELECT models FROM accounts WHERE id='sol'")).rows[0].models.sort(),
    ).toEqual(['gpt-6-sol', 'gpt-6.1-sol'].sort());
  } finally {
    await pool.query("DELETE FROM settings WHERE id='verified-models:sol'");
  }
});
it('keeps all independent delegations from one Sol response and joins their completion', async () => {
  const f = await fixture();
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",
    [f.task],
  );
  await detectSupervisionCases();
  await wakeSupervisors();
  await schedule();
  const c = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  const run = (await pool.query('SELECT * FROM runs WHERE task_id=$1', [f.coordinator])).rows[0];
  const started = await startRun(run.id, 'supervision-test');
  const base = {
    caseId: c.id,
    diagnosis: 'Distinct independent causes block the integration.',
    instruction:
      'Reproduce the narrow cause using real evidence, preserve failure, and provide a tested patch without changing acceptance.',
  };
  const actions = [
    { ...base, kind: 'revise' },
    {
      ...base,
      kind: 'delegate',
      lane: 'muse',
      sourceTaskId: f.source,
      title: 'Repair native resource registration',
    },
    {
      ...base,
      kind: 'delegate',
      lane: 'glm',
      sourceTaskId: f.source,
      title: 'Diagnose persisted request identity',
    },
  ];
  await writeArtifact(
    run.id,
    started.fence,
    'coordination.json',
    JSON.stringify({ supervision: { actions } }),
    'application/json',
  );
  await completeRun(run.id, started.fence, 'Two independent actions', {});
  await validateTask(f.coordinator);
  await recordSupervisionDecisions();
  expect(
    (await pool.query('SELECT decision FROM supervision_cases')).rows[0].decision.actions,
  ).toHaveLength(3);
  await applySupervisionDecisions(async () => f.proof());
  await recordSupervisionDecisions();
  await applySupervisionDecisions(async () => f.proof());
  const saved = (await pool.query('SELECT * FROM supervision_cases')).rows[0];
  expect(saved.state).toBe('delegated');
  expect(saved.support_task_ids).toHaveLength(2);
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1', [f.task])).rowCount).toBe(
    2,
  );
  const completeSupport = async (id: string) => {
    const support = (await pool.query('SELECT * FROM tasks WHERE id=$1', [id])).rows[0];
    const runId = randomUUID(),
      fence = randomUUID();
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
      [runId, id, fence],
    );
    await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1", [id]);
    for (const c of support.criteria)
      await writeArtifact(
        runId,
        fence,
        c.name,
        c.name.endsWith('.json')
          ? JSON.stringify({ passed: true, suiteExecuted: true })
          : 'Real fixture artifact',
        'text/plain',
      );
    await completeRun(runId, fence, 'Accepted support fixture', {});
    await validateTask(id);
  };
  await completeSupport(saved.support_task_ids[0]);
  await detectSupervisionCases();
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('delegated');
  await completeSupport(saved.support_task_ids[1]);
  await expect(pool.query("UPDATE tasks SET state='queued' WHERE id=$1", [f.task])).rejects.toThrow(
    'SUPERVISION_PENDING',
  );
  // Ready support must not rewrite a live consumer, even with stale task state.
  const liveRun = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'running')",
    [liveRun, f.task, randomUUID()],
  );
  await detectSupervisionCases();
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('delegated');
  await pool.query('DELETE FROM runs WHERE id=$1', [liveRun]);
  const sourceBytes = Buffer.from('Original accepted source contract and patch provenance');
  const originalInput = {
    id: randomUUID(),
    name: 'source-contract.json',
    digest: hash(sourceBytes),
    base64: sourceBytes.toString('base64'),
  };
  const deliveredArtifact = (
    await pool.query(
      'SELECT a.id,a.name,a.digest FROM artifacts a JOIN runs r ON r.id=a.run_id WHERE r.task_id=$1 LIMIT 1',
      [saved.support_task_ids[0]],
    )
  ).rows[0];
  const deliveredBytes = (await import('../server/service.js')).readArtifact;
  const alreadyReceived = await deliveredBytes(deliveredArtifact.id);
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
    [
      f.task,
      JSON.stringify([
        originalInput,
        { ...deliveredArtifact, base64: alreadyReceived.bytes.toString('base64') },
      ]),
    ],
  );
  await detectSupervisionCases();
  expect((await pool.query('SELECT state FROM supervision_cases')).rows[0].state).toBe('applied');
  const target = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(target.execution.inputs).toHaveLength(9);
  expect(target.execution.inputs[0]).toEqual(originalInput);
  expect(target.execution.inputs.filter((a: any) => a.id === deliveredArtifact.id)).toHaveLength(1);
  expect(Buffer.from(target.execution.inputs[0].base64, 'base64').length).toBeGreaterThan(0);
  expect(target.execution.continuation).toBeUndefined();
  expect(target.execution.supervisionHandoff).toMatchObject({
    caseId: saved.id,
    sourceRunId: f.run,
    kind: 'repair_delivery',
  });
  expect(target.execution.supervisionHandoff.artifactIds).toHaveLength(8);
});

it('refuses changed bytes under an existing supervisor input identity', () => {
  const id = randomUUID();
  const input = (text: string) => ({
    id,
    name: 'implementation.patch',
    digest: hash(text),
    base64: Buffer.from(text).toString('base64'),
  });
  expect(() => mergeSupervisorInputs([input('original')], [input('different')])).toThrow(
    'SUPERVISION_INPUT_COLLISION',
  );
  expect(() =>
    mergeSupervisorInputs([{ ...input('original'), digest: hash('different') }], []),
  ).toThrow('SUPERVISION_INPUT_DIGEST');
});

const routeDecision = {
  diagnosis: 'The failed run shows this worker route is unavailable for the remaining integration work.',
  instruction: 'Continue the same integration contract with a route already authorized for this task and verify the original criteria unchanged.',
};
async function prepareRouteFixture(f: any, options: { allowed?: string[]; glmQuota?: any; glmEnabled?: boolean } = {}) {
  await pool.query(`UPDATE settings SET value=value||$2::jsonb WHERE id=$1`, [
    'workforce:' + f.goal,
    JSON.stringify({ glmHarnessEnabled: true, lunaTarget: 2, lunaAccounts: ['codex'] }),
  ]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id,quota)
    VALUES('codex','codex','codex','gpt-6-luna','["gpt-6-luna","gpt-6.1-sol"]',true,false,8,'codex',
      '{"status":"ok","groups":[{"label":"codex","windows":[{"remainingPercent":70}]}]}')
    ON CONFLICT(id) DO UPDATE SET provider=excluded.provider,model=excluded.model,models=excluded.models,
      enabled=excluded.enabled,allow_unknown=excluded.allow_unknown,max_concurrency=excluded.max_concurrency,
      bridge_id=excluded.bridge_id,quota=excluded.quota`);
  await pool.query(`UPDATE accounts SET quota=$1,enabled=$2,allow_unknown=false WHERE id='glm'`, [
    JSON.stringify(options.glmQuota ?? { status: 'ok', groups: [{ windows: [{ remainingPercent: 70 }] }] }),
    options.glmEnabled ?? true,
  ]);
  await pool.query(`UPDATE tasks SET account_id='codex',model='gpt-6-luna',allowed_accounts=$2::jsonb,
    execution=execution||'{"workforceLane":"luna"}'::jsonb WHERE id=$1`, [
    f.task,
    JSON.stringify(options.allowed ?? ['codex', 'glm', 'sol']),
  ]);
  await detectSupervisionCases();
}

it('accepts a legal Luna-to-GLM revise route and records immutable route provenance for the next run', async () => {
  const f = await fixture();
  await prepareRouteFixture(f);
  const before = (await pool.query('SELECT id,attempt,fence,account_id,model,instruction_snapshot FROM runs WHERE task_id=$1 ORDER BY attempt', [f.task])).rows;
  const caseId = await decide(f, { ...routeDecision, route: { accountId: 'glm', model: 'glm-5.3-flash' } }, true);
  await applySupervisionDecisions(async () => f.proof());
  const updated = (await pool.query('SELECT account_id,model,execution,criteria FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(updated).toMatchObject({ account_id: 'glm', model: 'glm-5.3-flash', criteria: f.criteria,
    execution: { workforceLane: 'glm', supervision: { routeChange: {
      schema: 1, caseId, sourceRunId: f.run, sourceAttempt: 3, previous: { accountId: 'codex', model: 'gpt-6-luna', lane: 'luna' },
      next: { accountId: 'glm', model: 'glm-5.3-flash', lane: 'glm' },
    } } } });
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [caseId])).rows[0].state).toBe('applied');
  expect((await pool.query("SELECT payload FROM events WHERE kind='SUPERVISION_ROUTE_CHANGED' AND entity_id=$1", [f.task])).rows[0].payload)
    .toMatchObject({ caseId, sourceRunId: f.run, decisionRunId: expect.any(String),
      previous: { accountId: 'codex', model: 'gpt-6-luna' }, next: { accountId: 'glm', model: 'glm-5.3-flash' } });
  expect((await pool.query('SELECT id,attempt,fence,account_id,model,instruction_snapshot FROM runs WHERE task_id=$1 ORDER BY attempt', [f.task])).rows).toEqual(before);
  expect(await resumeSupervisedWork(async () => f.proof())).toEqual([f.task]);
  await schedule();
  const nextRun = (await pool.query('SELECT account_id,model,instruction_snapshot FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [f.task])).rows[0];
  expect(nextRun).toMatchObject({ account_id: 'glm', model: 'glm-5.3-flash' });
  expect(nextRun.instruction_snapshot.task).toMatchObject({ account_id: 'glm', model: 'glm-5.3-flash' });
});

it.each([
  ['account is outside allowed_accounts', { allowed: ['codex'] }, { accountId: 'glm', model: 'glm-5.3-flash' }],
  ['model is absent from the current account catalog', {}, { accountId: 'glm', model: 'glm-not-catalogued' }],
  ['account is disabled', { glmEnabled: false }, { accountId: 'glm', model: 'glm-5.3-flash' }],
  ['quota is exhausted', { glmQuota: { ordinaryUsageAllowed: false } }, { accountId: 'glm', model: 'glm-5.3-flash' }],
  ['Sol remains reserved for coordinator work', {}, { accountId: 'sol', model: 'gpt-6.1-sol' }],
])('reopens a revise when route is rejected because %s without changing the previous route', async (_label, options, route) => {
  const f = await fixture();
  await prepareRouteFixture(f, options as any);
  const caseId = await decide(f, { ...routeDecision, route }, true);
  await applySupervisionDecisions(async () => f.proof());
  expect((await pool.query("SELECT account_id,model,execution->>'workforceLane' lane FROM tasks WHERE id=$1", [f.task])).rows[0])
    .toEqual({ account_id: 'codex', model: 'gpt-6-luna', lane: 'luna' });
  expect((await pool.query('SELECT state,reason FROM supervision_cases WHERE id=$1', [caseId])).rows[0])
    .toMatchObject({ state: 'open', reason: expect.stringContaining('Supervisor deve revisar a decisão') });
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_ROUTE_CHANGED' AND entity_id=$1", [f.task])).rows[0].n).toBe(0);
});

it('rejects route fields on delegates, initial admission, and source resolution actions', () => {
  const base = { caseId: randomUUID(), diagnosis: routeDecision.diagnosis, instruction: routeDecision.instruction,
    route: { accountId: 'codex', model: 'gpt-6.1-sol' } };
  expect(actionSchema.safeParse({ ...base, kind: 'delegate', lane: 'glm', sourceTaskId: randomUUID(), title: 'Delegate route test' }).success).toBe(false);
  expect(actionSchema.safeParse({ ...base, kind: 'revise', initialAdmission: { phaseEventId: '1', contractDigest: 'a'.repeat(64) } }).success).toBe(false);
  expect(actionSchema.safeParse({ ...base, kind: 'revise', sourceTaskId: randomUUID(), deliveryResolution: { obligationId: randomUUID(), generation: 'b'.repeat(64), expectedSourceRunId: randomUUID() } }).success).toBe(false);
});

it.each(['stale-attempt', 'live-run', 'cancel-requested', 'stale-stop-proof'])('does not route a %s supervision case', async (guard) => {
  const f = await fixture();
  await prepareRouteFixture(f);
  const caseId = await decide(f, { ...routeDecision, route: { accountId: 'glm', model: 'glm-5.3-flash' } }, true);
  if (guard === 'stale-attempt') await pool.query('UPDATE tasks SET attempt_count=4 WHERE id=$1', [f.task]);
  if (guard === 'live-run') {
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,4,$3,'harness',1,'running','codex','gpt-6-luna')", [randomUUID(), f.task, randomUUID()]);
    await pool.query("UPDATE tasks SET attempt_count=4,state='running' WHERE id=$1", [f.task]);
  }
  if (guard === 'cancel-requested') await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [f.task, JSON.stringify({ runId: f.run })]);
  const inspect = guard === 'stale-stop-proof' ? async () => ({ ...f.proof(), checkedAt: new Date(Date.now() - 120000).toISOString() }) : async () => f.proof();
  await applySupervisionDecisions(inspect as any);
  expect((await pool.query("SELECT account_id,model,execution->>'workforceLane' lane FROM tasks WHERE id=$1", [f.task])).rows[0])
    .toMatchObject({ account_id: 'codex', model: 'gpt-6-luna', lane: 'luna' });
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_ROUTE_CHANGED' AND entity_id=$1", [f.task])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1', [caseId])).rows[0].state).not.toBe('applied');
});

it('reconciles orphaned analytical supervisor runs in reconciling HEARTBEAT_LOST without artifacts when owner backend is gone', async () => {
  const f = await fixture();
  const runId = randomUUID();
  const fence = randomUUID();
  await pool.query("UPDATE tasks SET state='reconciling', attempt_count=1 WHERE id=$1", [f.coordinator]);
  await pool.query(
    `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,worker_id,started_at)
     VALUES($1,$2,1,$3,'harness',1,'reconciling','HEARTBEAT_LOST','agentos-backlog',now() - interval '10 minutes')`,
    [runId, f.coordinator, fence]
  );
  await pool.query(
    `INSERT INTO supervisor_jobs(run_id,state) VALUES($1,'running')`,
    [runId]
  );
  await pool.query(
    `INSERT INTO settings(id,value) VALUES($1,$2)`,
    [
      'supervisor-owner:' + runId,
      JSON.stringify({
        version: 1,
        phase: 'claimed',
        runId,
        runFence: fence,
        taskId: f.coordinator,
        goalId: f.goal,
        attempt: 1,
        ownerNonce: randomUUID(),
        backendPid: 99999999,
      }),
    ]
  );

  const count = await reconcileLostAnalyticalSupervisors();
  expect(count).toBe(1);
  expect((await pool.query('SELECT state, error_code, summary FROM runs WHERE id=$1', [runId])).rows[0]).toMatchObject({
    state: 'failed',
    error_code: 'HEARTBEAT_LOST',
    summary: expect.stringContaining('Analytical supervisor owner connection lost'),
  });
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.coordinator])).rows[0].state).toBe('failed');
  expect((await pool.query('SELECT state FROM supervisor_jobs WHERE run_id=$1', [runId])).rows[0].state).toBe('completed');
  expect(
    (await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISOR_ANALYTICAL_RECONCILED' AND entity_id=$1", [f.coordinator])).rows[0].n
  ).toBe(1);
});

import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {WorkerEngine} from '../server/worker.js';

// Exercise the real WorkerEngine orchestration with inert infrastructure.
// This proves scheduling/lifecycle behavior, not PostgreSQL recovery authority.
const io=vi.hoisted(()=>({
  recover:vi.fn(),generic:vi.fn(),schedule:vi.fn(),send:vi.fn(),bossStop:vi.fn(),
  plannerTick:vi.fn(),plannerClose:vi.fn(),reviewTick:vi.fn(),reviewClose:vi.fn(),wakeClose:vi.fn(),
  query:vi.fn(),outbox:[] as {id:number;run_id:string}[],
}));
vi.mock('pg-boss',()=>({PgBoss:class{send=io.send;stop=io.bossStop;}}));
vi.mock('../server/config.js',()=>({config:{databaseUrl:'',workerSlots:12}}));
vi.mock('../server/db.js',()=>({pool:{query:io.query},tx:async(fn:any)=>fn({query:io.query}),lock:vi.fn(),event:vi.fn()}));
// Pure orchestration fixture: PG/schema authority is exercised by the separate
// transport-task-recovery and source/guidance suites, with their real bootstrap.
vi.mock('../server/schema-readiness.js',()=>({prepareSchema:vi.fn(async()=>{})}));
vi.mock('../server/run-criteria-metadata.js',()=>({installRunCriteriaMetadata:vi.fn(async()=>{})}));
vi.mock('../server/service.js',()=>({schedule:io.schedule,markUncertain:vi.fn(async()=>{}),
  startRun:vi.fn(),heartbeat:vi.fn(),writeArtifact:vi.fn(),completeRun:vi.fn(),validateTask:vi.fn(),failRun:vi.fn(),confirmStopped:vi.fn()}));
vi.mock('../server/memory.js',()=>({assembleContext:vi.fn()}));
vi.mock('../server/runtime.js',()=>({executeRuntime:vi.fn()}));
vi.mock('../server/recall.js',()=>({syncRecallOnce:vi.fn(async()=>{})}));
vi.mock('../server/recovery.js',()=>({recoverStoppedTasks:io.generic,acceptedDependencyArtifacts:vi.fn()}));
vi.mock('../server/transport-task-recovery.js',()=>({recoverTransportTasks:io.recover}));
vi.mock('../server/planner.js',()=>({PlanningEngine:class{tick=io.plannerTick;close=io.plannerClose;}}));
vi.mock('../server/objective-review.js',()=>({installObjectiveReview:vi.fn(),ObjectiveReviewEngine:class{tick=io.reviewTick;close=io.reviewClose;}}));
vi.mock('../server/planning-wake.js',()=>({PlanningWake:class{close=io.wakeClose;}}));
vi.mock('../server/validation-identity.js',()=>({readPendingValidations:vi.fn(async()=>[])}));
vi.mock('../server/candidate-validation.js',()=>({reconcileWorkerValidationIntents:vi.fn(async()=>{}),reconcileCandidateValidations:vi.fn(async()=>{})}));
vi.mock('../server/candidate-validation-evidence.js',()=>({reconcileCandidateValidationEvidence:vi.fn(async()=>{})}));
vi.mock('../server/published-rebase.js',()=>({reconcilePublishedRebases:vi.fn(async()=>{})}));
vi.mock('../server/verification-source-binding.js',()=>({reconcileVerificationSourceBindings:vi.fn(async()=>{})}));

let engine:WorkerEngine,resolveInspection:(value:string[])=>void,rejectInspection:(error:Error)=>void;
beforeEach(()=>{
  vi.clearAllMocks();io.outbox=[];
  const inspection=new Promise<string[]>((resolve,reject)=>{resolveInspection=resolve;rejectInspection=reject;});
  io.recover.mockImplementation(()=>inspection);
  for(const mock of [io.generic,io.schedule,io.send,io.bossStop,io.plannerTick,io.plannerClose,io.reviewTick,io.reviewClose,io.wakeClose])
    mock.mockResolvedValue(undefined);
  io.query.mockImplementation(async(sql:string,values:any[]=[])=>{
    if(sql.startsWith('SELECT * FROM outbox'))return {rows:[...io.outbox]};
    if(sql.startsWith('UPDATE outbox'))io.outbox=io.outbox.filter(row=>row.id!==values[0]);
    return {rows:[],rowCount:0};
  });
  engine=new WorkerEngine();
});
afterEach(async()=>{resolveInspection([]);await engine.stop();});

it('publishes independent ready work while transport inspection is still pending',async()=>{
  io.outbox=[{id:1,run_id:'independent-ready-run'}];
  const tick=engine.tick();
  await vi.waitFor(()=>expect(io.send).toHaveBeenCalledWith('runs',{runId:'independent-ready-run'},expect.objectContaining({singletonKey:'independent-ready-run'})),{timeout:1000});
  await tick;
  expect(io.recover).toHaveBeenCalledTimes(1);expect(io.generic).toHaveBeenCalledTimes(1);
  expect(io.schedule).toHaveBeenCalledTimes(1);expect(engine.transportRecoverySync).not.toBeNull();
  expect(engine.lastTick).not.toBeNull();expect(io.outbox).toEqual([]);
});
it('keeps one transport reconciliation across overlapping and subsequent ticks until it settles',async()=>{
  await Promise.all([engine.tick(),engine.tick(),engine.tick()]);
  await engine.tick();await engine.tick();
  expect(io.recover).toHaveBeenCalledTimes(1);expect(io.schedule).toHaveBeenCalledTimes(3);
  const pending=engine.transportRecoverySync;resolveInspection([]);await pending;
  expect(engine.transportRecoverySync).toBeNull();
  await engine.tick();expect(io.recover).toHaveBeenCalledTimes(2);
});
it('shutdown waits for the existing inspection without starting another reconciliation or admission',async()=>{
  await engine.tick();const scheduled=io.schedule.mock.calls.length;
  const active=new AbortController();engine.active.set('active-runtime',active);
  let completed=false;const stopping=engine.stop().then(()=>{completed=true;});
  await vi.waitFor(()=>expect(io.bossStop).toHaveBeenCalled(),{timeout:1000});
  expect(engine.stopped).toBe(true);expect(active.signal.aborted).toBe(true);expect(completed).toBe(false);
  await engine.tick();await engine.tick();
  expect(io.recover).toHaveBeenCalledTimes(1);expect(io.schedule).toHaveBeenCalledTimes(scheduled);
  resolveInspection([]);await stopping;
  expect(completed).toBe(true);expect(engine.transportRecoverySync).toBeNull();
  await engine.tick();expect(io.recover).toHaveBeenCalledTimes(1);expect(io.schedule).toHaveBeenCalledTimes(scheduled);
});
it('contains a rejected inspection with a safe error and releases the single-flight owner',async()=>{
  await engine.tick();const pending=engine.transportRecoverySync;
  rejectInspection(Error('PRIVATE_SSH_ERROR_WITH_CREDENTIAL_SENTINEL'));await pending;
  expect(engine.transportRecoverySync).toBeNull();expect(engine.lastError).toBeTruthy();
  expect(engine.lastError).not.toContain('PRIVATE_SSH_ERROR');expect(engine.lastError).not.toContain('CREDENTIAL_SENTINEL');
  expect(io.schedule).toHaveBeenCalledTimes(1);expect(io.recover).toHaveBeenCalledTimes(1);
});
it('does not erase an immediate transport failure when the ordinary admission tick succeeds',async()=>{
  io.recover.mockRejectedValueOnce(Error('PRIVATE_TRANSPORT_ERROR'));
  await engine.tick();
  expect(engine.lastError).toBe(engine.transportRecoveryError);
  expect(engine.transportRecoveryError).toBeTruthy();expect(engine.lastError).not.toContain('PRIVATE_TRANSPORT_ERROR');
  io.recover.mockResolvedValueOnce([]);
  await engine.tick();
  expect(engine.transportRecoveryError).toBeNull();expect(engine.lastError).toBeNull();
});

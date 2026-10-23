import { beforeAll, beforeEach, afterAll, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, readFile, unlink, symlink } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate, lock, tx } from '../server/db.js';
import { schedule,failRun } from '../server/service.js';
import {quotaFor} from '../server/accounts.js';
import {assertRetryProgress} from '../server/retry-progress.js';
import {providerAvailabilityFailure} from '../shared/recovery.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';
import { hash } from '../server/memory.js';
import { config } from '../server/config.js';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { seedRegistry } from '../server/registry.js';
import { readAcceptedAuxiliaryProof } from '../server/accepted-auxiliary.js';
import {
  ContinuationSupervisor,
  resumeFailedHarness,
  chooseRepairRoute,
  routeVerificationRepair,
  queueVerificationRecheckInTransaction,
  repeatedWithoutProgress,
  repairInstruction,
  inspectStopped,
  assertStopped,
  reconcileUnstartedHarnessRun,
} from '../server/continuation.js';
it.each(['OPENCODE_SANDBOX_OOM', 'HARNESS_SANDBOX_OOM'])('keeps %s recovery bounded without concealing the failing regression', (code) => {
  const instruction = repairInstruction(code);
  expect(instruction).toContain('systemd confirmou oom-kill');
  expect(instruction).toContain('/dev/full');
  expect(instruction).toContain('fixture regular local');
  expect(instruction).toContain('preservando a intenção e assertivas');
  expect(instruction).toContain('Não aumente recursos');
});
it('ordinary continuation names the assigned suite while retaining real failed test results', () => {
  const instruction = repairInstruction(
    'OPENCODE_DELIVERY_INVALID_REPORT_TEST_FILE',
    'qa-protocol/assigned.test.mjs',
  );
  expect(instruction).toContain('qa-protocol/assigned.test.mjs');
  expect(instruction).toContain('nunca altere o comando registrado para simular');
  expect(instruction).toContain('não transforme testes que falham em aprovados');
});
beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installTaskExecutionMetadata();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(
    "DELETE FROM settings WHERE id LIKE 'autonomy:%' OR id LIKE 'exclusive-workstreams:%'",
  );
});
afterAll(async () => {
  await pool.query("DELETE FROM settings WHERE id LIKE 'autonomy:%'");
  await pool.end();
});
async function fixture() {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID(),
    child = randomUUID();
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id) VALUES('repair-account','Test','codex','gpt-6-luna','[\"gpt-6-luna\"]',true,5,true,'test-route')",
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Test','Test','continuation-test',$2)",
    [goal, 'goal-' + goal],
  );
  await pool.query(
    "INSERT INTO settings VALUES('autonomy:continuation-test','{\"enabled\":true}')",
  );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count,allowed_accounts) VALUES($1,$2,'fix','Fix','Repair','qa.backend','failed','[]',$3,3,'[\"repair-account\"]')",
    [
      task,
      goal,
      JSON.stringify({
        kind: 'harness',
        profile: 'code',
        workspace: 'continuation-test/fix',
        baseWorkspace: 'continuation-test/game',
      }),
    ],
  );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'qa','QA','QA','qa.backend','blocked_failed','[]')",
    [child, goal],
  );
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [child, task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state,error_code,finished_at) VALUES($1,$2,3,$3,'old-account','old-model','harness',1,'failed','AUTH_REQUIRED',now())",
    [run, task, fence],
  );
  const proof = {
    runId: run,
    workspace: 'continuation-test/fix',
    checkedAt: new Date().toISOString(),
    candidate: { patch: 'diff' },
    process: {
      unit: 'agentos-run-' + run,
      stopped: true,
      activeState: 'inactive',
      mainPid: 0,
      populated: false,
    },
  };
  return { goal, task, run, fence, child, proof };
}
it('uses not-started inspection only with the matching persisted controller receipt', async () => {
  const f = await fixture();
  await pool.query("UPDATE runs SET error_code='CONTEXT_TOO_LARGE' WHERE id=$1", [f.run]);
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",
    [f.task, JSON.stringify({ runId: f.run, confirmed: true, code: 'CONTEXT_TOO_LARGE' })],
  );
  const execute = vi.fn().mockResolvedValue({ stdout: JSON.stringify(f.proof), stderr: '' });
  await inspectStopped(f.run, false, execute);
  expect(execute.mock.calls[0][1].slice(-3)).toEqual([
    f.run,
    '--not-started',
    'continuation-test/fix',
  ]);
});
it('inspects and closes a never-started reconciling attempt without scheduling or accepting another attempt',async()=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET state='reconciling',error_code='HEARTBEAT_LOST',finished_at=NULL WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET state='reconciling' WHERE id=$1",[f.task]);
  await pool.query("INSERT INTO leases VALUES('test-slot',$1)",[f.run]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",[f.task,JSON.stringify({runId:f.run,confirmed:true})]);
  const execute=vi.fn().mockResolvedValue({stdout:JSON.stringify({...f.proof,state:'not_started'}),stderr:''});
  const inspected=await inspectStopped(f.run,false,execute);
  expect(execute.mock.calls[0][1].slice(-3)).toEqual([f.run,'--not-started',f.proof.workspace]);
  const inspect=vi.fn().mockImplementation(async()=>({...inspected,checkedAt:new Date().toISOString()}));
  const before=(await pool.query('SELECT instruction_snapshot,checkpoint FROM runs WHERE id=$1',[f.run])).rows[0];
  expect(await reconcileUnstartedHarnessRun(f.run,inspect)).toBe(true);
  expect(inspect).toHaveBeenCalledTimes(2);
  expect((await pool.query('SELECT state,error_code,instruction_snapshot,checkpoint FROM runs WHERE id=$1',[f.run])).rows[0])
    .toEqual({...before,state:'failed',error_code:'HEARTBEAT_LOST'});
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual({state:'failed',attempt_count:3});
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect(await reconcileUnstartedHarnessRun(f.run,inspect)).toBe(false);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='UNSTARTED_RUN_RECONCILED'")).rows[0].n).toBe(1);
});
it.each(['receipt_missing','cancel_requested','runtime_activity','goal_paused','autonomy_disabled','stale_proof','process_live'])
('keeps an uncertain reservation when never-started reconciliation has %s',async(reason)=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET state='reconciling',error_code='HEARTBEAT_LOST',finished_at=NULL WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET state='reconciling' WHERE id=$1",[f.task]);
  await pool.query("INSERT INTO leases VALUES('test-slot',$1)",[f.run]);
  if(reason!=='receipt_missing')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",[f.task,JSON.stringify({runId:f.run,confirmed:true})]);
  if(reason==='cancel_requested'||reason==='runtime_activity')await pool.query('INSERT INTO events(kind,entity_id,payload) VALUES($1,$2,$3)',[reason==='cancel_requested'?'CANCEL_REQUESTED':'MODEL_RESPONSE',f.task,JSON.stringify({runId:f.run})]);
  if(reason==='goal_paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(reason==='autonomy_disabled')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id='autonomy:continuation-test'");
  const inspect=vi.fn().mockImplementation(async()=>({...f.proof,state:'not_started',checkedAt:reason==='stale_proof'?'2020-01-01':new Date().toISOString(),
    process:{...f.proof.process,...(reason==='process_live'?{mainPid:42,populated:true,stopped:false}:{})}}));
  if(['stale_proof','process_live'].includes(reason))await expect(reconcileUnstartedHarnessRun(f.run,inspect)).rejects.toThrow('STOP_UNCONFIRMED');
  else expect(await reconcileUnstartedHarnessRun(f.run,inspect)).toBe(false);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('reconciling');
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1',[f.run])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='UNSTARTED_RUN_RECONCILED'")).rowCount).toBe(0);
});
it('confirms the immutable attempt workspace when a continuation already points to its next workspace', async () => {
  const f = await fixture();
  const run = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  expect(run.instruction_snapshot.task.execution.workspace).toBe(f.proof.workspace);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"continuation-test/next\"') WHERE id=$1", [f.task]);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(() => assertStopped(f.proof, run, task)).not.toThrow();
  expect(() => assertStopped({ ...f.proof, workspace: task.execution.workspace }, run, task))
    .toThrow('STOP_UNCONFIRMED');
  expect(() => assertStopped({ ...f.proof, process: { ...f.proof.process, populated: true } }, run, task))
    .toThrow('STOP_UNCONFIRMED');
  expect(() => assertStopped({ ...f.proof, checkedAt: '2020-01-01' }, run, task))
    .toThrow('STOP_UNCONFIRMED');
  let invalidSnapshot: any;
  try { assertStopped(f.proof, { ...run, instruction_snapshot: { ...run.instruction_snapshot,
    task: { ...run.instruction_snapshot.task, id: randomUUID() } } }, task); }
  catch (error) { invalidSnapshot = error; }
  expect(invalidSnapshot).toMatchObject({ code: 'INSTRUCTION_SNAPSHOT_INVALID' });
});
it('a context error, an unconfirmed receipt or a receipt for another run never authorizes the fallback', async () => {
  const f = await fixture();
  await pool.query("UPDATE runs SET error_code='CONTEXT_TOO_LARGE' WHERE id=$1", [f.run]);
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2),('HARNESS_NOT_STARTED',$1,$3)",
    [
      f.task,
      JSON.stringify({ runId: f.run, confirmed: false }),
      JSON.stringify({ runId: randomUUID(), confirmed: true }),
    ],
  );
  const execute = vi.fn().mockResolvedValue({ stdout: JSON.stringify(f.proof), stderr: '' });
  await inspectStopped(f.run, false, execute);
  expect(execute.mock.calls[0][1]).not.toContain('--not-started');
});
it('does not use the prelaunch fallback for a running record or the OpenCode inspector', async () => {
  const f = await fixture();
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",
    [f.task, JSON.stringify({ runId: f.run, confirmed: true })],
  );
  const execute = vi.fn().mockResolvedValue({ stdout: JSON.stringify(f.proof), stderr: '' });
  await inspectStopped(f.run, true, execute);
  expect(execute.mock.calls[0][1]).toContain('/opt/agentos-bridge/inspect-opencode-recovery.mjs');
  expect(execute.mock.calls[0][1]).not.toContain('--not-started');
  await pool.query("UPDATE runs SET state='running',finished_at=null WHERE id=$1", [f.run]);
  await inspectStopped(f.run, false, execute);
  expect(execute.mock.calls[1][1]).not.toContain('--not-started');
});
it('inspects a never-started OpenCode run through the real unit probe only with its native receipt', async () => {
  const f = await fixture();
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",
    [f.task],
  );
  await pool.query(
    "UPDATE runs SET runtime='opencode',error_code='SUPERVISION_CONTEXT_CHANGED' WHERE id=$1",
    [f.run],
  );
  const execute = vi.fn().mockResolvedValue({
    stdout: JSON.stringify({ ...f.proof, state: 'not_started' }),
    stderr: '',
  });
  await inspectStopped(f.run, true, execute);
  expect(execute.mock.calls[0][1]).not.toContain('--not-started');
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('OPENCODE_NOT_STARTED',$1,$2)",
    [f.task, JSON.stringify({ runId: f.run, confirmed: true })],
  );
  const proof = await inspectStopped(f.run, true, execute);
  expect(execute.mock.calls[1][1]).toContain('/opt/agentos-bridge/inspect-stopped-run.mjs');
  expect(execute.mock.calls[1][1].slice(-3)).toEqual([
    f.run,
    '--not-started',
    'continuation-test/fix',
  ]);
  expect(proof).toMatchObject({
    runtime: 'opencode',
    notStarted: true,
    state: { state: 'not_started', code: 'SUPERVISION_CONTEXT_CHANGED' },
    process: f.proof.process,
  });
  execute.mockResolvedValueOnce({
    stdout: JSON.stringify({ ...f.proof, state: 'failed' }),
    stderr: '',
  });
  await expect(inspectStopped(f.run, true, execute)).rejects.toThrow(
    'NOT_STARTED_RECEIPT_CONFLICT',
  );
});
it('automatically changes an unavailable route and preserves evidence while unblocking dependants', async () => {
  const f = await fixture();
  const supervisor = new ContinuationSupervisor(async () => f.proof);
  await supervisor.scan();
  const t = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(t).toMatchObject({
    state: 'retry_scheduled',
    account_id: 'repair-account',
    model: 'gpt-6-luna',
    max_attempts: 4,
  });
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.child])).rows[0].state).toBe(
    'blocked_dependencies',
  );
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(2);
  expect(
    (await pool.query('SELECT checkpoint FROM runs WHERE id=$1', [f.run])).rows[0].checkpoint.kind,
  ).toBe('automatic_recovery');
});
it('rejects stale stop proof and deduplicates simultaneous recovery', async () => {
  const f = await fixture();
  await expect(
    resumeFailedHarness(f.run, f.fence, { ...f.proof, checkedAt: '2020-01-01' }),
  ).rejects.toThrow('STOP_UNCONFIRMED');
  const r = await Promise.allSettled([
    resumeFailedHarness(f.run, f.fence, f.proof),
    resumeFailedHarness(f.run, f.fence, f.proof),
  ]);
  expect(r.filter((x) => x.status === 'fulfilled')).toHaveLength(1);
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1', [f.run])).rowCount).toBe(2);
});
it('leaves uncertain live processes untouched when stop cannot be confirmed', async () => {
  const f = await fixture();
  const supervisor = new ContinuationSupervisor(async () => {
    throw Error('STOP_UNCONFIRMED');
  });
  await supervisor.scan();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'failed',
  );
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0].execution
      .continuation.state,
  ).toBe('checking');
  expect((await pool.query('SELECT * FROM artifacts')).rowCount).toBe(0);
});
it('respects an explicit cancellation and never restarts it', async () => {
  const f = await fixture();
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [
    f.task,
    JSON.stringify({ runId: f.run }),
  ]);
  let inspected = false;
  await new ContinuationSupervisor(async () => {
    inspected = true;
    return f.proof;
  }).scan();
  expect(inspected).toBe(false);
});
it('never routes to unauthorized or exhausted accounts', () => {
  const a = {
    id: 'x',
    provider: 'codex',
    enabled: true,
    bridge_id: 'x',
    model: 'gpt-6-luna',
    models: ['gpt-6-luna'],
    allow_unknown: true,
  };
  expect(chooseRepairRoute({ allowed_accounts: ['other'], execution: {} }, {}, [a])).toBeNull();
  expect(
    chooseRepairRoute({ execution: {} }, {}, [{ ...a, quota: { ordinaryUsageAllowed: false } }]),
  ).toBeNull();
});
it('repairs on the only route when there is nothing to switch to', () => {
  // One connected account exposing one model is a whole installation, not a
  // misconfiguration. Requiring a different route after the third attempt left
  // such a host with no candidate, so the task waited on an account that was
  // connected the entire time.
  const only = {
    id: 'solo',
    provider: 'openai',
    enabled: true,
    bridge_id: 'solo',
    model: 'glm-5.3-flash',
    models: ['glm-5.3-flash'],
    allow_unknown: true,
  };
  const task = { attempt_count: 3, stage: 'work', execution: { profile: 'code' } };
  const run = { account_id: 'solo', model: 'glm-5.3-flash', error_code: 'RESULT_FORMAT' };
  expect(chooseRepairRoute(task, run, [only])).toMatchObject({
    accountId: 'solo',
    model: 'glm-5.3-flash',
  });
});
it('still prefers a different route when one exists', () => {
  const base = { provider: 'openai', enabled: true, allow_unknown: true };
  const used = { ...base, id: 'used', bridge_id: 'used', model: 'glm-5.3-flash', models: ['glm-5.3-flash'] };
  const spare = { ...base, id: 'spare', bridge_id: 'spare', model: 'gpt-6-luna', models: ['gpt-6-luna'] };
  const task = { attempt_count: 3, stage: 'work', execution: { profile: 'code' } };
  const run = { account_id: 'used', model: 'glm-5.3-flash', error_code: 'RESULT_FORMAT' };
  expect(chooseRepairRoute(task, run, [used, spare])).toMatchObject({ accountId: 'spare' });
});
it('honours a route cooldown before falling back to the only route', () => {
  const only = {
    id: 'solo',
    provider: 'openai',
    enabled: true,
    bridge_id: 'solo',
    model: 'glm-5.3-flash',
    models: ['glm-5.3-flash'],
    allow_unknown: true,
  };
  const until = new Date(Date.now() + 600000).toISOString();
  const task = {
    attempt_count: 3,
    stage: 'work',
    execution: {
      profile: 'code',
      continuationRoutes: [{ accountId: 'solo', model: 'glm-5.3-flash', until }],
    },
  };
  const run = { account_id: 'solo', model: 'glm-5.3-flash', error_code: 'RESULT_FORMAT' };
  expect(chooseRepairRoute(task, run, [only])).toBeNull();
});

async function candidateArtifact(
  runId: string,
  candidateDigest: string,
  baseDigest = candidateDigest,
) {
  const id = randomUUID(),
    bytes = Buffer.from(JSON.stringify({ baseDigest, candidateDigest }));
  await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
  await writeFile(path.join(config.dataDir, 'artifacts', id), bytes, { flag: 'wx' });
  const ref = {
    id,
    name: 'candidate.json',
    digest: hash(bytes),
    run_id: runId,
    size: bytes.length,
  };
  await pool.query(
    "INSERT INTO artifacts(id,run_id,name,digest,size,mime) VALUES($1,$2,$3,$4,$5,'application/json')",
    [id, runId, ref.name, ref.digest, ref.size],
  );
  return ref;
}
async function verificationFixture() {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [f.task]);
  await pool.query(
    "UPDATE runs SET state='submitted',error_code=NULL,started_at=now()-interval '1 minute' WHERE id=$1",
    [f.run],
  );
  const targetCandidate = await candidateArtifact(f.run, hash('candidate-a'), hash('baseline'));
  await pool.query("UPDATE tasks SET state='submitted',attempt_count=1,execution=$2 WHERE id=$1", [
    f.child,
    JSON.stringify({
      kind: 'harness',
      readOnlySource: true,
      workspace: 'continuation-test/qa',
      sourceWorkspace: 'continuation-test/fix',
    }),
  ]);
  const qaRun = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",
    [qaRun, f.child, randomUUID()],
  );
  const qaCandidate = await candidateArtifact(qaRun, hash('candidate-a'));
  const contextId = randomUUID(),
    manifest = { dependencyArtifacts: [{ ...targetCandidate, task_id: f.task }] };
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'immutable QA input')",
    [contextId, qaRun, JSON.stringify(manifest)],
  );
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [qaRun, contextId]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
    [
      randomUUID(),
      f.child,
      qaRun,
      JSON.stringify([{ type: 'json_field', passed: false, label: 'Wallet conservation' }]),
    ],
  );
  return { ...f, qaRun, qaCandidate, targetCandidate, contextId, manifest };
}
it('routes failed read-only QA to its implementation and preserves acceptance history', async () => {
  const f = await verificationFixture();
  expect(await routeVerificationRepair(f.child)).toBe(f.task);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.child])).rows[0].state).toBe(
    'blocked_dependencies',
  );
  const target = (await pool.query('SELECT state,execution FROM tasks WHERE id=$1', [f.task]))
    .rows[0];
  expect(target.state).toBe('failed');
  expect(target.execution.repairFeedback.sourceRunId).toBe(f.qaRun);
  expect(target.execution.repairFeedback).toMatchObject({
    targetRunId: f.run,
    targetAttempt: 3,
    candidateDigest: hash('candidate-a'),
  });
  expect((await pool.query('SELECT count(*) FROM acceptances')).rows[0].count).toBe('1');
});

async function acceptedAuxiliary(f: Awaited<ReturnType<typeof fixture>>, sourceRunId: string, stage = 'work', legacyMismatch?: string) {
  const input = (await pool.query("SELECT id,name,digest,size,run_id FROM artifacts WHERE run_id=$1 AND name='candidate.json'", [sourceRunId])).rows[0]
    ?? await candidateArtifact(sourceRunId, hash('frozen-source'), hash('frozen-base'));
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), f.task, sourceRunId]);
  const id = randomUUID(), runId = randomUUID(), contextId = randomUUID();
  const sourceAttempt = (await pool.query('SELECT attempt FROM runs WHERE id=$1', [sourceRunId])).rows[0].attempt;
  const provenance = { role: 'accepted_source', taskId: f.task, runId: sourceRunId,
    artifactId: input.id, name: input.name, digest: input.digest };
  if (legacyMismatch && ['runId', 'artifactId', 'taskId'].includes(legacyMismatch)) (provenance as any)[legacyMismatch] = randomUUID();
  if (legacyMismatch === 'digest') provenance.digest = hash('wrong-source');
  if (legacyMismatch === 'role') provenance.role = 'rejected_evidence';
  const execution = { kind: 'opencode', managed: true, workKind: 'repair',
    workspace: 'continuation-test/aux-' + id, baseWorkspace: 'continuation-test/game',
    sourceWorkspace: 'continuation-test/fix', writePaths: ['qa-protocol/', 'evidence/'],
    ...(legacyMismatch ? { backlogContext: { version: 1, sourceTaskId: f.task,
      sourceAttempt: sourceAttempt + (legacyMismatch === 'attempt' ? 1 : 0), provenance: [provenance] } } : {}),
    inputs: [{ id: input.id, name: input.name, digest: input.digest,
      base64: (await readFile(path.join(config.dataDir, 'artifacts', input.id))).toString('base64') }] };
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count,stage)
    VALUES($1,$2,$6,'Pinned auxiliary','A fact about the frozen source','qa.backend','accepted',$3,$4,1,$5)`,
    [id, f.goal, JSON.stringify([{ id: 'result', type: 'artifact', name: 'candidate.json', label: 'Actual output' }]), JSON.stringify(execution), stage, 'aux-' + id]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [id, f.task]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'opencode',1,'submitted',now())",
    [runId, id, randomUUID()]);
  const snapshot = (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [runId])).rows[0].instruction_snapshot;
  const manifest = { executionInputs: snapshot.executionInputs, dependencyArtifacts: [],
    taskInstructions: { taskId: id, goalId: f.goal, revisionId: snapshot.revisionId,
      descriptionHash: hash(snapshot.task.description) } };
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Frozen evidence')", [contextId, runId, JSON.stringify(manifest)]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [runId, contextId]);
  const output = await candidateArtifact(runId, hash('aux-output'));
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,$4,'test')",
    [randomUUID(), id, runId, JSON.stringify([{ id: 'result', passed: true, type: 'artifact' }])]);
  return { id, runId, contextId, input, output, manifest };
}

it.each(['qa-routing', 'harness-resume'])('preserves immutable accepted auxiliary evidence during %s but invalidates integration and QA', async mode => {
  const f = mode === 'qa-routing' ? await verificationFixture() : await fixture();
  let sourceRunId = f.run;
  if (mode === 'harness-resume') {
    sourceRunId = randomUUID();
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,2,$3,'harness',1,'submitted',now())",
      [sourceRunId, f.task, randomUUID()]);
  }
  const aux = await acceptedAuxiliary(f, sourceRunId);
  const integrate = await acceptedAuxiliary(f, sourceRunId, 'integrate');
  const verify = await acceptedAuxiliary(f, sourceRunId, 'verify');
  const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [aux.id])).rows[0];
  const acceptanceBefore = (await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [aux.runId])).rows;
  expect(await readAcceptedAuxiliaryProof(pool, aux.id)).toMatchObject({ eligible: true, runId: aux.runId, attempt: 1 });
  if (mode === 'qa-routing') await routeVerificationRepair(f.child);
  else await resumeFailedHarness(f.run, f.fence, f.proof);
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1', [aux.id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1', [aux.runId])).rows).toEqual(acceptanceBefore);
  expect((await pool.query('SELECT state FROM tasks WHERE id=ANY($1::uuid[])', [[integrate.id, verify.id]])).rows)
    .toEqual([{ state: 'blocked_dependencies' }, { state: 'blocked_dependencies' }]);
  expect((await pool.query("SELECT payload->'preservedAuxiliaryTaskIds' ids FROM events WHERE kind=$1 ORDER BY created_at DESC LIMIT 1",
    [mode === 'qa-routing' ? 'VERIFICATION_REPAIR_ROUTED' : 'CONTINUATION_QUEUED'])).rows[0].ids).toEqual([aux.id]);
});

it.each(['valid', 'runId', 'artifactId', 'taskId', 'attempt', 'digest', 'role'])
('requires exact frozen provenance for a legacy source without an instruction snapshot: %s', async mismatch => {
  const f = await verificationFixture();
  const aux = await acceptedAuxiliary(f, f.run, 'work', mismatch);
  // Model only the historic null source snapshot returned by PostgreSQL. All
  // task/run/context/acceptance/input/output evidence remains real and immutable.
  const historicalDB = { query: async (sql: string, args: any[]) => {
    const result = await pool.query(sql, args);
    return sql.includes('source_state') ? { ...result, rows: result.rows.map(row => ({ ...row, source_workspace: null })) } : result;
  } } as any;
  expect((await readAcceptedAuxiliaryProof(historicalDB, aux.id)).eligible).toBe(mismatch === 'valid');
});

it.each(['contract', 'resources', 'allowlist', 'revision', 'source', 'source-state', 'inline-bytes', 'output-bytes', 'output-symlink', 'manifest', 'revoked-acceptance', 'cancel', 'live-actor'])
('does not preserve an accepted auxiliary when its %s proof changed', async reason => {
  const f = await verificationFixture();
  const aux = await acceptedAuxiliary(f, f.run);
  if (reason === 'contract') await pool.query("UPDATE tasks SET description=description||' changed' WHERE id=$1", [aux.id]);
  if (reason === 'resources') await pool.query("UPDATE tasks SET resources='[\"new-resource\"]' WHERE id=$1", [aux.id]);
  if (reason === 'allowlist') await pool.query("UPDATE tasks SET allowed_accounts='[\"new-account\"]' WHERE id=$1", [aux.id]);
  if (reason === 'revision') {
    const origin = randomUUID(), revision = randomUUID(), text = 'A fact about the frozen source';
    await pool.query("INSERT INTO task_instruction_origins(id,task_id,goal_id,baseline,baseline_hash,original,original_hash,proof) VALUES($1,$2,$3,$4,$5,$4,$5,'{}')",
      [origin, aux.id, f.goal, text, hash(text)]);
    await pool.query("INSERT INTO task_instruction_revisions(id,task_id,goal_id,origin_id,number,operation_key,instruction,instruction_hash,provenance,rendered,rendered_hash) VALUES($1,$2,$3,$4,1,'test-revision',$5,$6,'{}',$5,$6)",
      [revision, aux.id, f.goal, origin, text, hash(text)]);
    await pool.query('UPDATE tasks SET instruction_revision_id=$2 WHERE id=$1', [aux.id, revision]);
  }
  if (reason === 'source') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{sourceWorkspace}','\"continuation-test/other\"') WHERE id=$1", [aux.id]);
  if (reason === 'source-state') await pool.query("UPDATE runs SET state='failed' WHERE id=$1", [f.run]);
  if (reason === 'inline-bytes') await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs,0,base64}','\"YmFk\"') WHERE id=$1", [aux.id]);
  if (reason === 'output-bytes') await writeFile(path.join(config.dataDir, 'artifacts', aux.output.id), 'corrupt');
  if (reason === 'output-symlink') {
    const artifactPath = path.join(config.dataDir, 'artifacts', aux.output.id);
    await writeFile(artifactPath + '-target', await readFile(artifactPath));
    await unlink(artifactPath);
    await symlink(artifactPath + '-target', artifactPath);
  }
  if (reason === 'manifest') await pool.query("UPDATE contexts SET manifest=jsonb_set(manifest,'{executionInputs}','[]') WHERE id=$1", [aux.contextId]);
  if (reason === 'revoked-acceptance') await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','test',now()+interval '1 second')", [randomUUID(), aux.id, aux.runId]);
  if (reason === 'cancel') await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [aux.id, JSON.stringify({ runId: aux.runId })]);
  if (reason === 'live-actor') await pool.query("UPDATE runs SET state='running' WHERE id=$1", [aux.runId]);
  expect((await readAcceptedAuxiliaryProof(pool, aux.id)).eligible).toBe(false);
  if (['live-actor', 'cancel'].includes(reason)) {
    await expect(routeVerificationRepair(f.child)).rejects.toThrow('BRANCH_BUSY');
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [aux.id])).rows[0].state).toBe('accepted');
  } else if (reason !== 'source-state') {
    await routeVerificationRepair(f.child);
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [aux.id])).rows[0].state).toBe('blocked_dependencies');
  }
});

async function newerCandidate(
  f: Awaited<ReturnType<typeof verificationFixture>>,
  digest = hash('candidate-b'),
) {
  const runId = randomUUID();
  await pool.query("UPDATE tasks SET state='accepted',attempt_count=4 WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at,finished_at) VALUES($1,$2,4,$3,'harness',1,'submitted',now(),now())",
    [runId, f.task, randomUUID()],
  );
  const candidate = await candidateArtifact(runId, digest, hash('baseline'));
  return { runId, candidate };
}
it('an older QA cannot reopen a newer accepted implementation, even with unchanged candidate bytes', async () => {
  const f = await verificationFixture();
  const current = await newerCandidate(f, hash('candidate-a'));
  expect(await routeVerificationRepair(f.child)).toBeNull();
  expect(
    (await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1', [f.task])).rows[0],
  ).toEqual({ state: 'accepted', attempt_count: 4 });
  const qa = (
    await pool.query('SELECT state,execution,max_attempts FROM tasks WHERE id=$1', [f.child])
  ).rows[0];
  expect(qa).toMatchObject({
    state: 'blocked_dependencies',
    execution: {
      workspace: 'continuation-test/oqa-' + f.qaRun,
      verificationRecheck: { reason: 'SOURCE_ATTEMPT_CHANGED', targetRunId: current.runId },
    },
  });
  expect(
    (await pool.query('SELECT manifest,content FROM contexts WHERE id=$1', [f.contextId])).rows[0],
  ).toEqual({ manifest: f.manifest, content: 'immutable QA input' });
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rowCount,
  ).toBe(0);
});
it('a fresh input manifest does not disguise an older physical QA snapshot', async () => {
  const f = await verificationFixture();
  const bytes = Buffer.from(
    JSON.stringify({ baseDigest: hash('candidate-old'), candidateDigest: hash('candidate-old') }),
  );
  await writeFile(path.join(config.dataDir, 'artifacts', f.qaCandidate.id), bytes);
  await pool.query('UPDATE artifacts SET digest=$2,size=$3 WHERE id=$1', [
    f.qaCandidate.id,
    hash(bytes),
    bytes.length,
  ]);
  expect(await routeVerificationRepair(f.child)).toBeNull();
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.child])).rows[0].execution
      .verificationRecheck.reason,
  ).toBe('TESTED_CANDIDATE_CHANGED');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'accepted',
  );
});
it('deduplicates parallel negative QA routes by source run, target and candidate', async () => {
  const f = await verificationFixture();
  const results = await Promise.allSettled(
    Array.from({ length: 6 }, () => routeVerificationRepair(f.child)),
  );
  expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
  expect(
    (await pool.query("SELECT payload FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rows,
  ).toHaveLength(1);
  const payload = (
    await pool.query("SELECT payload FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")
  ).rows[0].payload;
  expect(payload).toMatchObject({
    sourceRunId: f.qaRun,
    targetRunId: f.run,
    targetAttempt: 3,
    candidateArtifact: { id: f.targetCandidate.id, digest: f.targetCandidate.digest },
  });
});
it('a consumed legacy QA receipt schedules a new QA rather than reopening corrected integration', async () => {
  const f = await verificationFixture();
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('VERIFICATION_REPAIR_ROUTED',$1,$2)",
    [f.child, JSON.stringify({ sourceRunId: f.qaRun, targetTaskId: f.task })],
  );
  expect(await routeVerificationRepair(f.child)).toBeNull();
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rowCount,
  ).toBe(1);
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
  ).toBe(1);
  await expect(routeVerificationRepair(f.child)).rejects.toThrow('TASK_CHANGED');
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED'")).rowCount,
  ).toBe(1);
});
it('a consumed QA does not send the same correction twice while implementation is still waiting', async () => {
  const f = await verificationFixture();
  await routeVerificationRepair(f.child);
  await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1", [f.child]);
  expect(await routeVerificationRepair(f.child)).toBe(f.task);
  expect(
    (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rowCount,
  ).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'failed',
  );
});
it('a genuinely new negative QA can request another repair on the same candidate', async () => {
  const f = await verificationFixture();
  await routeVerificationRepair(f.child);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1", [f.task]);
  await pool.query("UPDATE tasks SET state='submitted',attempt_count=2 WHERE id=$1", [f.child]);
  const runId = randomUUID(),
    contextId = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'submitted')",
    [runId, f.child, randomUUID()],
  );
  await candidateArtifact(runId, hash('candidate-a'));
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'new QA input')",
    [contextId, runId, JSON.stringify(f.manifest)],
  );
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [runId, contextId]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'test')",
    [randomUUID(), f.child, runId, JSON.stringify([{ type: 'json_field', passed: false }])],
  );
  expect(await routeVerificationRepair(f.child)).toBe(f.task);
  const events = (
    await pool.query(
      "SELECT payload FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED' ORDER BY id",
    )
  ).rows;
  expect(events).toHaveLength(2);
  expect(events[0].payload.routingKey).not.toBe(events[1].payload.routingKey);
});
it.each(['context-missing', 'binding-digest', 'qa-corrupt', 'current-corrupt', 'qa-writable'])(
  'does not classify invalid provenance as an old QA: %s',
  async (mode) => {
    const f = await verificationFixture();
    if (mode === 'context-missing')
      await pool.query('UPDATE runs SET context_id=NULL WHERE id=$1', [f.qaRun]);
    if (mode === 'binding-digest')
      await pool.query(
        "UPDATE contexts SET manifest=jsonb_set(manifest,'{dependencyArtifacts,0,digest}',to_jsonb($2::text)) WHERE id=$1",
        [f.contextId, hash('wrong')],
      );
    if (mode === 'qa-corrupt') {
      await newerCandidate(f);
      await writeFile(path.join(config.dataDir, 'artifacts', f.qaCandidate.id), '{}');
    }
    if (mode === 'current-corrupt')
      await writeFile(path.join(config.dataDir, 'artifacts', f.targetCandidate.id), '{}');
    if (mode === 'qa-writable') {
      const bytes = Buffer.from(
        JSON.stringify({ baseDigest: hash('baseline'), candidateDigest: hash('candidate-a') }),
      );
      await writeFile(path.join(config.dataDir, 'artifacts', f.qaCandidate.id), bytes);
      await pool.query('UPDATE artifacts SET digest=$2,size=$3 WHERE id=$1', [
        f.qaCandidate.id,
        hash(bytes),
        bytes.length,
      ]);
    }
    await expect(routeVerificationRepair(f.child)).rejects.toThrow(
      mode === 'context-missing'
        ? 'VERIFICATION_CANDIDATE_UNBOUND'
        : 'VERIFICATION_CANDIDATE_INVALID',
    );
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
      'accepted',
    );
    expect(
      (
        await pool.query(
          "SELECT 1 FROM events WHERE kind IN('VERIFICATION_REPAIR_ROUTED','VERIFICATION_RECHECK_QUEUED')",
        )
      ).rowCount,
    ).toBe(0);
  },
);
it.each(['paused', 'cancelled'])(
  'does not schedule QA or repairs while goal is %s',
  async (state) => {
    const f = await verificationFixture();
    await pool.query('UPDATE goals SET state=$2 WHERE id=$1', [f.goal, state]);
    await expect(routeVerificationRepair(f.child)).rejects.toThrow('GOAL_NOT_ACTIVE');
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
      'accepted',
    );
  },
);
it('a newer live implementation blocks stale QA without changing its leases, fence or workspace', async () => {
  const f = await verificationFixture();
  const current = await newerCandidate(f);
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [current.runId]);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.task]);
  await pool.query("INSERT INTO leases(resource,run_id) VALUES('live-qa-fence',$1)", [
    current.runId,
  ]);
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [current.runId])).rows[0];
  await expect(routeVerificationRepair(f.child)).rejects.toThrow('BRANCH_BUSY');
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [current.runId])).rows[0]).toEqual(
    before,
  );
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [current.runId])).rowCount).toBe(
    1,
  );
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.child])).rows[0].execution
      .workspace,
  ).toBe('continuation-test/qa');
});
it('an integration published before a waiting QA transaction is re-read under the scheduler lock', async () => {
  const f = await verificationFixture();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await lock(client);
    await client.query("UPDATE tasks SET state='accepted',attempt_count=4 WHERE id=$1", [f.task]);
    const current = randomUUID();
    await client.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,4,$3,'harness',1,'submitted')",
      [current, f.task, randomUUID()],
    );
    // The artifact is written before commit but metadata belongs to this transaction.
    const id = randomUUID(),
      bytes = Buffer.from(
        JSON.stringify({ baseDigest: hash('baseline'), candidateDigest: hash('candidate-b') }),
      );
    await writeFile(path.join(config.dataDir, 'artifacts', id), bytes);
    await client.query(
      "INSERT INTO artifacts(id,run_id,name,digest,size,mime) VALUES($1,$2,'candidate.json',$3,$4,'application/json')",
      [id, current, hash(bytes), bytes.length],
    );
    const pending = routeVerificationRepair(f.child);
    await client.query('COMMIT');
    expect(await pending).toBeNull();
    expect(
      (await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1', [f.task])).rows[0],
    ).toEqual({ state: 'accepted', attempt_count: 4 });
    expect(
      (await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED'")).rowCount,
    ).toBe(0);
  } finally {
    await client.query('ROLLBACK');
    client.release();
  }
});

it('new QA actually becomes a queued attempt in the fresh workspace without changing old evidence', async () => {
  const f = await verificationFixture();
  await newerCandidate(f);
  await routeVerificationRepair(f.child);
  const previous = { bridgeHost: config.bridgeHost, remoteExecution: config.remoteExecution };
  try {
    config.bridgeHost = 'test-only-no-executor';
    config.remoteExecution = true;
    expect(await schedule()).toHaveLength(1);
  } finally {
    Object.assign(config, previous);
  }
  const latest = (
    await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [f.child])
  ).rows[0];
  expect(latest).toMatchObject({
    state: 'queued',
    attempt: 2,
    instruction_snapshot: {
      task: { execution: { workspace: 'continuation-test/oqa-' + f.qaRun, readOnlySource: true } },
    },
  });
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.qaRun])).rows[0].state).toBe(
    'submitted',
  );
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1', [f.qaRun])).rowCount).toBe(
    1,
  );
});
it('queueing a replacement never changes a live QA actor or its leases', async () => {
  const f = await verificationFixture();
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.qaRun]);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.child]);
  await pool.query("INSERT INTO leases(resource,run_id) VALUES('live-qa',$1)", [f.qaRun]);
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.qaRun])).rows[0];
  await expect(
    tx((db) =>
      queueVerificationRecheckInTransaction(db, {
        sourceTaskId: f.child,
        sourceRunId: f.qaRun,
        targetTaskId: f.task,
        targetRunId: f.run,
      }),
    ),
  ).rejects.toThrow('TASK_CHANGED');
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.qaRun])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT 1 FROM leases WHERE run_id=$1', [f.qaRun])).rowCount).toBe(1);
});
it('mismatched current source attempt never creates repair or recheck', async () => {
  const f = await verificationFixture();
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.child]);
  await expect(routeVerificationRepair(f.child)).rejects.toThrow('TASK_CHANGED');
  expect(
    (
      await pool.query(
        "SELECT 1 FROM events WHERE kind IN('VERIFICATION_RECHECK_QUEUED','VERIFICATION_REPAIR_ROUTED')",
      )
    ).rowCount,
  ).toBe(0);
});
it('continues new projects by default but respects their explicit manual recovery mode', async () => {
  const f = await fixture();
  await pool.query("DELETE FROM settings WHERE id='autonomy:continuation-test'");
  await pool.query('UPDATE tasks SET recovery=\'{"mode":"manual"}\' WHERE id=$1', [f.task]);
  let calls = 0;
  const supervisor = new ContinuationSupervisor(async () => {
    calls++;
    return f.proof;
  });
  await supervisor.scan();
  expect(calls).toBe(0);
  await pool.query('UPDATE tasks SET recovery=NULL WHERE id=$1', [f.task]);
  await supervisor.scan();
  expect(calls).toBe(1);
});

it('detects repeated unchanged failures rather than imposing a lifetime attempt cap', () => {
  expect(
    repeatedWithoutProgress(
      Array.from({ length: 6 }, () => ({ signature: 'same' })),
      'same',
    ),
  ).toBe(true);
  expect(
    repeatedWithoutProgress(
      [{ signature: 'changed' }, ...Array.from({ length: 5 }, () => ({ signature: 'same' }))],
      'same',
    ),
  ).toBe(false);
});

it('requests real user intervention when all authorized accounts are disconnected', async () => {
  const f = await fixture();
  await pool.query("UPDATE accounts SET enabled=false WHERE id='repair-account'");
  await new ContinuationSupervisor(async () => f.proof).scan();
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.task])).rows[0].execution
      .continuation.state,
  ).toBe('needs_action');
});

it('recovers a quota refusal after six unchanged candidates without treating it as a functional diagnosis',async()=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET error_code='QUOTA_EXCEEDED' WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1",[f.task,
    JSON.stringify({continuation:{runId:f.run,state:'needs_action',message:'Old false stagnation notice'}})]);
  const signature=hash(JSON.stringify(['QUOTA_EXCEEDED','a'.repeat(64)]));
  for(let i=0;i<6;i++)await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CONTINUATION_QUEUED',$1,$2),('EXECUTION_RETRY_ADMITTED',$1,$3)",[
    f.task,JSON.stringify({signature}),JSON.stringify({signature:'previous-functional-signature'})]);
  await new ContinuationSupervisor(async()=>({...f.proof,candidate:{patch:'diff',candidateDigest:'a'.repeat(64)}})).scan();
  expect((await pool.query('SELECT state,account_id FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'retry_scheduled',account_id:'repair-account'});
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'")).rows[0].n).toBe(6);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
});

it('shares actual model quota refusals across tasks without disabling other models or changing quota measurements',async()=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET state='running',account_id='repair-account',model='gpt-6-luna' WHERE id=$1",[f.run]);
  await failRun(f.run,f.fence,'QUOTA_EXCEEDED','Provider rejected quota');
  const account=(await pool.query("SELECT * FROM accounts WHERE id='repair-account'")).rows[0];
  expect(quotaFor(account,'gpt-6-luna')).toMatchObject({status:'exhausted',remaining:null,source:'execution_feedback'});
  expect(quotaFor(account,'other-model').status).toBe('unknown');
  expect(account.execution_feedback['gpt-6-luna'].sourceRunId).toBe(f.run);
  expect(account.quota).toEqual({});
  await pool.query("UPDATE tasks SET allowed_accounts='[\"repair-account\"]' WHERE id=$1",[f.task]);
  await new ContinuationSupervisor(async()=>f.proof).scan();
  const task=(await pool.query('SELECT state,attempt_count,execution FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(task).toMatchObject({state:'failed',attempt_count:3,execution:{continuation:{state:'waiting_quota',owner:'scheduler'}}});
  const expired={...account,execution_feedback:{'gpt-6-luna':{code:'QUOTA_EXCEEDED',blockedUntil:'2020-01-01'}}};
  expect(quotaFor(expired,'gpt-6-luna').status).toBe('unknown');
  expect(quotaFor({...expired,quota:{ordinaryUsageAllowed:false}},'gpt-6-luna').status).toBe('exhausted');
});

it('scheduled recovery waits cannot occupy all thirty scan entries ahead of due work',async()=>{
  const f=await fixture();
  for(let i=0;i<31;i++){
    const id=randomUUID(),runId=randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count,updated_at)
      VALUES($1,$2,$3,'Future retry','Future retry','qa.backend','failed','[]',$4,1,'2020-01-01')`,[
      id,f.goal,'waiting-'+i,JSON.stringify({kind:'harness',baseWorkspace:'test/wait',
        continuation:{runId,state:'waiting_quota',nextAt:new Date(Date.now()+3600000).toISOString()}})]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code) VALUES($1,$2,1,$3,'harness',1,'failed','QUOTA_EXCEEDED')",[runId,id,randomUUID()]);
  }
  const inspect=vi.fn().mockResolvedValue(f.proof);
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('retry_scheduled');
});

it('claims a blocked failed read-only QA for supervision instead of excluding it from the scan',async()=>{
  const f=await verificationFixture();
  const {installSupervision}=await import('../server/supervision.js');await installSupervision();
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+f.goal,JSON.stringify({enabled:true})]);
  await pool.query("UPDATE tasks SET state='blocked_failed' WHERE id=$1",[f.child]);
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.qaRun])).rows[0];
  await new ContinuationSupervisor(async()=>f.proof).scan();
  const qa=(await pool.query('SELECT state,execution FROM tasks WHERE id=$1',[f.child])).rows[0];
  expect(qa).toMatchObject({state:'recovery_blocked',execution:{readOnlySource:true,
    continuation:{runId:f.qaRun,state:'supervising',owner:'supervisor',reasonCode:'VERIFICATION_ROUTING_FAILED'}}});
  expect((await pool.query("SELECT state FROM supervision_cases WHERE task_id=$1",[f.child])).rows).toEqual([{state:'open'}]);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.qaRun])).rows[0]).toEqual(before);
});

it('claims a technical recovery for the supervisor once, without changing the immutable attempt',async()=>{
  const f=await fixture();
  const {installSupervision}=await import('../server/supervision.js');await installSupervision();
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+f.goal,JSON.stringify({enabled:true})]);
  await pool.query("UPDATE runs SET error_code='EXECUTION_FAILED' WHERE id=$1",[f.run]);
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
  const results=await Promise.all([requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Cannot locate implementation; supervisor must resolve the verified dependency'),
    requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Cannot locate implementation; supervisor must resolve the verified dependency')]);
  expect(results[0]?.caseId).toBe(results[1]?.caseId);
  expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
});

it.each([true, false])('routes conflicting positive OpenCode evidence to supervision when enabled=%s', async (enabled) => {
  const f = await fixture();
  const { installSupervision } = await import('../server/supervision.js');
  await installSupervision();
  await pool.query('INSERT INTO settings VALUES($1,$2)', ['supervision:' + f.goal, JSON.stringify({ enabled })]);
  await pool.query("UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1", [f.task,
    JSON.stringify({ kind: 'opencode', repairFeedback: { sourceRunId: randomUUID(), checks: [{ passed: false }] } })]);
  await pool.query("UPDATE runs SET runtime='opencode',state='submitted',error_code=NULL WHERE id=$1", [f.run]);
  const acceptanceId = randomUUID();
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,$4,'test-contract')",
    [acceptanceId, f.task, f.run, JSON.stringify([{ id: 'executed', type: 'json_field', passed: true }])]);
  const beforeRun = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  const beforeAcceptance = (await pool.query('SELECT * FROM acceptances WHERE id=$1', [acceptanceId])).rows[0];
  const proof = { ...f.proof, runtime: 'opencode', state: { state: 'submitted' } };
  await new ContinuationSupervisor(async () => proof).scan();
  const task = (await pool.query('SELECT state,attempt_count,criteria,execution FROM tasks WHERE id=$1', [f.task])).rows[0];
  expect(task).toMatchObject({ state: enabled ? 'recovery_blocked' : 'failed', attempt_count: 3, criteria: [],
    execution: { continuation: { state: enabled ? 'supervising' : 'needs_action',
      owner: enabled ? 'supervisor' : 'controller', reasonCode: 'NO_FAILED_CHECK' } } });
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(beforeRun);
  expect((await pool.query('SELECT * FROM acceptances WHERE id=$1', [acceptanceId])).rows[0]).toEqual(beforeAcceptance);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1', [f.run])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1', [f.task])).rows[0].n).toBe(enabled ? 1 : 0);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation,nextAt}','\"2020-01-01T00:00:00Z\"') WHERE id=$1", [f.task]);
  await new ContinuationSupervisor(async () => proof).scan();
  expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1', [f.task])).rows[0].n).toBe(enabled ? 1 : 0);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_REQUESTED' AND entity_id=$1", [f.task])).rows[0].n).toBe(enabled ? 1 : 0);
});

it.each(['stale','live','paused','cancelled','manual','quota'])('does not claim unsafe or unavailable recovery: %s',async(reason)=>{
  const f=await fixture();
  const {installSupervision}=await import('../server/supervision.js');await installSupervision();
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+f.goal,JSON.stringify({enabled:true})]);
  await pool.query("UPDATE runs SET error_code='EXECUTION_FAILED' WHERE id=$1",[f.run]);
  if(reason==='live'){
    await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.run]);
    await pool.query("UPDATE tasks SET state='reconciling' WHERE id=$1",[f.task]);
  }
  if(reason==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(reason==='cancelled')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
  if(reason==='manual'){
    await pool.query("DELETE FROM settings WHERE id='autonomy:continuation-test'");
    await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  }
  if(reason==='quota')await pool.query("UPDATE runs SET error_code='QUOTA_EXCEEDED' WHERE id=$1",[f.run]);
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
  expect(await requestRecoveryReview(f.task,reason==='stale'?randomUUID():f.run,'VERIFICATION_ROUTING_FAILED','Routing failed')).toBeNull();
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
});

it.each(['AUTH_REQUIRED','QUOTA_EXCEEDED','RATE_LIMIT'])('keeps availability error %s outside functional retry accounting',async(code)=>{
  const f=await fixture();
  const run={...(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0],error_code:code};
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  await assertRetryProgress(pool,task,run,{candidate:{candidateDigest:'a'.repeat(64)}});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'")).rowCount).toBe(0);
  expect(providerAvailabilityFailure(code)).toBe(true);
  expect(providerAvailabilityFailure('ARTIFACT_QUOTA_CORRUPT')).toBe(false);
});

it('can repair failed checks after a collection attempt using the original execution stop proof', async () => {
  const f = await fixture(),
    physicalId = randomUUID();
  await pool.query("UPDATE runs SET state='submitted',error_code=NULL,checkpoint=$2 WHERE id=$1", [
    f.run,
    JSON.stringify({ kind: 'artifact_recovery', sourceRunId: physicalId }),
  ]);
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1", [f.task]);
  const proof = {
    ...f.proof,
    executionRunId: physicalId,
    process: { ...f.proof.process, unit: 'agentos-run-' + physicalId },
  };
  await resumeFailedHarness(f.run, f.fence, proof);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'retry_scheduled',
  );
});

it('provider Retry-After schedules a durable wait without human attention or repeated workspace inspection', async () => {
  const f=await fixture();
  await pool.query("UPDATE runs SET runtime='opencode',error_code='RATE_LIMIT',account_id='repair-account',model='gpt-6-luna' WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",[f.task]);
  const notBefore=new Date(Date.now()+3600000).toISOString();
  const proof={...f.proof,runtime:'opencode',state:{runId:f.run,workspace:f.proof.workspace,model:'gpt-6-luna',
    providerBackoff:{version:1,source:'http_retry_after',runId:f.run,workspace:f.proof.workspace,model:'gpt-6-luna',
      status:429,category:'rate_limit',observedAt:new Date(Date.now()-1000).toISOString(),notBefore}}};
  const inspect=vi.fn().mockResolvedValue(proof);
  const supervisor=new ContinuationSupervisor(inspect);
  await supervisor.scan();
  const continuation=(await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.task])).rows[0].execution.continuation;
  expect(continuation).toMatchObject({state:'waiting_slot',owner:'scheduler',reasonCode:'PROVIDER_RETRY_PENDING',runId:f.run});
  expect(Date.parse(continuation.nextAt)).toBeGreaterThan(Date.parse(notBefore)-2000);
  expect(continuation.humanRequest).toBeUndefined();
  await supervisor.scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1 AND attempt>3',[f.task])).rowCount).toBe(0);
});

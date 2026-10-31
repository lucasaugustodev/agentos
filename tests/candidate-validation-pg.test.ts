import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { pool, migrate, tx } from '../server/db.js';
import { claimWorkerDrain, admitWorkerRestart, admitWorkerServiceRestart } from '../server/worker-rollout.js';
import { seedRegistry } from '../server/registry.js';
import {
  installManagement,
  applyManagementAction,
  managementContract,
} from '../server/management.js';
import { writeArtifact } from '../server/service.js';
import {
  proposeCandidateValidation,
  reconcileCandidateValidations,
  readCandidateValidations,
} from '../server/candidate-validation.js';

let f: any;
const sha = (text: string) => createHash('sha256').update(text).digest('hex');
beforeAll(async () => {
  await migrate();
  await installManagement();
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='model-policy'");
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
  await pool.query("DELETE FROM settings WHERE id LIKE 'candidate-validation:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
  const goal = randomUUID(),
    managerTask = randomUUID(),
    managerRun = randomUUID(),
    managerFence = randomUUID(),
    taskId = randomUUID(),
    runId = randomUUID(),
    runFence = randomUUID(),
    contextVersion = randomUUID(),
    sol = 'test-sol-' + randomUUID();
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency) VALUES($1,$1,'codex','gpt-6.1-sol','[\"gpt-6.1-sol\"]',true,true,4)",
    [sol],
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Validation','Frozen candidate validation','agentos',$1::text)",
    [goal],
  );
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2),($3,$2)', [
    'management:' + goal,
    '{"enabled":true}',
    'workforce:' + goal,
  ]);
  for (const [id, key, state, stage, execution] of [
    [managerTask, 'manager', 'accepted', 'coordinate', null],
    [
      taskId,
      'target',
      'failed',
      'integrate',
      JSON.stringify({
        kind: 'harness',
        workspace: 'agentos/otest',
        baseWorkspace: 'agentos/test',
      }),
    ],
  ]) {
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
   VALUES($1,$2,$3,$3,'Exact immutable contract','qa.backend',$4,$5,'[]',$6,1)`,
      [id, goal, key, state, stage, execution],
    );
  }
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,1,$3,'harness',1,'running',$4,'gpt-6.1-sol')",
    [managerRun, managerTask, managerFence, sol],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",
    [runId, taskId, runFence, contextVersion],
  );
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Immutable context')",
    [contextVersion, runId],
  );
  const candidateDigest = sha('[]'),
    candidate = JSON.stringify({ baseDigest: candidateDigest, candidateDigest, files: [] });
  const artifact = await writeArtifact(runId, runFence, 'candidate.json', candidate);
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [runId]);
  f = {
    goal,
    managerRun,
    managerTask,
    managerFence,
    sol,
    taskId,
    runId,
    runFence,
    contextVersion,
    candidateDigest,
    candidateArtifactId: artifact.id,
    candidateArtifactDigest: artifact.digest,
  };
  await writeArtifact(
    managerRun,
    managerFence,
    'coordination.json',
    JSON.stringify({
      management: {
        summary: 'Approve exact frozen candidate validation',
        capacityReason: 'Existing isolated CI owns the deterministic checks',
        actions: Array.from({ length: 7 }, () => proposal()),
      },
    }),
  );
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [managerRun]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), managerTask, managerRun],
  );
});
afterAll(async () => {
  await pool.query("DELETE FROM settings WHERE id='model-policy'");
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'candidate-validation:%'");
  await pool.end();
});
function proposal(extra: any = {}) {
  return {
    kind: 'request_validation',
    expectedAttempt: f.expectedAttempt ?? 1,
    taskId: f.taskId,
    runId: f.runId,
    runFence: f.runFence,
    contextVersion: f.contextVersion,
    candidateArtifactId: f.candidateArtifactId,
    candidateArtifactDigest: f.candidateArtifactDigest,
    candidateDigest: f.candidateDigest,
    reason: 'Validar publicação e critérios com ação correta no validador isolado. 🚀',
    ...extra,
  };
}
async function approveActions(actions: unknown[]) {
  const attempt = (
    await pool.query('SELECT COALESCE(max(attempt),0)+1 attempt FROM runs WHERE task_id=$1', [
      f.managerTask,
    ])
  ).rows[0].attempt;
  f.managerRun = randomUUID();
  f.managerFence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model) VALUES($1,$2,$3,$4,'harness',1,'running',$5,'gpt-6.1-sol')",
    [f.managerRun, f.managerTask, attempt, f.managerFence, f.sol],
  );
  await writeArtifact(
    f.managerRun,
    f.managerFence,
    'coordination.json',
    JSON.stringify({
      management: {
        summary: 'Approve exact scoped validation',
        capacityReason: 'Existing deterministic isolated CI',
        actions,
      },
    }),
  );
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1", [
    f.managerRun,
  ]);
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), f.managerTask, f.managerRun],
  );
}
async function inspect() {
  return {
    runId: f.runId,
    workspace: (await pool.query('SELECT execution FROM tasks WHERE id=$1', [f.taskId])).rows[0]
      .execution.workspace,
    checkedAt: new Date().toISOString(),
    candidate: { candidateDigest: f.candidateDigest },
    process: {
      unit: 'agentos-run-' + f.runId,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
  };
}
async function action() {
  return proposeCandidateValidation(f.goal, f.managerRun, 0, proposal(), inspect);
}
async function stored(id: string) {
  return (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['candidate-validation:' + id])
  ).rows[0]?.value;
}
const receipt = (request: any, state: string, extra: any = {}) => ({
  actionId: request.actionId,
  candidateDigest: request.candidateDigest,
  requestDigest: sha(
    JSON.stringify(request, (_key, value) =>
      value && !Array.isArray(value) && typeof value === 'object'
        ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
        : value,
    ),
  ),
  state,
  ...extra,
});

const maintenanceRelease = () => ({candidate:'/opt/agentos/releases/managed-'+randomUUID(),digest:'d'.repeat(64)});

it('serializes a committed drain before CI claims, preserving proposed work without dispatch', async () => {
  const proposed = await action(), before = await stored(proposed.actionId!);
  const db = await pool.connect();
  let reconcile: Promise<void> | undefined;
  const calls: string[] = [];
  try {
    await db.query('BEGIN');
    const reservation = await claimWorkerDrain(db, maintenanceRelease());
    expect(reservation.status).toBe('draining');
    reconcile = reconcileCandidateValidations(async mode => { calls.push(mode); throw Error('MUST_NOT_DISPATCH'); }, inspect);
    // The reservation is still uncommitted. A second connection cannot acquire
    // the same transaction lock, irrespective of when its claim query arrives.
    const observer = await pool.connect();
    try {
      await observer.query('BEGIN');
      expect((await observer.query('SELECT pg_try_advisory_xact_lock(4280001) ok')).rows[0].ok).toBe(false);
      await observer.query('ROLLBACK');
    } finally { observer.release(); }
    await db.query('COMMIT');
    await reconcile;
    expect(calls).toEqual([]);
    expect(await stored(proposed.actionId!)).toEqual(before);
    expect(await tx(client => admitWorkerRestart(client, reservation.token!))).toMatchObject({ready:true,counts:{candidate_validations:0}});
  } finally {
    await db.query('ROLLBACK'); db.release();
    if(reconcile) await reconcile;
  }
});

it('a CI claim before the drain blocks restart until its receipt, then defers frozen execution', async () => {
  const proposed = await action();
  let entered!: () => void, finish!: () => void;
  const started = new Promise<void>(resolve => { entered=resolve; });
  const release = new Promise<void>(resolve => { finish=resolve; });
  const calls: string[] = [];
  const reconcile = reconcileCandidateValidations(async(mode, request) => {
    calls.push(mode); entered(); await release;
    return receipt(request,'frozen',{frozenWorkspace:'agentos/ci-frozen-test'});
  },inspect);
  try {
    await started;
    const reservation=await tx(db=>claimWorkerDrain(db,maintenanceRelease()));
    expect((await stored(proposed.actionId!)).state).toBe('freezing');
    expect(await tx(db=>admitWorkerRestart(db,reservation.token!))).toMatchObject({ready:false,counts:{candidate_validations:1}});
    finish(); await reconcile;
    const frozen=await stored(proposed.actionId!);
    expect(frozen.state).toBe('frozen');
    await reconcileCandidateValidations(async mode=>{calls.push(mode);throw Error('MUST_NOT_START');},inspect);
    expect(await stored(proposed.actionId!)).toEqual(frozen);
    expect(calls).toEqual(['freeze']);
    expect(await tx(db=>admitWorkerRestart(db,reservation.token!))).toMatchObject({ready:true});
    await pool.query("DELETE FROM settings WHERE id='maintenance'");
    await reconcileCandidateValidations(async(mode,request)=>{
      calls.push(mode);return receipt(request,'completed',{passed:true});
    },inspect);
    expect(calls).toEqual(['freeze','execute']);
    expect((await stored(proposed.actionId!)).state).toBe('completed');
    expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[f.taskId])).rows[0].n).toBe(0);
  } finally { finish(); await reconcile; }
});

it.each(['running','uncertain'])('continues receipt-only reconciliation for %s ownership during drain',async state=>{
  const proposed=await action(), value=await stored(proposed.actionId!);
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1',['candidate-validation:'+proposed.actionId,
    JSON.stringify({...value,state,claimedAt:'2000-01-01T00:00:00Z'})]);
  const reservation=await tx(db=>claimWorkerDrain(db,maintenanceRelease()));
  expect(await tx(db=>admitWorkerRestart(db,reservation.token!))).toMatchObject({ready:false,counts:{candidate_validations:1}});
  const modes:string[]=[];
  await reconcileCandidateValidations(async(mode,request)=>{modes.push(mode);return receipt(request,'completed',{passed:true});},inspect,()=>false);
  expect(modes).toEqual(['receipt']);
  expect((await stored(proposed.actionId!)).state).toBe('completed');
  expect(await tx(db=>admitWorkerRestart(db,reservation.token!))).toMatchObject({ready:true});
});

it.each(['freezing','running','uncertain','future-active-state',null])('blocks canonical and worker-lane restart for unresolved CI state %s',async state=>{
  const proposed=await action(), value=await stored(proposed.actionId!);
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1',['candidate-validation:'+proposed.actionId,JSON.stringify({...value,state})]);
  const reservation=await tx(db=>claimWorkerDrain(db,maintenanceRelease(),'per_service'));
  expect(await tx(db=>admitWorkerRestart(db,reservation.token!))).toMatchObject({ready:false,counts:{candidate_validations:1}});
  expect(await tx(db=>admitWorkerServiceRestart(db,reservation.token!,'agentos-workers'))).toMatchObject({ready:false,counts:{candidate_validations:1}});
});

it('local shutdown gate defers a proposed effect without clearing ownership or touching its contract',async()=>{
  const proposed=await action(), before=await stored(proposed.actionId!);
  const modes:string[]=[];
  await reconcileCandidateValidations(async mode=>{modes.push(mode);throw Error('NO_DISPATCH');},inspect,()=>false);
  expect(modes).toEqual([]);expect(await stored(proposed.actionId!)).toEqual(before);
});

it('admits one accepted Sol decision and preserves canonical task/candidate/criteria', async () => {
  const before = (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0];
  const results = await Promise.all([action(), action()]);
  expect(results[0]).toEqual(results[1]);
  expect(results[0].code).toBe('VALIDATION_PROPOSED');
  expect((await stored(results[0].actionId!)).request.candidateArtifactDigest).toBe(
    f.candidateArtifactDigest,
  );
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1', [f.taskId])).rows[0]).toEqual(before);
  expect(
    (await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1', [f.taskId]))
      .rows[0].n,
  ).toBe(0);
  expect(managementContract).toContain('request_validation');
});
it('rejects live/cross-goal/stale fence/stale context/candidate changes before dispatch', async () => {
  let calls = 0;
  const observer = async () => {
    calls++;
    return inspect();
  };
  for (const [index, extra] of [
    { runFence: randomUUID() },
    { contextVersion: randomUUID() },
    { candidateArtifactDigest: 'f'.repeat(64) },
    { candidateDigest: 'f'.repeat(64) },
  ].entries()) {
    const result = await proposeCandidateValidation(
      f.goal,
      f.managerRun,
      index,
      proposal(extra),
      observer,
    );
    expect(result.code).not.toBe('VALIDATION_PROPOSED');
  }
  const foreign = randomUUID();
  expect(
    (await proposeCandidateValidation(foreign, f.managerRun, 5, proposal(), observer)).code,
  ).toBe('VALIDATION_MANAGER_UNAUTHORIZED');
  await pool.query("UPDATE runs SET state='running' WHERE id=$1", [f.runId]);
  const previous = calls;
  expect(
    (await proposeCandidateValidation(f.goal, f.managerRun, 6, proposal(), observer)).code,
  ).toBe('VALIDATION_BINDING_STALE');
  expect(calls).toBe(previous);
});
it('requires accepted actual Sol authority and refuses arbitrary command payloads', async () => {
  await pool.query("UPDATE runs SET model='gpt-6.1-luna' WHERE id=$1", [f.managerRun]);
  expect((await action()).code).toBe('VALIDATION_MANAGER_UNAUTHORIZED');
  expect(
    (
      await applyManagementAction(
        f.goal,
        f.managerRun,
        1,
        proposal({ command: 'publish' }),
        inspect,
      )
    ).code,
  ).toBe('VALIDATION_FORMAT');
});
it('blocks retry/run admission atomically only for owned task and releases after failed receipt', async () => {
  const p = await action();
  expect(p.code).toBe('VALIDATION_PROPOSED');
  await pool.query("UPDATE tasks SET state='retry_scheduled' WHERE id=$1", [f.taskId]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.taskId])).rows[0].state).toBe(
    'failed',
  );
  await expect(
    pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,2,$3,'harness',1)",
      [randomUUID(), f.taskId, randomUUID()],
    ),
  ).rejects.toThrow('CANDIDATE_VALIDATION_OWNS_TASK');
  await expect(
    pool.query('UPDATE tasks SET criteria=\'[{"id":"weakened"}]\' WHERE id=$1', [f.taskId]),
  ).rejects.toThrow('CANDIDATE_VALIDATION_CONTRACT_FROZEN');
  const other = randomUUID();
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'other','Other','Other goal task','qa.backend','ready','[]')",
    [other, f.goal],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",
    [randomUUID(), other, randomUUID()],
  );
  await reconcileCandidateValidations(
    async (mode, request) => receipt(request, 'frozen', { frozenWorkspace: 'agentos/ocvtest' }),
    inspect,
  );
  await reconcileCandidateValidations(
    async (mode, request) =>
      receipt(request, 'failed', { code: 'VALIDATION_COMMAND_FAILED', passed: false, exitCode: 7 }),
    inspect,
  );
  expect((await stored(p.actionId!)).result.passed).toBe(false);
  await pool.query("UPDATE tasks SET state='retry_scheduled' WHERE id=$1", [f.taskId]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.taskId])).rows[0].state).toBe(
    'retry_scheduled',
  );
});
it('crash recovery and lost replies probe durable receipts and never rerun the command', async () => {
  const p = await action(),
    modes: string[] = [];
  await reconcileCandidateValidations(async (mode, request) => {
    modes.push(mode);
    return receipt(request, 'frozen', { frozenWorkspace: 'agentos/ocvtest' });
  }, inspect);
  await reconcileCandidateValidations(async (mode) => {
    modes.push(mode);
    throw Error('SSH reply lost');
  }, inspect);
  expect((await stored(p.actionId!)).state).toBe('uncertain');
  await reconcileCandidateValidations(async (mode, request) => {
    modes.push(mode);
    return receipt(request, 'completed', {
      code: 'VALIDATION_CHECKS_FAILED',
      passed: false,
      diagnostic: 'actual failing test',
    });
  }, inspect);
  expect(modes).toEqual(['freeze', 'execute', 'receipt']);
  const result = (await readCandidateValidations(pool, f.goal))[0];
  expect(result.result.diagnostic).toBe('actual failing test');
  expect(result.result.acceptance).toBe(false);
  expect(result.request).toBeUndefined();
  expect(
    (await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1', [f.taskId]))
      .rows[0].n,
  ).toBe(0);
});
it('concurrent reconcilers cannot steal a live validation lease', async () => {
  const p = await action();
  let calls = 0;
  await Promise.all(
    [1, 2, 3].map(() =>
      reconcileCandidateValidations(async (mode, request) => {
        calls++;
        await new Promise((resolve) => setTimeout(resolve, 30));
        return receipt(request, 'frozen', { frozenWorkspace: 'agentos/ocvtest' });
      }, inspect),
    ),
  );
  expect(calls).toBe(1);
  expect((await stored(p.actionId!)).state).toBe('frozen');
});
it('retains exact old result as stale evidence when authority changes and never accepts it', async () => {
  const p = await action();
  await reconcileCandidateValidations(
    async (mode, request) => receipt(request, 'frozen', { frozenWorkspace: 'agentos/ocvtest' }),
    inspect,
  );
  await reconcileCandidateValidations(async (mode, request) => {
    await pool.query('UPDATE runs SET fence=$2 WHERE id=$1', [f.runId, randomUUID()]);
    return receipt(request, 'completed', { code: 'VALIDATION_PASSED', passed: true });
  }, inspect);
  expect((await stored(p.actionId!)).result.stale).toBe(true);
  expect((await stored(p.actionId!)).result.acceptance).toBe(false);
});

it('authorizes the actual framework department namespace only through a goal-bound operator registry', async () => {
  const project = 'harness-improvement-team';
  await pool.query('UPDATE goals SET project=$2 WHERE id=$1', [f.goal, project]);
  await pool.query('UPDATE tasks SET execution=$2,attempt_count=2 WHERE id=$1', [
    f.taskId,
    JSON.stringify({
      kind: 'harness',
      workspace: project + '/otest',
      baseWorkspace: project + '/framework',
    }),
  ]);
  const runId = randomUUID(),
    runFence = randomUUID(),
    contextVersion = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,2,$3,'harness',1,'running',$4)",
    [runId, f.taskId, runFence, contextVersion],
  );
  await pool.query(
    "INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Department scoped immutable context')",
    [contextVersion, runId],
  );
  const artifact = await writeArtifact(
    runId,
    runFence,
    'candidate.json',
    JSON.stringify({
      baseDigest: f.candidateDigest,
      candidateDigest: f.candidateDigest,
      files: [],
    }),
  );
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1", [runId]);
  Object.assign(f, {
    runId,
    runFence,
    contextVersion,
    expectedAttempt: 2,
    candidateArtifactId: artifact.id,
    candidateArtifactDigest: artifact.digest,
  });
  await approveActions([proposal(), proposal(), proposal()]);
  let inspected = 0;
  const observer = async () => {
    inspected++;
    return inspect();
  };
  expect(
    (await proposeCandidateValidation(f.goal, f.managerRun, 0, proposal(), observer)).code,
  ).toBe('VALIDATION_PROJECT_UNREGISTERED');
  expect(inspected).toBe(0);
  const registry = {
    version: 1,
    enabled: true,
    project,
    role: 'framework',
    sourceKind: 'agentos_published_runtime',
    goalIds: [randomUUID()],
  };
  await pool.query(
    'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['framework-project:' + project, JSON.stringify(registry)],
  );
  expect(
    (await proposeCandidateValidation(f.goal, f.managerRun, 1, proposal(), observer)).code,
  ).toBe('VALIDATION_PROJECT_UNREGISTERED');
  registry.goalIds = [f.goal];
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [
    'framework-project:' + project,
    JSON.stringify(registry),
  ]);
  const accepted = await proposeCandidateValidation(f.goal, f.managerRun, 2, proposal(), observer);
  expect(accepted.code).toBe('VALIDATION_PROPOSED');
  expect(inspected).toBe(1);
  const value = await stored(accepted.actionId!);
  expect(value.request.project).toBe(project);
  expect(value.request.workspace).toBe(project + '/otest');
  expect(value.request.frameworkAuthorization.registry.goalIds).toEqual([f.goal]);
  await reconcileCandidateValidations(
    async (mode, request) => receipt(request, 'frozen', { frozenWorkspace: 'agentos/ocvtest' }),
    inspect,
  );
  expect((await stored(accepted.actionId!)).state).toBe('frozen');
  expect(
    (
      await proposeCandidateValidation(
        f.goal,
        f.managerRun,
        3,
        proposal({ project: 'agentos' }),
        observer,
      )
    ).code,
  ).toBe('VALIDATION_FORMAT');
});

it('refuses a fabricated action attached to an otherwise accepted Sol coordination run', async () => {
  const result = await proposeCandidateValidation(
    f.goal,
    f.managerRun,
    0,
    proposal({
      reason: 'A forged alternative reason was never approved in the immutable coordination file.',
    }),
    inspect,
  );
  expect(result.code).toBe('VALIDATION_DECISION_UNBOUND');
  expect(
    (
      await pool.query(
        "SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'",
      )
    ).rows[0].n,
  ).toBe(0);
  const legitimate = await action();
  expect(legitimate.code).toBe('VALIDATION_DECISION_UNBOUND');
  // A new accepted decision is required after an index has received a refusal.
  await approveActions([proposal()]);
  expect((await action()).code).toBe('VALIDATION_PROPOSED');
});

async function glmCoordinatorFixture() {
  const model='glm-5.3-flash',bridgeId='agentos:api:test-glm';
  await pool.query("UPDATE accounts SET provider='openai',bridge_id=$2,model=$3,models=$4 WHERE id=$1",[f.sol,bridgeId,model,JSON.stringify([model])]);
  await pool.query('UPDATE runs SET model=$2 WHERE id=$1',[f.managerRun,model]);
  await pool.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1',['workforce:'+f.goal,JSON.stringify({coordinatorAccountId:f.sol,coordinatorModel:model})]);
  await pool.query("INSERT INTO settings VALUES('model-policy',$1)",[JSON.stringify({version:1,mode:'glm-only',accountId:f.sol,bridgeId,model,sessionLimit:12,requestLimit:6})]);
}
it('admits protected validation by the exact configured GLM coordinator with immutable accepted action',async()=>{
  await glmCoordinatorFixture();
  const result=await action();expect(result.code).toBe('VALIDATION_PROPOSED');
  expect((await stored(result.actionId!)).request.candidateArtifactDigest).toBe(f.candidateArtifactDigest);
});
it('rejects a GLM account which is not the configured coordinator',async()=>{
  await glmCoordinatorFixture();
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{coordinatorAccountId}','\"different-glm\"') WHERE id=$1",['workforce:'+f.goal]);
  expect((await action()).code).toBe('VALIDATION_MANAGER_UNAUTHORIZED');
});
it('rejects a historical Sol authorization after the exclusive policy is enabled',async()=>{
  await pool.query("INSERT INTO settings VALUES('model-policy',$1)",[JSON.stringify({version:1,mode:'glm-only',accountId:'other-glm',model:'glm-5.3-flash'})]);
  expect((await action()).code).toBe('VALIDATION_MANAGER_UNAUTHORIZED');
});

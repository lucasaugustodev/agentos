import { beforeAll, beforeEach, afterEach, afterAll, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { config } from '../server/config.js';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { accountBridge } from '../server/bridge.js';
import { objectiveSchema, planSchema, validatePlan } from '../server/objective-contracts.js';
import {
  submitObjective,
  claimPlanning,
  applyPlannedResult,
  reconcilePlanning,
  PlanningEngine,
  feedbackObjective,
} from '../server/planner.js';
import * as service from '../server/service.js';
import { importRecall } from '../server/recall.js';
import { rankContext } from '../server/context-rank.js';
import { recoverStoppedTasks } from '../server/recovery.js';
import { ChatEngine, newChat } from '../server/chat.js';

const criterion = {
  id: 'check',
  label: 'Executable check',
  type: 'json_field',
  name: 'result.json',
  field: 'passed',
  equals: true,
};
function plan() {
  const fixture=planSchema.parse({
    title: 'Entrega composta',
    summary: 'Duas frentes, integração e verificação independente.',
    requirements: [{ id: 'working', description: 'As duas funções devem passar seus testes.' }],
    tasks: [
      {
        key: 'left',
        title: 'Função esquerda',
        description: 'Implementar função esquerda.',
        agentId: 'engineering.backend.node',
        stage: 'work',
        baseWorkspace: 'test/base',
        covers: ['working'],
        criteria: [criterion],
      },
      {
        key: 'right',
        title: 'Função direita',
        description: 'Implementar função direita.',
        agentId: 'engineering.backend.node',
        stage: 'work',
        baseWorkspace: 'test/base',
        covers: ['working'],
        criteria: [criterion],
      },
      {
        key: 'join',
        title: 'Integrar alterações',
        description: 'Aplicar ambos os patches.',
        agentId: 'engineering.integration',
        stage: 'integrate',
        baseWorkspace: 'test/base',
        covers: ['working'],
        dependencies: ['left', 'right'],
        criteria: [criterion],
      },
      {
        key: 'check',
        title: 'Verificar candidata',
        description: 'Executar testes independentes.',
        agentId: 'qa.integration',
        stage: 'verify',
        baseWorkspace: 'test/base',
        covers: ['working'],
        dependencies: ['join'],
        sourceTask: 'join',
        criteria: [criterion],
      },
    ],
  });
  // This suite exercises orchestration using result.json, without source files.
  fixture.sourceContractVersion=1;
  for(const task of fixture.tasks)task.sourceContract={version:1,required:[],preserve:[],
    noSourceReason:'Synthetic controller fixture emits only result.json; no implementation source is claimed by this test.'};
  return fixture;
}
function request(extra = {}) {
  return objectiveSchema.parse({
    message: 'Implementar duas funções e testar a candidata integrada.',
    project: 'test',
    workspaces: ['test/base'],
    mode: 'execute',
    idempotencyKey: randomUUID(),
    ...extra,
  });
}
beforeAll(async () => {
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(
    "UPDATE accounts SET provider='codex',model='test-model',models='[\"test-model\"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true",
  );
  config.bridgeHost = 'fixture';
  config.remoteExecution = true;
});
afterEach(() => {
  vi.restoreAllMocks();
  config.bridgeHost = '';
  config.remoteExecution = false;
});
afterAll(() => pool.end());
async function finish(id: string, passed = true) {
  const r = await service.startRun(id, 'test');
  await service.writeArtifact(id, r.fence, 'result.json', JSON.stringify({ passed }));
  await service.completeRun(id, r.fence, 'Verified test fixture');
  return service.validateTask(r.task_id);
}
it('deduplicates objectives and claims only one planner across competing schedulers', async () => {
  const input = request();
  const responses = await Promise.all(Array.from({ length: 5 }, () => submitObjective(input)));
  expect(new Set(responses.map((r) => r.objectiveId)).size).toBe(1);
  await expect(submitObjective({ ...input, message: 'Different request' })).rejects.toMatchObject({
    code: 'IDEMPOTENCY_CONFLICT',
  });
  const claims = await Promise.all(Array.from({ length: 5 }, () => claimPlanning()));
  expect(claims.filter(Boolean)).toHaveLength(1);
});
it('excludes native Muse subscriptions from planner calls without hiding them from task routing', async () => {
  const { objectiveId } = await submitObjective(request());
  await pool.query("UPDATE accounts SET provider='muse'");
  await expect(
    submitObjective(request({ plannerAccountId: 'local-validation' })),
  ).rejects.toMatchObject({ code: 'NATIVE_RUNTIME_ONLY' });
  expect(await claimPlanning()).toBeNull();
  expect(
    (await pool.query('SELECT account_id FROM planning_runs WHERE objective_id=$1', [objectiveId]))
      .rows[0].account_id,
  ).toBeNull();
  await pool.query("UPDATE accounts SET provider='codex'");
  await pool.query('UPDATE planning_runs SET next_attempt_at=now() WHERE objective_id=$1', [
    objectiveId,
  ]);
  await applyPlannedResult(await claimPlanning(), plan());
  await pool.query("UPDATE accounts SET provider='muse'");
  const runs = await service.schedule();
  expect(runs).toHaveLength(2);
  expect(
    (
      await pool.query(
        'SELECT a.provider,t.execution FROM runs r JOIN accounts a ON a.id=r.account_id JOIN tasks t ON t.id=r.task_id WHERE r.id=ANY($1::uuid[])',
        [runs],
      )
    ).rows.every((r) => r.provider === 'muse' && r.execution.kind === 'harness'),
  ).toBe(true);
  const analytical = await service.createGoal({
    title: 'Analytical fixture',
    project: 'test',
    idempotencyKey: randomUUID(),
    tasks: [
      {
        key: 'analysis',
        title: 'Analyze without a native workspace',
        agentId: 'research.web',
        criteria: [criterion],
      },
    ],
  });
  expect(await service.schedule()).toHaveLength(0);
  expect((await service.goalDetail(analytical.id)).tasks[0].state).toBe('waiting_configuration');
});
it('hands off to the workforce coordinator without pausing its queue or applying a stale plan', async () => {
  const { objectiveId } = await submitObjective(request());
  const initial = await claimPlanning();
  await applyPlannedResult(initial, plan());
  await feedbackObjective(objectiveId, 'workforce-feedback', 'Inspect remaining game evidence.');
  await pool.query('INSERT INTO settings(id,value) VALUES($1,\'{"enabled":true}\')', [
    'workforce:' + objectiveId,
  ]);
  await reconcilePlanning();
  expect(
    (await pool.query('SELECT state FROM goals WHERE id=$1', [objectiveId])).rows[0].state,
  ).toBe('active');
  expect(
    (
      await pool.query('SELECT count(*)::int n FROM planning_runs WHERE objective_id=$1', [
        objectiveId,
      ])
    ).rows[0].n,
  ).toBe(1);
  // Even a pre-existing queued planner cannot claim work after the handover.
  await pool.query("UPDATE planning_runs SET state='queued',next_attempt_at=now() WHERE id=$1", [
    initial.id,
  ]);
  expect(await claimPlanning()).toBeNull();
  await pool.query("UPDATE planning_runs SET state='running',base_revision=1 WHERE id=$1", [
    initial.id,
  ]);
  await expect(applyPlannedResult({ ...initial, base_revision: 1 }, plan())).rejects.toMatchObject({
    code: 'STALE_PLAN',
  });
  expect(
    (await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1', [objectiveId])).rows[0]
      .n,
  ).toBe(4);
});
it('releases two isolated workers then integration then read-only QA only after acceptance', async () => {
  const { objectiveId } = await submitObjective(request());
  const run = await claimPlanning();
  await applyPlannedResult(run, plan());
  const first = await service.schedule();
  expect(first).toHaveLength(2);
  const detail = await service.goalDetail(objectiveId);
  expect(new Set(detail.tasks.map((t) => t.execution.workspace)).size).toBe(4);
  expect(detail.tasks.find((t) => t.stage === 'verify').execution.readOnlySource).toBe(true);
  await finish(first[0]);
  expect(await service.schedule()).toHaveLength(0);
  await finish(first[1]);
  const join = await service.schedule();
  expect(join).toHaveLength(1);
  await finish(join[0]);
  const check = await service.schedule();
  expect(check).toHaveLength(1);
  await finish(check[0]);
  expect((await service.goalDetail(objectiveId)).goal.state).toBe('completed');
});
it('never creates tasks or prepares workspaces for planning-only requests', async () => {
  const spy = vi.spyOn(accountBridge, 'request').mockImplementation(async (method) => {
    if (method === 'memory.search') return { items: [] };
    if (method === 'worker.workspaces') return [{ id: 'test/base' }];
    if (method === 'generate')
      return { message: { content: [{ type: 'text', text: JSON.stringify(plan()) }] } };
    throw Error('Expected fallback or forbidden mutation: ' + method);
  });
  const { objectiveId } = await submitObjective(request({ mode: 'plan' }));
  const r = await claimPlanning();
  await new PlanningEngine().execute(r, new AbortController().signal);
  expect((await service.goalDetail(objectiveId)).goal.state).toBe('planned');
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(0);
  expect(spy.mock.calls.some((c) => c[0] === 'worker.prepare')).toBe(false);
});
it('rejects cycles, unauthorized routes and missing integration before persisting any task', async () => {
  const req = request(),
    agents = (await pool.query('SELECT * FROM agents')).rows,
    accounts = (await pool.query('SELECT * FROM accounts')).rows;
  const cycle = plan();
  cycle.tasks[0].dependencies = ['check'];
  expect(() => validatePlan(cycle, req, agents, accounts)).toThrow(/circulares/);
  const route = plan();
  route.tasks[0].accountId = 'not-authorized';
  expect(() => validatePlan(route, req, agents, accounts)).toThrow(/Conta/);
  const missing = plan();
  missing.tasks[2].stage = 'work';
  expect(() => validatePlan(missing, req, agents, accounts)).toThrow(/integração/);
  const oldCandidate = plan();
  oldCandidate.tasks[3].sourceTask = 'left';
  expect(() => validatePlan(oldCandidate, req, agents, accounts)).toThrow(/candidata integrada/);
  await submitObjective(req);
  const run = await claimPlanning();
  await expect(applyPlannedResult(run, cycle)).rejects.toMatchObject({ code: 'PLAN_INVALID' });
  expect((await pool.query('SELECT * FROM plan_revisions')).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(0);
});
it('fences stale planners on recovery without duplicating the applied plan', async () => {
  await submitObjective(request());
  const old = await claimPlanning();
  await pool.query("UPDATE planning_runs SET heartbeat_at=now()-interval '70 seconds'");
  await reconcilePlanning();
  const next = await claimPlanning();
  expect(next.fence).not.toBe(old.fence);
  await expect(applyPlannedResult(old, plan())).rejects.toMatchObject({ code: 'STALE_PLAN' });
  await applyPlannedResult(next, plan());
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(4);
});
it('defers revisions while a worker is active and preserves requirements and historical tasks', async () => {
  const { objectiveId } = await submitObjective(request());
  await applyPlannedResult(await claimPlanning(), plan());
  const first = await service.schedule();
  await service.startRun(first[0], 'active');
  await feedbackObjective(
    objectiveId,
    'new-evidence',
    'Revise a estratégia conforme a nova evidência.',
  );
  await reconcilePlanning();
  expect(await claimPlanning()).toBeNull();
  await pool.query(
    "UPDATE runs SET state='failed',finished_at=now() WHERE state IN('running','queued')",
  );
  await pool.query('DELETE FROM leases');
  await pool.query("UPDATE tasks SET state='failed' WHERE state='queued' OR state='running'");
  await reconcilePlanning();
  const revised = await claimPlanning();
  expect(revised).toBeTruthy();
  const next = plan();
  next.approachChange = 'Nova abordagem: separar teste mínimo antes da integração final.';
  const wrong = structuredClone(next);
  wrong.requirements[0].description = 'Critério reduzido';
  await expect(applyPlannedResult(revised, wrong)).rejects.toMatchObject({ code: 'PLAN_INVALID' });
  await applyPlannedResult(revised, next);
  const detail = await service.goalDetail(objectiveId);
  expect(detail.retiredTasks).toHaveLength(4);
  expect(detail.tasks).toHaveLength(4);
  expect((await pool.query('SELECT * FROM runs')).rowCount).toBe(2);
});
it('imports only scoped Recall evidence once without creating an export loop', async () => {
  vi.spyOn(accountBridge, 'request').mockResolvedValue({
    source: 'harness-recall-notes',
    items: [
      { id: 'a', description: 'Regra', body: 'Conteúdo relevante ao projeto.', project: 'test' },
      { id: 'b', description: 'Outro', body: 'Privado', project: 'other' },
    ],
  });
  expect((await importRecall('test', 'relevante')).imported).toBe(1);
  expect((await importRecall('test', 'relevante')).imported).toBe(0);
  expect((await pool.query('SELECT * FROM memories')).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM memory_outbox')).rowCount).toBe(0);
});
it('batches Laya candidates and rejects incomplete rankings', async () => {
  const spy = vi.spyOn(accountBridge, 'request').mockImplementation(async (_method, p: any) => ({
    truncated: false,
    scores: Object.fromEntries(p.blocks.map((b: any) => [b.id, 0.8])),
  }));
  const scores = await rankContext(
    'Objetivo',
    Array.from({ length: 28 }, (_, i) => ({ id: String(i), text: 'candidate' })),
  );
  expect(Object.keys(scores)).toHaveLength(28);
  expect(spy).toHaveBeenCalledTimes(2);
  spy.mockResolvedValue({ truncated: true, scores: {} });
  await expect(rankContext('Objetivo', [{ id: 'x', text: 'test' }])).rejects.toThrow(/incompleta/);
});

it('reuses a validated proposal when workspace preparation must wait', async () => {
  let prepares = 0,
    generations = 0;
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method) => {
    if (method === 'memory.search') return { items: [] };
    if (method === 'worker.workspaces') return [{ id: 'test/base' }];
    if (method === 'generate') {
      generations++;
      return { message: { content: [{ type: 'text', text: JSON.stringify(plan()) }] } };
    }
    if (method === 'worker.prepare') {
      if (++prepares === 1) throw Object.assign(Error('busy'), { code: 'SOURCE_BUSY' });
      return {};
    }
    throw Error('ranking fallback');
  });
  await submitObjective(request());
  const engine = new PlanningEngine();
  await engine.execute(await claimPlanning(), new AbortController().signal);
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(0);
  await pool.query('UPDATE planning_runs SET next_attempt_at=now()');
  await pool.query("UPDATE accounts SET quota_status='exhausted',remaining_percent=0");
  await engine.execute(await claimPlanning(), new AbortController().signal);
  expect(generations).toBe(1);
  expect(prepares).toBe(2);
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(4);
});

it('recovers a confirmed generation timeout without creating duplicate work', async () => {
  let generations = 0;
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method) => {
    if (method === 'memory.search') return { items: [] };
    if (method === 'worker.workspaces') return [{ id: 'test/base' }];
    if (method === 'generate') {
      if (++generations === 1) throw Object.assign(Error('confirmed timeout'), { code: 'PROVIDER_TIMEOUT' });
      return { message: { content: [{ type: 'text', text: JSON.stringify(plan()) }] } };
    }
    if (method === 'worker.prepare') return {};
    throw Error('ranking fallback');
  });
  const { objectiveId } = await submitObjective(request());
  const engine = new PlanningEngine();
  await engine.execute(await claimPlanning(), new AbortController().signal);
  expect((await pool.query('SELECT state FROM planning_runs')).rows[0].state).toBe('queued');
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(0);
  await pool.query('UPDATE planning_runs SET next_attempt_at=now()');
  await engine.execute(await claimPlanning(), new AbortController().signal);
  expect(generations).toBe(2);
  expect((await pool.query('SELECT state FROM planning_runs')).rows[0].state).toBe('completed');
  expect((await pool.query('SELECT * FROM tasks WHERE goal_id=$1', [objectiveId])).rowCount).toBe(4);
});

it.each([
  ['generate', 'STOP_UNCONFIRMED', 1, 'failed', false],
  ['worker.prepare', 'PROVIDER_TIMEOUT', 1, 'failed', false],
  ['generate', 'PROVIDER_TIMEOUT', 3, 'queued', false],
  ['generate', 'STOP_UNCONFIRMED', 1, 'failed', true],
  ['worker.prepare', 'STOP_UNCONFIRMED', 1, 'failed', true],
])('distinguishes confirmed model failure from uncertain effects [case %#]: %s %s attempt %s -> %s', async (phase, code, attempt, expectedState, interrupt) => {
  const controller = new AbortController();
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method) => {
    if (method === phase) {
      if (interrupt) controller.abort();
      throw Object.assign(Error(code), { code });
    }
    if (method === 'memory.search') return { items: [] };
    if (method === 'worker.workspaces') return [{ id: 'test/base' }];
    if (method === 'generate') return { message: { content: [{ type: 'text', text: JSON.stringify(plan()) }] } };
    if (method === 'worker.prepare') return {};
    throw Error('ranking fallback');
  });
  await submitObjective(request());
  const run = await claimPlanning();
  run.attempt = attempt;
  await new PlanningEngine().execute(run, controller.signal);
  expect((await pool.query('SELECT state FROM planning_runs')).rows[0].state).toBe(expectedState);
  expect((await pool.query('SELECT * FROM tasks')).rowCount).toBe(0);
});

it('automatically replans rejected QA while retaining previously accepted branches', async () => {
  const { objectiveId } = await submitObjective(request());
  await applyPlannedResult(await claimPlanning(), plan());
  for (const id of await service.schedule()) await finish(id);
  for (const id of await service.schedule()) await finish(id);
  const [qa] = await service.schedule();
  await finish(qa, false);
  await recoverStoppedTasks();
  await reconcilePlanning();
  const next = await claimPlanning();
  expect(next.feedback.sourceOverrides['test/base']).toContain('-check');
  expect(next.feedback.artifacts).toHaveLength(1);
  const repair = plan();
  repair.approachChange = 'Reproduzir e corrigir o caso mínimo que reprovou a candidata.';
  repair.tasks = [
    repair.tasks[0],
    { ...repair.tasks[3], dependencies: ['left'], sourceTask: 'left' },
  ];
  await applyPlannedResult(next, repair);
  for (const id of await service.schedule()) await finish(id);
  for (const id of await service.schedule()) await finish(id);
  const d = await service.goalDetail(objectiveId);
  expect(d.goal.state).toBe('completed');
  expect(d.tasks.filter((t) => t.plan_revision === 1 && t.state === 'accepted')).toHaveLength(3);
  expect(d.retiredTasks).toHaveLength(1);
  expect(d.runs).toHaveLength(6);
});

it('the chat receiver retains user context and selects the workspace project', async () => {
  const chat = await newChat();
  const turn = randomUUID();
  await pool.query(
    "INSERT INTO chat_messages(id,chat_id,role,content) VALUES($1,$2,'user','Somente planejar a correção da API')",
    [randomUUID(), chat.id],
  );
  const result: any = await new ChatEngine().tool(
    'submit_objective',
    { mode: 'execute', workspaces: ['test/base'] },
    chat,
    { id: 'local-validation' },
    'test-model',
    turn,
    0,
  );
  const row = (await pool.query('SELECT * FROM objectives WHERE id=$1', [result.objectiveId]))
    .rows[0];
  expect(row.mode).toBe('plan');
  expect(row.request.project).toBe('test');
  expect(
    (await pool.query('SELECT project FROM chats WHERE id=$1', [chat.id])).rows[0].project,
  ).toBe('test');
});

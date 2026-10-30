import { beforeAll, beforeEach, afterAll, afterEach, expect, it, vi } from 'vitest';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import {
  createCycle,
  runCycle,
  updateCycle,
  cycleDetail,
  occurrenceDetail,
  commandOccurrence,
  setCycleState,
} from '../server/cycles/store.js';
import { CycleEngine, resolveCycleStep } from '../server/cycles/engine.js';
import { parseCycle } from '../server/cycles/contracts.js';
import { managerTool } from '../server/manager.js';
import { makeApp } from '../server/app.js';
import * as integrations from '../server/cycles/integrations.js';
import { trustedTailnet } from '../server/tailnet.js';
import { saveCycleArtifacts, readCycleArtifact } from '../server/cycles/runtime.js';
import * as cycleRuntime from '../server/cycles/runtime.js';
import { DomainError } from '../server/contracts.js';
const definition = (steps: any[] = []) => ({
  name: 'Ciclo isolado',
  project: 'fixture',
  trigger: { kind: 'manual' },
  concurrency: 2,
  steps: steps.length
    ? steps
    : [{ key: 'wait', title: 'Espera configurada', kind: 'wait', seconds: 1 }],
});
let engine: CycleEngine;
beforeAll(async () => {
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE cycles,cycle_commands,accounts,goals,memories,events CASCADE');
  engine = new CycleEngine();
  config.bridgeHost = 'fixture';
  config.remoteExecution = true;
});
afterEach(async () => {
  await engine.close();
  vi.restoreAllMocks();
  config.bridgeHost = '';
  config.remoteExecution = false;
  config.tailnetHost = '';
});
it('allows only the configured private proxy identity and rejects forged or cross-site requests', async () => {
  config.tailnetHost = 'agentos.example.ts.net';
  const app = makeApp();
  expect(
    (await app.inject({ url: '/api/session', headers: { host: config.tailnetHost } })).statusCode,
  ).toBe(403);
  const headers = { host: config.tailnetHost, 'tailscale-user-login': 'operator@example.com' };
  expect(
    (await app.inject({ url: '/api/session', headers, remoteAddress: '10.0.0.1' })).statusCode,
  ).toBe(403);
  const session = await app.inject({ url: '/api/session', headers, remoteAddress: '127.0.0.1' });
  expect(session.statusCode).toBe(200);
  expect(session.headers['set-cookie']).toContain('Secure');
  expect(
    (
      await app.inject({
        url: '/api/cycles',
        headers: { ...headers, cookie: String(session.headers['set-cookie']) },
        remoteAddress: '127.0.0.1',
      })
    ).statusCode,
  ).toBe(200);
  expect(
    (
      await app.inject({
        url: '/api/session',
        headers: { ...headers, origin: 'https://evil.example' },
        remoteAddress: '127.0.0.1',
      })
    ).statusCode,
  ).toBe(403);
  await app.close();
});
it('authenticates tagged tailnet devices only through the local Serve proxy and a matching whois identity', async () => {
  config.tailnetHost = 'agentos.fixture.ts.net';
  const lookup = vi.fn().mockResolvedValue({
    Node: { Name: 'pc.fixture.ts.net.', Addresses: ['100.64.10.22/32'], Expired: false },
  });
  const req = {
    ip: '127.0.0.1',
    headers: { host: config.tailnetHost, 'x-forwarded-for': '1.2.3.4, 100.64.10.22' },
  };
  expect(await trustedTailnet(req, lookup)).toBe(true);
  expect(lookup).toHaveBeenCalledWith('100.64.10.22');
  expect(await trustedTailnet({ ...req, ip: '10.0.0.1' }, lookup)).toBe(false);
  expect(
    await trustedTailnet(
      { ...req, headers: { ...req.headers, 'x-forwarded-for': '100.64.10.23' } },
      lookup,
    ),
  ).toBe(false);
});
it('polls a baseline without replaying old cards, then creates only new revisions and records connector failures', async () => {
  const poll = vi
    .spyOn(integrations, 'listBitrix')
    .mockResolvedValue([{ id: 1, title: 'Existing', stageId: 5655, revision: 'v1' }]);
  const c = await createCycle(
    { ...definition(), trigger: { kind: 'bitrix', groupId: 217, stageId: 5655 } },
    'poll-cycle',
  );
  await setCycleState(c.id, 1, 'active');
  await engine.poll();
  expect((await cycleDetail(c.id)).occurrences).toHaveLength(0);
  poll.mockResolvedValue([
    { id: 1, title: 'Existing', stageId: 5655, revision: 'v1' },
    { id: 2, title: 'New', stageId: 5655, revision: 'v1' },
  ]);
  for (let i = 0; i < 2; i++) {
    await pool.query('UPDATE cycles SET next_check_at=now()');
    await engine.poll();
  }
  expect((await cycleDetail(c.id)).occurrences).toHaveLength(1);
  poll.mockRejectedValue(Object.assign(Error(), { code: 'BITRIX_UNAVAILABLE' }));
  await pool.query('UPDATE cycles SET next_check_at=now()');
  await engine.poll();
  expect((await cycleDetail(c.id)).cycle.last_error).toBe('BITRIX_UNAVAILABLE');
  expect((await cycleDetail(c.id)).occurrences).toHaveLength(1);
});
afterAll(async () => {
  await pool.end();
});
it('keeps occurrences out of goals/tasks and deduplicates manual requests', async () => {
  const c = await createCycle(definition(), 'create-cycle-1');
  const a = await runCycle(c.id, 'run-cycle-1', { value: 1 }),
    b = await runCycle(c.id, 'run-cycle-1', { value: 1 });
  expect(a.id).toBe(b.id);
  expect((await pool.query('SELECT count(*)::int n FROM goals')).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM tasks')).rows[0].n).toBe(0);
  await expect(runCycle(c.id, 'run-cycle-1', { value: 2 })).rejects.toMatchObject({
    code: 'VERSION_CONFLICT',
  });
});
it('freezes an occurrence definition and rejects stale updates and dependency cycles', async () => {
  const c = await createCycle(definition(), 'create-cycle-1'),
    o = await runCycle(c.id, 'run-cycle-1');
  const changed = await updateCycle(c.id, 1, { ...definition(), name: 'Nova versão' });
  expect(changed.version).toBe(2);
  expect((await occurrenceDetail(o.id)).occurrence.definition.name).toBe('Ciclo isolado');
  await expect(updateCycle(c.id, 1, definition())).rejects.toMatchObject({
    code: 'VERSION_CONFLICT',
  });
  expect(() =>
    parseCycle(
      definition([
        { key: 'a', title: 'Etapa A', kind: 'wait', dependsOn: ['b'] },
        { key: 'b', title: 'Etapa B', kind: 'wait', dependsOn: ['a'] },
      ]),
    ),
  ).toThrow();
});
it('waits durably, pauses new steps, and completes without a model', async () => {
  const c = await createCycle(
      definition([
        { key: 'a', title: 'Espera A', kind: 'wait', seconds: 1 },
        { key: 'b', title: 'Espera B', kind: 'wait', dependsOn: ['a'], seconds: 1 },
      ]),
      'create-cycle-1',
    ),
    o = await runCycle(c.id, 'run-cycle-1');
  await engine.claim();
  let d = await occurrenceDetail(o.id);
  expect(d.steps.map((s) => s.state)).toEqual(['waiting', 'queued']);
  await commandOccurrence(o.id, 'pause');
  await pool.query(
    "UPDATE cycle_steps SET next_at=now()-interval '2 seconds' WHERE occurrence_id=$1",
    [o.id],
  );
  await engine.recover();
  expect((await occurrenceDetail(o.id)).steps[0].state).toBe('waiting');
  await commandOccurrence(o.id, 'resume');
  await engine.recover();
  await engine.claim();
  await pool.query(
    "UPDATE cycle_steps SET next_at=now()-interval '2 seconds' WHERE occurrence_id=$1",
    [o.id],
  );
  await engine.recover();
  expect((await occurrenceDetail(o.id)).occurrence.state).toBe('completed');
});
it('requires explicit review and never lets approval bypass a failed execution', async () => {
  const c = await createCycle(
      definition([
        {
          key: 'review',
          title: 'Mudança destrutiva',
          kind: 'review',
          instructions: 'Revisar escopo',
        },
      ]),
      'create-cycle-1',
    ),
    o = await runCycle(c.id, 'run-cycle-1');
  await engine.claim();
  const step = (await occurrenceDetail(o.id)).steps[0];
  expect(step.state).toBe('attention');
  await resolveCycleStep(step.id, 'approve', 'Aprovado somente o escopo descrito');
  await engine.recover();
  expect((await occurrenceDetail(o.id)).occurrence.state).toBe('completed');
});
it('claims an agent once under concurrency, stores evidence, then releases the dependency', async () => {
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown,max_concurrency) VALUES('a','Conta','muse','muse-spark-1.3','[\"muse-spark-1.3\"]',true,'a',true,1)",
  );
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string) =>
    method === 'worker.execute'
      ? {
          summary: 'Executado',
          evidence: { stopped: true },
          artifacts: [
            { name: 'changes.patch', base64: '', mime: 'text/plain' },
            {
              name: 'report.json',
              content: '',
              base64: Buffer.from('{"passed":true}').toString('base64'),
              mime: 'application/json',
            },
          ],
        }
      : method === 'memory.search'
        ? { items: [] }
        : { scores: {}, truncated: false },
  );
  const c = await createCycle(
      definition([
        {
          key: 'agent',
          title: 'Executar trabalho',
          kind: 'agent',
          workspace: 'fixture/source',
          criteria: [
            {
              id: 'passed',
              label: 'Teste passou',
              type: 'json_field',
              name: 'report.json',
              field: 'passed',
              equals: true,
            },
          ],
        },
        {
          key: 'after',
          title: 'Depois da entrega',
          kind: 'wait',
          seconds: 1,
          dependsOn: ['agent'],
        },
      ]),
      'create-cycle-1',
    ),
    o = await runCycle(c.id, 'run-cycle-1');
  const jobs = await Promise.all([engine.claim(), engine.claim()]);
  expect(jobs.filter(Boolean)).toHaveLength(1);
  await engine.execute(jobs.find(Boolean));
  const d = await occurrenceDetail(o.id);
  expect(d.steps[0].state).toBe('completed');
  expect(d.artifacts).toHaveLength(2);
  expect(
    (await readCycleArtifact(d.artifacts.find((a: any) => a.name === 'changes.patch').id)).bytes
      .length,
  ).toBe(0);
  expect(d.attempts[0].context.manifest.occurrenceId).toBe(o.id);
});
it('keeps ambiguous executor termination in reconciliation without duplicate attempts', async () => {
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown) VALUES('a','Conta','muse','muse-spark-1.3','[\"muse-spark-1.3\"]',true,'a',true)",
  );
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string) => {
    if (method === 'worker.execute')
      throw Object.assign(Error('sem confirmação'), { code: 'STOP_UNCONFIRMED' });
    return method === 'memory.search' ? { items: [] } : { scores: {}, truncated: false };
  });
  const c = await createCycle(
      definition([
        {
          key: 'agent',
          title: 'Executar trabalho',
          kind: 'agent',
          workspace: 'fixture/source',
          criteria: [{ id: 'file', label: 'Relatório', type: 'artifact', name: 'report.md' }],
        },
      ]),
      'create-cycle-1',
    ),
    o = await runCycle(c.id, 'run-cycle-1');
  await engine.execute(await engine.claim());
  expect((await occurrenceDetail(o.id)).occurrence.state).toBe('reconciling');
  expect(await engine.claim()).toBeNull();
  expect((await occurrenceDetail(o.id)).attempts).toHaveLength(1);
});
it('holds new cycle work during control-plane maintenance', async () => {
  const c = await createCycle(definition(), 'maintenance-cycle');
  await runCycle(c.id, 'maintenance-run');
  await pool.query(
    "INSERT INTO settings VALUES('maintenance','{\"draining\":true}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
  try {
    expect(await engine.claim()).toBeNull();
    expect((await cycleDetail(c.id)).occurrences[0].state).toBe('queued');
  } finally {
    await pool.query("DELETE FROM settings WHERE id='maintenance'");
  }
});
it('manager uses separate cycle domain and honors planning-only', async () => {
  await expect(
    managerTool(
      'manage_cycles',
      { action: 'create', definitionJson: JSON.stringify(definition()) },
      'manager-create',
      true,
    ),
  ).rejects.toMatchObject({ code: 'PLANNING_ONLY' });
  const c = await managerTool(
    'manage_cycles',
    { action: 'create', definitionJson: JSON.stringify(definition()) },
    'manager-create',
  );
  expect((await cycleDetail(c.id)).cycle.project).toBe('fixture');
  expect(await managerTool('manage_cycles', { action: 'list' }, 'manager-list', true)).toHaveLength(
    1,
  );
});
it('starts self-development from the currently published source snapshot', async () => {
  await pool.query(
    'INSERT INTO settings VALUES(\'agentos-source-workspace\',\'{"workspace":"agentos/src-current"}\') ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  );
  try {
    const result = await managerTool(
      'develop_agentos',
      { request: 'Melhorar a exibição do progresso dos ciclos' },
      'source-version-test',
    );
    const detail = await cycleDetail(result.cycleId);
    expect(detail.cycle.definition.steps[0].workspace).toBe('agentos/src-current');
  } finally {
    await pool.query("DELETE FROM settings WHERE id='agentos-source-workspace'");
  }
});

const validationCycle = () => ({ ...definition([{
  key: 'validate', title: 'Validação registrada', kind: 'action', action: 'agentos.validate', maxAttempts: 3,
}]), project: 'agentos', allowedActions: ['agentos.validate'] });

it('keeps a lost registered validator in reconciliation without scheduling another attempt', async () => {
  const action = vi.spyOn(cycleRuntime, 'runCycleAction').mockRejectedValue(
    new DomainError('STOP_UNCONFIRMED', 'Guard perdido após despacho; unidade pode iniciar depois.'),
  );
  const cycle = await createCycle(validationCycle(), 'uncertain-validator-cycle');
  const occurrence = await runCycle(cycle.id, 'uncertain-validator-run');
  await engine.execute(await engine.claim());
  await engine.recover();
  await engine.reconcile();
  const detail = await occurrenceDetail(occurrence.id);
  expect(detail.attempts).toHaveLength(1);
  expect(detail.attempts[0]).toMatchObject({ state: 'reconciling', error_code: 'STOP_UNCONFIRMED' });
  expect(detail.steps[0].state).toBe('reconciling');
  expect(detail.occurrence.state).toBe('reconciling');
  expect(detail.events.some(event => event.kind === 'RETRY_SCHEDULED')).toBe(false);
  expect(await engine.claim()).toBeNull();
  expect(action).toHaveBeenCalledTimes(1);
});

it('preserves confirmed failed-CI artifacts and permits repair even without a configured criterion', async () => {
  vi.spyOn(cycleRuntime, 'runCycleAction').mockResolvedValue({
    ok: true, output: { passed: false }, validationStopProof: { populated: false },
    artifacts: [{ name: 'verification.json', content: '{"passed":false}' }],
  });
  const cycle = await createCycle(validationCycle(), 'failed-validator-cycle');
  const occurrence = await runCycle(cycle.id, 'failed-validator-run');
  await engine.execute(await engine.claim());
  const detail = await occurrenceDetail(occurrence.id);
  expect(detail.attempts[0]).toMatchObject({ state: 'failed', error_code: 'VALIDATION_CHECKS_FAILED' });
  expect(detail.steps[0].state).toBe('retry');
  expect(detail.occurrence.state).toBe('recovering');
  expect(detail.artifacts).toHaveLength(1);
  expect(JSON.parse((await readCycleArtifact(detail.artifacts[0].id)).bytes.toString())).toEqual({ passed: false });
  expect(detail.events.some(event => event.kind === 'RETRY_SCHEDULED')).toBe(true);
});

it('completes a successful registered validation after the runtime confirmed its stop', async () => {
  vi.spyOn(cycleRuntime, 'runCycleAction').mockResolvedValue({
    ok: true, output: { digest: 'a'.repeat(64) }, validationStopProof: { populated: false },
    artifacts: [{ name: 'verification.json', content: '{"passed":true}' }],
  });
  const cycle = await createCycle(validationCycle(), 'passed-validator-cycle');
  const occurrence = await runCycle(cycle.id, 'passed-validator-run');
  await engine.execute(await engine.claim());
  await engine.recover();
  const detail = await occurrenceDetail(occurrence.id);
  expect(detail.attempts[0].state).toBe('completed');
  expect(detail.occurrence.state).toBe('completed');
});

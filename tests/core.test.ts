import { beforeAll, beforeEach, afterAll, afterEach, describe, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { createServer, type Server, type RequestListener } from 'node:http';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { config } from '../server/config.js';
import { pool, migrate } from '../server/db.js';
import { seedRegistry, searchAgents } from '../server/registry.js';
import * as service from '../server/service.js';
import { saveMemory, assembleContext } from '../server/memory.js';
import { syncRecallOnce, recallStatus } from '../server/recall.js';
import { WorkerEngine } from '../server/worker.js';
import { makeApp } from '../server/app.js';
import { executeHarness } from '../server/runtime.js';
import * as runtime from '../server/runtime.js';
import { recoverStoppedTasks, acceptedDependencyArtifacts } from '../server/recovery.js';

const criterion = {
  id: 'result',
  type: 'json_field',
  label: 'Verified result',
  name: 'result.json',
  field: 'validated',
  equals: true,
};
const task = (key: string, extra: Record<string, unknown> = {}) => ({
  key,
  title: 'Task ' + key,
  agentId: 'system.validation',
  accountId: 'local-validation',
  criteria: [criterion],
  ...extra,
});
const goal = (tasks = [task('first')], extra: Record<string, unknown> = {}) => ({
  title: 'Isolated validation',
  project: 'test-project',
  idempotencyKey: randomUUID(),
  tasks,
  ...extra,
});
const getTask = async (key = 'first') =>
  (await pool.query('SELECT * FROM tasks WHERE key=$1', [key])).rows[0];
const servers: Server[] = [];
async function http(handler: RequestListener) {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return 'http://127.0.0.1:' + (server.address() as any).port;
}
beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query(
    "UPDATE settings SET value='{" + '"maxRuns":4,"browserSlots":2' + "}' WHERE id='capacity'",
  );
  config.dshCommand = '';
  config.layaUrl = '';
  config.recallUrl = '';
  config.recallKey = '';
  config.recallNamespace = 'agentos-test';
  await seedRegistry();
});
afterEach(async () => {
  for (const s of servers.splice(0)) {
    s.closeAllConnections();
    await new Promise<void>((r) => s.close(() => r()));
  }
});
afterAll(async () => {
  await pool.end();
});

describe('Durable control plane', () => {
  it('recovers a failed delivery through the real queue and automatically releases its dependent', async () => {
    const original = runtime.executeRuntime;
    const attempts: any[] = [];
    const spy = vi.spyOn(runtime, 'executeRuntime').mockImplementation(async (input) => {
      attempts.push(input);
      if (input.task.key === 'first' && input.run.attempt === 1)
        return {
          summary: 'Browser regression reproduced; needs a repair.',
          artifacts: [{ name: 'result.json', content: '{"validated":false}' }],
        };
      return original(input);
    });
    const engine = new WorkerEngine();
    try {
      const created = await service.createGoal(
        goal([
          task('first', { recovery: { mode: 'automatic' } }),
          task('second', { dependencies: ['first'] }),
        ]),
        true,
      );
      await engine.start();
      for (let i = 0; i < 120; i++) {
        if (
          (await pool.query('SELECT state FROM goals WHERE id=$1', [created.id])).rows[0].state ===
          'completed'
        )
          break;
        await delay(100);
      }
      expect(
        (await pool.query('SELECT state FROM goals WHERE id=$1', [created.id])).rows[0].state,
      ).toBe('completed');
      expect(attempts.map((a) => [a.task.key, a.run.attempt])).toEqual([
        ['first', 1],
        ['first', 2],
        ['second', 1],
      ]);
      const repaired = attempts[1];
      expect(repaired.run.checkpoint.kind).toBe('automatic_recovery');
      expect(repaired.context.manifest.recoveryArtifacts).toHaveLength(1);
      expect(repaired.context.manifest.dependencyArtifacts).toHaveLength(0);
      expect(
        (await pool.query('SELECT passed FROM acceptances ORDER BY created_at')).rows.map(
          (a) => a.passed,
        ),
      ).toEqual([false, true, true]);
      expect((await pool.query('SELECT status FROM recoveries')).rows[0].status).toBe('resolved');
    } finally {
      await engine.stop();
      spy.mockRestore();
    }
  });
  it('deduplicates concurrent recovery, carries failed evidence and changes strategy on repeated failure', async () => {
    await service.createGoal(
      goal([task('first', { recovery: { mode: 'automatic' }, maxAttempts: 4 })]),
      true,
    );
    for (let n = 1; n <= 2; n++) {
      const [id] = await service.schedule(),
        run = await service.startRun(id, 'test');
      await service.writeArtifact(id, run.fence, 'result.json', '{"validated":false}');
      await service.completeRun(id, run.fence, 'Same browser regression');
      await service.validateTask(run.task_id);
      const results = await Promise.all(Array.from({ length: 8 }, () => recoverStoppedTasks()));
      expect(results.flat()).toHaveLength(1);
      const records = (await pool.query('SELECT * FROM recoveries ORDER BY created_at')).rows;
      expect(records).toHaveLength(n);
      expect(records[n - 1].strategy).toBe(n === 1 ? 'targeted_repair' : 'replan_and_repair');
      expect(records[n - 1].checkpoint.artifacts).toHaveLength(1);
      await pool.query('UPDATE tasks SET next_attempt_at=now()');
    }
    expect((await pool.query('SELECT * FROM artifacts')).rowCount).toBe(2);
  });
  it('sends independent QA failures back to implementation and revalidates the changed candidate', async () => {
    await service.createGoal(
      goal([
        task('implement', { recovery: { mode: 'automatic' }, maxAttempts: 5 }),
        task('validate', {
          dependencies: ['implement'],
          recovery: { mode: 'automatic', repairTaskKey: 'implement' },
        }),
        task('review', {
          dependencies: ['validate'],
          recovery: { mode: 'automatic', repairTaskKey: 'implement' },
        }),
      ]),
      true,
    );
    async function deliver(validated: boolean) {
      const [id] = await service.schedule(),
        run = await service.startRun(id, 'test');
      await service.writeArtifact(id, run.fence, 'result.json', JSON.stringify({ validated }));
      await service.completeRun(
        id,
        run.fence,
        validated ? 'Candidate test passed' : 'Independent review found regression',
      );
      await service.validateTask(run.task_id);
      return run;
    }
    const old = await deliver(true);
    await deliver(true);
    const rejected = await deliver(false);
    expect(await recoverStoppedTasks()).toEqual([old.task_id]);
    expect((await getTask('implement')).state).toBe('retry_scheduled');
    expect((await getTask('validate')).state).toBe('blocked_dependencies');
    expect((await getTask('review')).state).toBe('blocked_dependencies');
    expect(await acceptedDependencyArtifacts((await getTask('validate')).id)).toHaveLength(0);
    await pool.query('UPDATE tasks SET next_attempt_at=now()');
    const fixed = await deliver(true);
    expect(fixed.checkpoint.sourceRunId).toBe(rejected.id);
    const inputs = await acceptedDependencyArtifacts((await getTask('validate')).id);
    expect(inputs).toHaveLength(1);
    expect(
      (await pool.query('SELECT run_id FROM artifacts WHERE id=$1', [inputs[0].id])).rows[0].run_id,
    ).toBe(fixed.id);
    expect((await pool.query('SELECT * FROM acceptances WHERE passed=true')).rowCount).toBe(3);
    await deliver(true);
    await deliver(true);
    expect((await pool.query('SELECT state FROM goals')).rows[0].state).toBe('completed');
  });
  it('does not recover paused goals, manual review gates, cancelled branches or uncertain processes', async () => {
    const created = await service.createGoal(
      goal([
        task('first', { recovery: { mode: 'automatic' } }),
        task('second', { dependencies: ['first'] }),
      ]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.writeArtifact(id, run.fence, 'result.json', '{"validated":false}');
    await service.completeRun(id, run.fence, 'Needs repair');
    await service.validateTask(run.task_id);
    await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [created.id]);
    expect(await recoverStoppedTasks()).toEqual([]);
    await pool.query("UPDATE goals SET state='active' WHERE id=$1", [created.id]);
    await service.cancelTask((await getTask('second')).id);
    expect(await recoverStoppedTasks()).toEqual([]);
    await pool.query("UPDATE tasks SET state='blocked_dependencies' WHERE key='second'");
    await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1", [id]);
    expect(await recoverStoppedTasks()).toEqual([]);
    await pool.query("UPDATE runs SET state='submitted' WHERE id=$1", [id]);
    await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1', [
      run.task_id,
      JSON.stringify([{ id: 'review', type: 'review', label: 'Human review' }]),
    ]);
    await service.validateTask(run.task_id);
    expect(await recoverStoppedTasks()).toEqual([]);
    expect((await pool.query('SELECT * FROM recoveries')).rowCount).toBe(0);
  });
  it('requires a confirmed stop for runtime failures and exposes exhausted recovery without false acceptance', async () => {
    await service.createGoal(
      goal([task('first', { recovery: { mode: 'automatic' }, maxAttempts: 1 })]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.failRun(id, run.fence, 'RESULT_FORMAT', 'Invalid output');
    expect(await recoverStoppedTasks()).toEqual([]);
    await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_STOPPED',$1,$2)", [
      run.task_id,
      JSON.stringify({ runId: id, confirmed: true }),
    ]);
    expect(await recoverStoppedTasks()).toEqual([]);
    expect((await getTask()).state).toBe('recovery_blocked');
    expect((await pool.query('SELECT status FROM recoveries')).rows[0].status).toBe('blocked');
    expect(await recoverStoppedTasks()).toEqual([]);
    expect((await pool.query('SELECT * FROM recoveries')).rowCount).toBe(1);
    expect((await pool.query('SELECT * FROM acceptances WHERE passed=true')).rowCount).toBe(0);
  });
  it.each(['MODEL_TIMEOUT', 'MODEL_CLIENT_DISCONNECTED', 'CANCELLED'])(
    'requires stop evidence and recovers transport failure %s without replaying a user cancellation', async (code) => {
      await service.createGoal(goal([task('first', { recovery: { mode: 'automatic' }, maxAttempts: 3 })]), true);
      const [id] = await service.schedule(), run = await service.startRun(id, 'test');
      await service.failRun(id, run.fence, code, code);
      expect(await recoverStoppedTasks()).toEqual([]);
      await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_STOPPED',$1,$2)", [
        run.task_id, JSON.stringify({ runId: id, confirmed: true }),
      ]);
      expect(await recoverStoppedTasks()).toEqual(code === 'CANCELLED' ? [] : [run.task_id]);
      expect((await getTask()).state).toBe(code === 'CANCELLED' ? 'failed' : 'retry_scheduled');
      expect((await pool.query('SELECT * FROM acceptances WHERE passed=true')).rowCount).toBe(0);
    },
  );
  it('rejects repair routes outside the dependency ancestry', async () => {
    await expect(
      service.createGoal(
        goal([
          task('first', { recovery: { mode: 'automatic', repairTaskKey: 'second' } }),
          task('second'),
        ]),
        true,
      ),
    ).rejects.toMatchObject({ code: 'RECOVERY_TARGET_INVALID' });
  });
  it('handles out-of-order Harness receipts and confirms child termination on cancellation', async () => {
    config.dshCommand = process.execPath;
    config.dshArgs = [path.resolve('tests/fixtures/runtime.mjs'), 'success'];
    const input = {
      run: { id: randomUUID() },
      task: { title: 'Protocol test', agent_id: 'test', description: 'Fixture only', criteria: [] },
      account: { id: 'fixture', provider: 'fixture', model: 'fixture' },
      context: { content: '', manifest: { dependencyArtifacts: [] } },
      signal: new AbortController().signal,
      onEvent: async () => {},
    };
    expect((await executeHarness(input)).artifacts[0].name).toBe('result.json');
    config.dshArgs[1] = 'invalid';
    await expect(executeHarness({ ...input, run: { id: randomUUID() } })).rejects.toThrow(
      'INVALID_RESULT_JSON',
    );
    config.dshArgs[1] = 'hang';
    const controller = new AbortController();
    const execution = executeHarness({
      ...input,
      run: { id: randomUUID() },
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(), 250);
    await expect(execution).rejects.toThrow('CANCELLED');
  });
  it('discovers specialists without putting the whole registry in a prompt', async () => {
    const rows = await searchAgents('', ['postgres']);
    expect(rows[0].id).toBe('engineering.database.postgres');
    expect(rows.length).toBeLessThan(10);
    expect(
      Number((await pool.query('SELECT count(*) FROM agents')).rows[0].count),
    ).toBeGreaterThanOrEqual(50);
  });
  it('delegates extra tasks into an existing DAG without modifying running work', async () => {
    const created = await service.createGoal(goal(), true);
    const [runId] = await service.schedule();
    await service.delegateTasks(created.id, [task('delegated', { dependencies: ['first'] })]);
    await service.schedule();
    expect((await getTask('delegated')).state).toBe('blocked_dependencies');
    expect((await pool.query('SELECT id FROM runs')).rows[0].id).toBe(runId);
    await expect(service.delegateTasks(created.id, [task('first')])).rejects.toMatchObject({
      code: 'DUPLICATE_TASK',
    });
  });
  it('reopens failed dependencies on a reviewed retry and carries the checkpoint', async () => {
    await service.createGoal(
      goal([task('first'), task('second', { dependencies: ['first'] })]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.failRun(id, run.fence, 'TEST_FAILURE', 'Fixture stopped');
    await service.schedule();
    expect((await getTask('second')).state).toBe('blocked_failed');
    await service.retryTask(run.task_id, 'Effects reviewed: resume from the saved artifact.');
    expect((await getTask('second')).state).toBe('blocked_dependencies');
    const [next] = await service.schedule();
    expect(
      (await pool.query('SELECT checkpoint FROM runs WHERE id=$1', [next])).rows[0].checkpoint
        .summary,
    ).toContain('Effects reviewed');
  });
  it('rejects cycles, missing dependencies and duplicate task IDs atomically', async () => {
    await expect(
      service.createGoal(
        goal([task('one', { dependencies: ['two'] }), task('two', { dependencies: ['one'] })]),
        true,
      ),
    ).rejects.toMatchObject({ code: 'CYCLIC_DAG' });
    await expect(
      service.createGoal(goal([task('one', { dependencies: ['missing'] })]), true),
    ).rejects.toMatchObject({ code: 'MISSING_DEPENDENCY' });
    await expect(service.createGoal(goal([task('one'), task('one')]), true)).rejects.toMatchObject({
      code: 'DUPLICATE_TASK',
    });
    expect((await pool.query('SELECT * FROM goals')).rowCount).toBe(0);
  });
  it('deduplicates concurrent goal submissions and rejects key reuse with different content', async () => {
    const input = goal();
    const results = await Promise.all(
      Array.from({ length: 8 }, () => service.createGoal(input, true)),
    );
    expect(new Set(results.map((r) => r.id)).size).toBe(1);
    expect(results.filter((r) => !r.duplicate)).toHaveLength(1);
    await expect(
      service.createGoal({ ...input, title: 'A different goal' }, true),
    ).rejects.toMatchObject({ code: 'IDEMPOTENCY_CONFLICT' });
  });
  it('keeps technical examples out of real operations and waits for unconfigured runtimes', async () => {
    await expect(service.createGoal(goal())).rejects.toMatchObject({ code: 'DEMO_ONLY' });
    await service.createGoal(
      goal([task('real', { agentId: 'engineering.backend.node', accountId: null })]),
    );
    await service.schedule();
    expect((await getTask('real')).state).toBe('waiting_configuration');
  });
  it('serializes competing schedulers, leases and global capacity', async () => {
    await service.createGoal(
      goal(
        Array.from({ length: 12 }, (_, i) =>
          task('job' + i, { resources: i < 5 ? ['browser'] : ['workspace:same'] }),
        ),
      ),
      true,
    );
    await Promise.all(Array.from({ length: 10 }, () => service.schedule()));
    expect((await pool.query('SELECT * FROM runs')).rowCount).toBe(3);
    expect((await pool.query("SELECT * FROM leases WHERE resource='browser'")).rowCount).toBe(2);
    expect(
      (await pool.query("SELECT * FROM leases WHERE resource='workspace:same'")).rowCount,
    ).toBe(1);
    expect((await pool.query('SELECT * FROM outbox')).rowCount).toBe(3);
  });
  it('honors account limits, unknown quotas and explicit unknown-quota allowance', async () => {
    await pool.query("UPDATE accounts SET max_concurrency=1,quota_status='unknown'");
    await service.createGoal(goal([task('first'), task('second')]), true);
    expect(await service.schedule()).toHaveLength(0);
    expect((await getTask()).state).toBe('waiting_quota');
    await pool.query('UPDATE accounts SET allow_unknown=true');
    expect(await service.schedule()).toHaveLength(1);
    await pool.query("UPDATE accounts SET quota_status='exhausted',max_concurrency=5");
    expect(await service.schedule()).toHaveLength(0);
  });
  it('releases dependencies only after artifacts and acceptance criteria pass', async () => {
    await service.createGoal(
      goal([task('first'), task('second', { dependencies: ['first'] })]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.writeArtifact(id, run.fence, 'result.json', JSON.stringify({ validated: true }));
    await service.completeRun(id, run.fence, 'Delivered');
    expect(await service.schedule()).toHaveLength(0);
    expect((await getTask('second')).state).toBe('blocked_dependencies');
    expect((await service.validateTask(run.task_id)).passed).toBe(true);
    expect(await service.schedule()).toHaveLength(1);
    expect((await pool.query("SELECT * FROM memories WHERE kind='evidence'")).rowCount).toBe(1);
  });
  it('names the missing field when a delivered manifest misses its contract', async () => {
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.writeArtifact(id, run.fence, 'result.json', JSON.stringify({ summary: 'done', artifacts: [] }));
    await service.completeRun(id, run.fence, 'Delivered');
    const result = await service.validateTask(run.task_id);
    expect(result.passed).toBe(false);
    expect(result.checks[0].detail).toBe('Conteúdo não atende ao contrato: result.json precisa do campo "validated" igual a true.');
  });
  it('preserves binary artifacts through storage and authenticated download', async () => {
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'binary-test');
    const bytes = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 0, 255, 128, 1]);
    const a = await service.writeArtifact(id, run.fence, 'screenshot.png', bytes, 'image/png');
    expect((await service.readArtifact(a.id)).bytes).toEqual(bytes);
    const app = makeApp();
    try {
      const response = await app.inject({
        url: '/api/artifacts/' + a.id,
        headers: { authorization: 'Bearer ' + config.apiToken, host: '127.0.0.1:4280' },
      });
      expect(response.statusCode).toBe(200);
      expect(response.rawPayload).toEqual(bytes);
      expect(response.headers['content-type']).toContain('image/png');
    } finally {
      await app.close();
    }
  });
  it('refuses checksum-corrupted artifacts and stale run publication', async () => {
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    const artifact = await service.writeArtifact(
      id,
      run.fence,
      'result.json',
      '{"validated":true}',
    );
    await service.completeRun(id, run.fence, 'Delivered');
    await writeFile(path.join(config.dataDir, 'artifacts', artifact.id), 'tampered');
    expect((await service.validateTask(run.task_id)).passed).toBe(false);
    await expect(service.readArtifact(artifact.id)).rejects.toMatchObject({
      code: 'ARTIFACT_CORRUPT',
    });
    await expect(service.writeArtifact(id, run.fence, 'late.json', '{}')).rejects.toMatchObject({
      code: 'STALE_RUN',
    });
    expect((await getTask()).state).toBe('submitted');
  });
  it('changes a retry model only after termination and preserves the old attempt route', async () => {
    await service.createGoal(goal(), true);
    const [first] = await service.schedule(),
      run = await service.startRun(first, 'route-test');
    await expect(
      service.retryTask(run.task_id, 'Not stopped yet', { model: 'fixture-next' }),
    ).rejects.toMatchObject({ code: 'RETRY_STATE' });
    await service.failRun(first, run.fence, 'TEST_FAILURE', 'Stopped for controlled test');
    await expect(
      service.retryTask(run.task_id, 'Confirmed stopped', { accountId: 'missing' }),
    ).rejects.toMatchObject({ code: 'ROUTE_INVALID' });
    await service.retryTask(run.task_id, 'Confirmed stopped and effects inspected', {
      model: 'fixture-next',
    });
    const [next] = await service.schedule();
    expect((await pool.query('SELECT model FROM runs WHERE id=$1', [first])).rows[0].model).toBe(
      'deterministic',
    );
    expect((await pool.query('SELECT model FROM runs WHERE id=$1', [next])).rows[0].model).toBe(
      'fixture-next',
    );
    expect(
      (await pool.query("SELECT * FROM events WHERE kind='TASK_ROUTE_CHANGED'")).rowCount,
    ).toBe(1);
  });
  it('keeps an uncertain runtime fenced and its resource lease held', async () => {
    await service.createGoal(
      goal([
        task('first', { resources: ['workspace:alpha'] }),
        task('second', { resources: ['workspace:alpha'] }),
      ]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'lost-worker');
    await pool.query("UPDATE runs SET heartbeat_at=now()-interval '60 seconds' WHERE id=$1", [id]);
    await service.markUncertain();
    expect((await getTask()).state).toBe('reconciling');
    expect(await service.schedule()).toHaveLength(0);
    expect((await pool.query('SELECT * FROM leases')).rowCount).toBe(1);
    await expect(service.completeRun(id, run.fence, 'late')).rejects.toMatchObject({
      code: 'STALE_RUN',
    });
  });
  it('retains capacity during cancellation until that runtime confirms stopping', async () => {
    await service.createGoal(goal([task('first', { resources: ['browser'] })]), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.cancelTask(run.task_id);
    expect((await getTask()).state).toBe('cancelling');
    expect((await pool.query('SELECT * FROM leases')).rowCount).toBe(1);
    await service.confirmStopped(id, randomUUID());
    expect((await pool.query('SELECT * FROM leases')).rowCount).toBe(1);
    await service.confirmStopped(id, run.fence);
    expect((await getTask()).state).toBe('cancelled');
    expect((await pool.query('SELECT * FROM leases')).rowCount).toBe(0);
    expect((await pool.query('SELECT state FROM goals')).rows[0].state).toBe('cancelled');
  });
  it('requires an explicit reviewer only for a review criterion', async () => {
    await service.createGoal(
      goal([
        task('first', {
          criteria: [criterion, { id: 'review', label: 'Review content', type: 'review' }],
        }),
      ]),
      true,
    );
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    await service.writeArtifact(id, run.fence, 'result.json', '{"validated":true}');
    await service.completeRun(id, run.fence, 'Delivered');
    expect((await service.validateTask(run.task_id)).passed).toBe(false);
    expect((await service.validateTask(run.task_id, 'reviewer:test')).passed).toBe(true);
  });
  it('isolates memory by project, preserves old required decisions and versions', async () => {
    const old = await saveMemory({
      project: 'test-project',
      title: 'Required decision',
      content: 'Keep this source of truth.',
      kind: 'decision',
      verification: 'confirmed',
      source: 'test:decision',
    });
    await pool.query("UPDATE memories SET created_at=now()-interval '1 year' WHERE id=$1", [
      old.id,
    ]);
    await pool.query(
      "INSERT INTO memories(id,project,title,content,kind,verification,source) SELECT gen_random_uuid(),'test-project','Newer unrelated','No overlap','observation','unverified','test' FROM generate_series(1,205)",
    );
    const other = await saveMemory({
      project: 'other-project',
      title: 'Private other project',
      content: 'Do not retrieve this.',
      kind: 'decision',
      verification: 'confirmed',
      source: 'test:other',
    });
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    const context = await assembleContext(run, await getTask(), []);
    expect(context.manifest.selected.map((m) => m.id)).toContain(old.id);
    expect(context.content).not.toContain(other.id);
    const next = await saveMemory({
      project: 'test-project',
      title: 'Updated decision',
      content: 'Use the new decision.',
      kind: 'decision',
      verification: 'confirmed',
      source: 'test:decision-v2',
      supersedes: old.id,
    });
    expect(next.version).toBe(2);
    const second = await assembleContext(run, await getTask(), []);
    expect(second.content).toContain(next.id);
    expect(second.content).not.toContain(old.id);
    await service.failRun(id,run.fence,'TEST_FIXTURE_STOPPED','Create a new admission with the required old decision.');
    await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1',[run.task_id,JSON.stringify([old.id])]);
    await service.retryTask(run.task_id,'Required decision remains explicit for the next attempt.');
    const [requiredId]=await service.schedule(),requiredRun=await service.startRun(requiredId,'test');
    await expect(
      assembleContext(requiredRun, await getTask(), []),
    ).rejects.toMatchObject({ code: 'REQUIRED_CONTEXT_MISSING' });
  });
  it('uses Laya scores and falls back visibly on incomplete rankings', async () => {
    const memory = await saveMemory({
      project: 'test-project',
      title: 'Useful first task fact',
      content: 'An observation about first task.',
      kind: 'observation',
      verification: 'confirmed',
      source: 'test:laya',
    });
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'test');
    let valid = true;
    config.layaUrl = await http((_req: any, res: any) => {
      res.setHeader('content-type', 'application/json');
      res.end(
        JSON.stringify(valid ? { truncated: false, scores: { [memory.id]: 0.9 } } : { scores: {} }),
      );
    });
    const ctx = await assembleContext(run, await getTask(), []);
    expect(ctx.manifest.selector).toBe('laya');
    expect(ctx.manifest.selected[0].id).toBe(memory.id);
    valid = false;
    const fallback = await assembleContext(run, await getTask(), []);
    expect(fallback.manifest.selector).toBe('lexical');
    expect(fallback.manifest.fallbackReason).toContain('incompleta');
  });
  it('syncs Recall only after an acknowledgement; retries use the same note ID', async () => {
    const bodies: any[] = [];
    let valid = false;
    config.recallUrl = await http((req: any, res: any) => {
      let body = '';
      req.on('data', (c: any) => (body += c));
      req.on('end', () => {
        bodies.push(JSON.parse(body));
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(valid ? { cursor: 12 } : {}));
      });
    });
    config.recallKey = 'test-only-key';
    await saveMemory({
      project: 'test-project',
      title: 'Recall evidence',
      content: 'Verified evidence in isolated tests.',
      kind: 'evidence',
      verification: 'confirmed',
      source: 'test:recall',
    });
    expect(await syncRecallOnce()).toBe(0);
    expect((await recallStatus()).pending).toBe(1);
    expect((await recallStatus()).synced).toBe(0);
    await pool.query('UPDATE memory_outbox SET next_attempt_at=now()');
    valid = true;
    expect(await syncRecallOnce()).toBe(1);
    expect(bodies[0].id).toBe(bodies[1].id);
    expect(bodies[1].space).toBe('agentos-test');
    expect((await recallStatus()).synced).toBe(1);
  });
  it('protects the API and serves verified artifact downloads', async () => {
    const app = makeApp();
    try {
      expect(
        (await app.inject({ url: '/api/state', headers: { host: '127.0.0.1:4280' } })).statusCode,
      ).toBe(401);
      expect(
        (await app.inject({ url: '/api/session', headers: { host: 'evil.example' } })).statusCode,
      ).toBe(403);
      expect(
        (
          await app.inject({
            url: '/api/session',
            headers: { host: '127.0.0.1:4280', origin: 'https://evil.example' },
          })
        ).statusCode,
      ).toBe(403);
      const session = await app.inject({
        url: '/api/session',
        headers: { host: '127.0.0.1:4280' },
      });
      const cookie = String(session.headers['set-cookie']).split(';')[0];
      expect(
        (await app.inject({ url: '/api/state', headers: { host: '127.0.0.1:4280', cookie } }))
          .statusCode,
      ).toBe(200);
      expect(
        (
          await app.inject({
            url: '/api/state',
            headers: { authorization: 'Bearer ' + config.apiToken },
          })
        ).statusCode,
      ).toBe(200);
    } finally {
      await app.close();
    }
  });
  it('completes a durable six-task DAG after restart without repeating accepted work', async () => {
    const created = await service.createDemo();
    await service.schedule();
    let engine = new WorkerEngine();
    try {
      await engine.start();
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        const detail = await service.goalDetail(created.id);
        if (detail.tasks.some((t) => t.state === 'accepted')) break;
        await delay(200);
      }
      await engine.stop();
      engine = new WorkerEngine();
      await engine.start();
      await expect
        .poll(async () => (await service.goalDetail(created.id)).goal.state, {
          timeout: 20000,
          interval: 300,
        })
        .toBe('completed');
      const detail = await service.goalDetail(created.id);
      expect(detail.tasks).toHaveLength(6);
      expect(detail.tasks.every((t) => t.state === 'accepted')).toBe(true);
      expect(detail.acceptances.filter((a) => a.passed)).toHaveLength(6);
      expect((await pool.query('SELECT * FROM leases')).rowCount).toBe(0);
      const rows = (
        await pool.query('SELECT task_id,count(*) FROM runs GROUP BY task_id HAVING count(*)>2')
      ).rows;
      expect(rows).toHaveLength(0);
      expect((await pool.query('SELECT * FROM contexts')).rowCount).toBeGreaterThanOrEqual(6);
    } finally {
      await engine.stop();
    }
  });
  it('recovers a delivered result when the worker stopped before validation', async () => {
    await service.createGoal(goal(), true);
    const [id] = await service.schedule(),
      run = await service.startRun(id, 'stopped-after-delivery');
    await service.writeArtifact(id, run.fence, 'result.json', '{"validated":true}');
    await service.completeRun(id, run.fence, 'Persisted before shutdown');
    const engine = new WorkerEngine();
    try {
      await engine.start();
      await expect.poll(async () => (await getTask()).state, { timeout: 5000 }).toBe('accepted');
      expect((await pool.query('SELECT * FROM runs')).rowCount).toBe(1);
    } finally {
      await engine.stop();
    }
  });
  it('drains fifty tasks with two workers while maintaining the configured global capacity', async () => {
    await pool.query(
      "UPDATE settings SET value='{" + '"maxRuns":8,"browserSlots":2' + "}' WHERE id='capacity'",
    );
    await pool.query('UPDATE accounts SET max_concurrency=8');
    const created = await service.createGoal(
      goal(Array.from({ length: 50 }, (_, i) => task('load-' + i))),
      true,
    );
    const engines = [new WorkerEngine(), new WorkerEngine()];
    let peak = 0;
    try {
      for (const engine of engines) await engine.start();
      const deadline = Date.now() + 25000;
      while (Date.now() < deadline) {
        const active = Number(
          (
            await pool.query(
              "SELECT count(*) FROM runs WHERE state IN('queued','running','cancelling','reconciling')",
            )
          ).rows[0].count,
        );
        peak = Math.max(peak, active);
        expect(active).toBeLessThanOrEqual(8);
        if ((await service.goalDetail(created.id)).goal.state === 'completed') break;
        await delay(120);
      }
      const detail = await service.goalDetail(created.id);
      expect(detail.goal.state).toBe('completed');
      expect(detail.tasks.every((t) => t.attempt_count === 1 && t.state === 'accepted')).toBe(true);
      expect(peak).toBe(8);
      expect((await pool.query('SELECT * FROM artifacts')).rowCount).toBe(50);
    } finally {
      for (const engine of engines) await engine.stop();
    }
  });
});

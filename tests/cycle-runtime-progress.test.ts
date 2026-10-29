import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { PassThrough } from 'node:stream';
import { EventEmitter } from 'node:events';

vi.mock('node:child_process', async (original) => ({
  ...(await original<typeof import('node:child_process')>()),
  spawn: vi.fn(),
}));
vi.mock('../server/db.js', () => ({
  pool: { query: vi.fn(async () => ({ rows: [], rowCount: 0 })) },
  tx: vi.fn(),
  lock: vi.fn(),
}));
vi.mock('../server/recall.js', () => ({ importRecall: vi.fn(async () => undefined) }));
vi.mock('../server/context-rank.js', () => ({ rankContext: vi.fn(async () => ({})) }));
vi.mock('../server/memory.js', async () => {
  const { createHash } = await import('node:crypto');
  return {
    hash: (value: string | Buffer) => createHash('sha256').update(value).digest('hex'),
    relevance: () => 0,
  };
});
vi.mock('../server/cycles/store.js', () => ({ cycleEvent: vi.fn() }));
// The shared test setup prepares PostgreSQL for integration suites. This suite
// deliberately exercises the real runtime/bridge with no database connection.
vi.mock('../server/schema-readiness.js', () => ({ prepareSchema: vi.fn(async () => undefined) }));
vi.mock('../server/run-criteria-metadata.js', () => ({
  installRunCriteriaMetadata: vi.fn(async () => undefined),
}));

import { spawn } from 'node:child_process';
import { accountBridge } from '../server/bridge.js';
import { config } from '../server/config.js';
import { runCycleAgent } from '../server/cycles/runtime.js';

const HOUR = 60 * 60 * 1000;
const runId = '10000000-0000-4000-8000-000000000001';
const runFence = '20000000-0000-4000-8000-000000000002';
const firstRequest = '30000000-0000-4000-8000-000000000003';
const secondRequest = '40000000-0000-4000-8000-000000000004';
type Envelope = { id?: string; method: string; params: Record<string, any> };
let currentChild:
  | (EventEmitter & { stdin: PassThrough; stdout: PassThrough; stderr: PassThrough })
  | undefined;

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubEnv('AGENTOS_GLM_ONLY', '0');
  vi.stubEnv('AGENTOS_MODEL_POLICY_FILE', '/nonexistent/agentos-cycle-runtime-test-policy.json');
  config.bridgeHost = 'fixture-no-network';
  vi.mocked(spawn).mockClear();
});
afterEach(() => {
  // Settle any outstanding bridge request even when an assertion failed.
  currentChild?.emit('close', 0);
  accountBridge.close();
  currentChild = undefined;
  config.bridgeHost = '';
  vi.unstubAllEnvs();
  vi.useRealTimers();
});

async function startCycle() {
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    killed: false,
  });
  currentChild = child;
  vi.mocked(spawn).mockReturnValue(child as any);
  const sent: Envelope[] = [];
  let buffer = '';
  child.stdin.on('data', (chunk) => {
    buffer += chunk.toString();
    for (;;) {
      const end = buffer.indexOf('\n');
      if (end < 0) break;
      const message = JSON.parse(buffer.slice(0, end)) as Envelope;
      buffer = buffer.slice(end + 1);
      sent.push(message);
      if (message.method === 'worker.prepare')
        child.stdout.write(JSON.stringify({ id: message.id, result: { prepared: true } }) + '\n');
    }
  });
  const controller = new AbortController(),
    onEvent = vi.fn(async () => undefined);
  const occurrence = {
    id: '50000000-0000-4000-8000-000000000005',
    cycle_id: '60000000-0000-4000-8000-000000000006',
    version: 7,
    input: {},
    definition: {
      project: 'fixture',
      name: 'Long repair',
      description: 'Preserve source and verified evidence.',
    },
  };
  const step = {
    id: '70000000-0000-4000-8000-000000000007',
    key: 'repair',
    definition: {
      title: 'Repair the actual defect',
      instructions: 'Keep immutable criteria and publish real test results.',
      workspace: 'fixture/source',
      dependsOn: [],
      readOnly: false,
      criteria: [{ id: 'tests', type: 'artifact', name: 'tests.log', label: 'Actual test output' }],
    },
  };
  const attempt = { id: runId, fence: runFence, number: 1, model: 'glm-5.3-flash' };
  const result = runCycleAgent(
    occurrence,
    step,
    attempt,
    { bridge_id: 'agentos:api:00000000-0000-4000-8000-000000000001' },
    controller.signal,
    onEvent,
  );
  let settled = false;
  const outcome = result
    .then(
      (value) => ({ value, error: undefined }),
      (error) => ({ value: undefined, error }),
    )
    .finally(() => {
      settled = true;
    });
  await vi.advanceTimersByTimeAsync(0);
  const execute = sent.find((message) => message.method === 'worker.execute');
  expect(execute).toBeDefined();
  const id = execute!.id!;
  const event = (kind: string, payload: Record<string, unknown>) =>
    child.stdout.write(JSON.stringify({ id, event: { kind, payload } }) + '\n');
  const reply = (response: Record<string, unknown>) =>
    child.stdout.write(JSON.stringify({ id, ...response }) + '\n');
  return {
    child,
    sent,
    execute: execute!,
    id,
    event,
    reply,
    outcome,
    controller,
    onEvent,
    settled: () => settled,
    cancels: () => sent.filter((message) => message.method === 'cancel'),
  };
}

it('keeps an advancing cycle beyond one hour and forwards the real attempt fence to runtime safeguards', async () => {
  const f = await startCycle();
  expect(f.execute.params.context.manifest).toMatchObject({
    source: 'cycle',
    definitionVersion: 7,
  });
  // Cycles retain their own identity; no fake goal/task/context identifiers.
  for (const field of ['goalId', 'taskId', 'contextVersion'])
    expect(f.execute.params).not.toHaveProperty(field);
  f.event('MODEL_REQUEST', { requestId: firstRequest, requests: 1 });
  await vi.advanceTimersByTimeAsync(25 * 60_000);
  f.event('MODEL_RESPONSE', { requestId: firstRequest, requests: 1 });
  await vi.advanceTimersByTimeAsync(25 * 60_000);
  f.event('HARNESS_TOOL_EVENT', { type: 'tool/result', toolEvents: 2 });
  f.event('MODEL_REQUEST', { requestId: secondRequest, requests: 2 });
  for (const characters of [10, 20]) {
    await vi.advanceTimersByTimeAsync(25 * 60_000);
    f.event('MODEL_STREAM_PROGRESS', {
      requestId: secondRequest,
      characters,
      reasoningCharacters: 0,
      toolCharacters: 0,
      semanticCharacters: characters,
    });
  }
  expect(f.cancels()).toEqual([]);
  expect(f.execute.params).toMatchObject({ runId, runFence, managed: true });
  expect(accountBridge.pending.get(f.id)?.cancelError).toBeUndefined();
  expect(f.settled()).toBe(false);
  f.reply({
    result: { summary: 'Source and evidence returned', evidence: { stopped: true }, artifacts: [] },
  });
  expect(await f.outcome).toMatchObject({
    value: {
      evidence: { stopped: true },
      output: { workspace: 'fixture/oc500000000000400080-repair' },
    },
    error: undefined,
  });
  expect(f.onEvent).toHaveBeenCalledWith(
    'MODEL_STREAM_PROGRESS',
    expect.objectContaining({ semanticCharacters: 20 }),
  );
  expect(accountBridge.pending.size).toBe(0);
  expect(vi.getTimerCount()).toBe(0);
});

it.each(['HEARTBEAT', 'MODEL_QUEUE_WAIT'])(
  '%s cannot keep a silent cycle alive or confirm its termination',
  async (kind) => {
    const f = await startCycle();
    for (let i = 0; i < 3; i++) {
      await vi.advanceTimersByTimeAsync(HOUR / 4);
      f.event(kind, { runId, active: 6, limit: 6, position: 3 - i });
    }
    await vi.advanceTimersByTimeAsync(HOUR / 4);
    expect(accountBridge.pending.get(f.id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
    expect(f.cancels()).toEqual([{ method: 'cancel', params: { id: f.id } }]);
    expect(f.settled()).toBe(false);
    f.reply({ error: { code: 'CANCELLED', message: 'Remote worker has stopped.' } });
    expect(await f.outcome).toMatchObject({
      error: { code: 'RUNTIME_IDLE_TIMEOUT' },
      value: undefined,
    });
    expect(vi.getTimerCount()).toBe(0);
  },
);

it('preserves an unconfirmed stop instead of turning the inactivity request into accepted output', async () => {
  const f = await startCycle();
  await vi.advanceTimersByTimeAsync(HOUR);
  f.reply({ error: { code: 'STOP_UNCONFIRMED', message: 'The sandbox may still be running.' } });
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' }, value: undefined });
  expect(f.sent.filter((message) => message.method === 'worker.execute')).toHaveLength(1);
});

it('expires stop confirmation even if semantic output arrives after cancellation was requested', async () => {
  const f = await startCycle();
  f.event('MODEL_REQUEST', { requestId: firstRequest, requests: 1 });
  await vi.advanceTimersByTimeAsync(HOUR + 14_999);
  f.event('MODEL_STREAM_PROGRESS', {
    requestId: firstRequest,
    characters: 100,
    reasoningCharacters: 0,
    toolCharacters: 0,
    semanticCharacters: 100,
  });
  await vi.advanceTimersByTimeAsync(1);
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' }, value: undefined });
  expect(f.cancels()).toHaveLength(1);
  expect(vi.getTimerCount()).toBe(0);
});

it('rejects a returned delivery that does not prove the runtime stopped', async () => {
  const f = await startCycle();
  f.reply({ result: { summary: 'Model says done', evidence: { stopped: false }, artifacts: [] } });
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' }, value: undefined });
});

it('preserves a confirmed progress-stall failure for recovery instead of treating its checkpoint as completion', async () => {
  const f = await startCycle();
  f.event('RUNTIME_PROGRESS_STALL_CONFIRMED', {
    runId,
    runFence,
    policy: 'failed-evidence-poll-v1',
  });
  expect(f.settled()).toBe(false);
  f.reply({
    error: {
      code: 'RUNTIME_PROGRESS_STALLED',
      message: 'Stopped with an evidence-bound checkpoint.',
    },
  });
  expect(await f.outcome).toMatchObject({
    error: { code: 'RUNTIME_PROGRESS_STALLED' },
    value: undefined,
  });
  expect(f.onEvent).toHaveBeenCalledWith(
    'RUNTIME_PROGRESS_STALL_CONFIRMED',
    expect.objectContaining({ runId, runFence }),
  );
  expect(f.sent.filter((message) => message.method === 'worker.execute')).toHaveLength(1);
});

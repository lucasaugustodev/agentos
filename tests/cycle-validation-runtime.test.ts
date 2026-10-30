import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

const mocks = vi.hoisted(() => {
  const observe = vi.fn(), exec = vi.fn();
  Object.defineProperty(exec, Symbol.for('nodejs.util.promisify.custom'), { value: observe });
  return { observe, exec, spawn: vi.fn(), readFile: vi.fn(), query: vi.fn(), profile: vi.fn() };
});
vi.mock('node:child_process', async original => ({ ...(await original<any>()), spawn: mocks.spawn, execFile: mocks.exec }));
vi.mock('node:fs/promises', async original => ({ ...(await original<any>()), readFile: mocks.readFile }));
vi.mock('../server/db.js', () => ({ pool: { query: mocks.query }, tx: (fn: any) => fn({ query: mocks.query }), lock: vi.fn() }));
vi.mock('../server/cycles/integrations.js', async original => ({ ...(await original<any>()), actionProfiles: mocks.profile }));
vi.mock('../server/cycles/store.js', () => ({ cycleEvent: vi.fn() }));
vi.mock('../server/recall.js', () => ({ importRecall: vi.fn() }));
vi.mock('../server/context-rank.js', () => ({ rankContext: vi.fn() }));
vi.mock('../server/memory.js', async () => {
  const { createHash } = await import('node:crypto');
  return { hash: (v: any) => createHash('sha256').update(v).digest('hex'), relevance: () => 0 };
});
vi.mock('../server/schema-readiness.js', () => ({ prepareSchema: vi.fn() }));
vi.mock('../server/run-criteria-metadata.js', () => ({ installRunCriteriaMetadata: vi.fn() }));

import { runCycleAction } from '../server/cycles/runtime.js';
import { registeredValidationExecutorSha } from '../server/cycles/validation-action.js';

const attempt = '10000000-0000-4000-8000-000000000001';
const unit = `agentos-validation-${attempt}.service`;
const profile = { label: 'Registered validation', command: '/usr/bin/python3',
  args: ['/opt/agentos-control/agentos-release-action.py', 'validate'],
  effect: 'read', projects: ['agentos'], timeoutSeconds: 5 };
let receipt: any, previousSteps: any[];

beforeEach(() => {
  vi.useFakeTimers();
  receipt = undefined; previousSteps = [];
  mocks.spawn.mockReset(); mocks.observe.mockReset(); mocks.query.mockReset();
  mocks.profile.mockResolvedValue({ 'agentos.validate': profile, 'fixture.read': { ...profile, args: ['unrelated-read'] } });
  mocks.observe.mockResolvedValue({ stdout: 'LoadState=not-found\nActiveState=inactive\nMainPID=0\nControlGroup=\n', stderr: '' });
  mocks.readFile.mockRejectedValue(Object.assign(Error('missing'), { code: 'ENOENT' }));
  mocks.query.mockImplementation(async (sql: string, values: any[] = []) => {
    if (sql.startsWith('SELECT * FROM cycle_action_receipts')) return { rows: receipt ? [structuredClone(receipt)] : [] };
    if (sql.startsWith('SELECT key,output')) return { rows: previousSteps };
    if (sql.startsWith('INSERT INTO cycle_action_receipts')) receipt = { state: 'started', input_hash: values[3] };
    if (sql.includes("SET state='started',input_hash")) {
      if (sql.includes('RETURNING action_key') && (receipt.state !== 'completed' || receipt.input_hash !== values[2] || JSON.stringify(receipt.receipt) !== values[3]))
        return { rows: [], rowCount: 0 };
      receipt = { state: 'started', input_hash: values[1] };
    }
    if (sql.includes("SET state='uncertain'")) receipt = { ...receipt, state: 'uncertain', receipt: JSON.parse(values[1]) };
    if (sql.includes("SET state='completed',receipt")) receipt = { ...receipt, state: 'completed', receipt: JSON.parse(values[1]) };
    return { rows: [], rowCount: 1 };
  });
});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

async function start(action = 'agentos.validate', flush = true) {
  const child = Object.assign(new EventEmitter(), { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kill: vi.fn() });
  mocks.spawn.mockReturnValue(child);
  const operation = runCycleAction(
    { id: '20000000-0000-4000-8000-000000000002', cycle_id: '30000000-0000-4000-8000-000000000003',
      version: 1, input: {}, definition: { project: 'agentos', allowedActions: [action] } },
    { key: 'validate', definition: { action, parameters: { workspace: 'agentos/oc-fixture' }, dependsOn: [] } },
    { id: attempt, number: 1 }, new AbortController().signal, vi.fn(async () => undefined),
  );
  const outcome = operation.then(value => ({ value, error: undefined }), error => ({ value: undefined, error }));
  if (flush) await vi.advanceTimersByTimeAsync(0);
  return { child, outcome, reply: (value: any) => { child.stdout.write(JSON.stringify(value)); child.emit('close', 0); } };
}

function result(passed = true) {
  return { ok: true, output: { executorSha256: registeredValidationExecutorSha,
    ...(passed ? { digest: 'a'.repeat(64) } : { passed: false }),
    validationExecution: { unit, stopProof: { unit, mainPID: 0, populated: false,
      controlGroup: '', activeState: 'inactive', loadState: 'not-found' } } },
    artifacts: [{ name: 'verification.json', content: JSON.stringify({ passed }) }] };
}

it.each(['signal', 'nonzero', 'error', 'pipe', 'truncated', 'timeout', 'guard-stop'])('retains uncertain ownership after %s and refuses another dispatch', async kind => {
  const f = await start();
  if (kind === 'signal') f.child.emit('close', null, 'SIGKILL');
  if (kind === 'nonzero') f.child.emit('close', 1);
  if (kind === 'error') f.child.emit('error', Error('lost dispatcher'));
  if (kind === 'pipe') f.child.stdin.emit('error', Error('broken pipe'));
  if (kind === 'truncated') { f.child.stdout.write('{"ok":'); f.child.emit('close', 0); }
  if (kind === 'timeout') await vi.advanceTimersByTimeAsync(5000);
  if (kind === 'guard-stop') { f.child.stderr.write('ValueError: VALIDATION_STOP_UNCONFIRMED'); f.child.emit('close', 1); }
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' }, value: undefined });
  expect(receipt.state).toBe('uncertain');
  // Even a new source/fingerprint cannot clear unknown ownership.
  previousSteps = [{ key: 'repair', output: { workspace: 'agentos/new-source' } }];
  expect(await (await start()).outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(mocks.spawn).toHaveBeenCalledTimes(1);
  expect(mocks.observe).not.toHaveBeenCalled();
});

it('cannot turn a claimed no-effect response into permission to retry a dispatched validator', async () => {
  const f = await start(); f.reply({ ok: false, effect: 'none', code: 'TRY_AGAIN' });
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(receipt.state).toBe('uncertain');
});

it.each(['wrong-attempt', 'wrong-executor', 'missing-proof', 'live-pid', 'populated-cgroup', 'probe-error'])('rejects %s despite a success-shaped response', async kind => {
  const r = result();
  if (kind === 'wrong-attempt') r.output.validationExecution.unit = 'agentos-validation-other.service';
  if (kind === 'wrong-executor') r.output.executorSha256 = 'b'.repeat(64);
  if (kind === 'missing-proof') delete (r.output.validationExecution as any).stopProof;
  if (kind === 'live-pid') mocks.observe.mockResolvedValue({ stdout: 'LoadState=loaded\nActiveState=active\nMainPID=42\nControlGroup=\n' });
  if (kind === 'populated-cgroup') mocks.readFile.mockResolvedValue('populated 1\nfrozen 0\n');
  if (kind === 'probe-error') mocks.observe.mockRejectedValue(Error('unavailable'));
  const f = await start(); f.reply(r);
  expect(await f.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(receipt.state).toBe('uncertain');
});

it('returns a confirmed CI failure with artifacts and preserves its terminal receipt', async () => {
  const f = await start(); f.reply(result(false));
  expect(await f.outcome).toMatchObject({ value: { output: { passed: false }, validationAttemptId: attempt,
    validationStopProof: { unit, populated: false }, artifacts: [{ name: 'verification.json' }] }, error: undefined });
  expect(receipt.state).toBe('completed');
  expect(mocks.observe).toHaveBeenCalledWith('/usr/bin/systemctl', ['show', unit, '--property=LoadState,ActiveState,MainPID,ControlGroup'], expect.anything());
  expect(await (await start()).outcome).toMatchObject({ value: { output: { passed: false } } });
  expect(mocks.spawn).toHaveBeenCalledTimes(1);
});

it('allows a repaired fingerprint after a confirmed terminal validation', async () => {
  const first = await start(); first.reply(result(false)); await first.outcome;
  previousSteps = [{ key: 'repair', output: { workspace: 'agentos/repaired' } }];
  const second = await start(); second.reply(result());
  expect(await second.outcome).toMatchObject({ value: { output: { digest: 'a'.repeat(64) } } });
  expect(mocks.spawn).toHaveBeenCalledTimes(2);
});

it('claims a repaired fingerprint once when two callers resume the completed receipt concurrently', async () => {
  const first = await start(); first.reply(result(false)); await first.outcome;
  previousSteps = [{ key: 'repair', output: { workspace: 'agentos/repaired' } }];
  const contenders = await Promise.all([start('agentos.validate', false), start('agentos.validate', false)]);
  await vi.advanceTimersByTimeAsync(0);
  expect(mocks.spawn).toHaveBeenCalledTimes(2); // Initial validation plus one repaired invocation.
  const child = mocks.spawn.mock.results[1].value;
  child.stdout.write(JSON.stringify(result())); child.emit('close', 0);
  const outcomes = await Promise.all(contenders.map(item => item.outcome));
  expect(outcomes.filter(item => item.value)).toHaveLength(1);
  expect(outcomes.filter(item => item.error?.code === 'STOP_UNCONFIRMED')).toHaveLength(1);
});

it('does not admit a cached legacy completion without its stop contract', async () => {
  const first = await start(); first.reply(result()); await first.outcome;
  delete receipt.receipt.validationAttemptId;
  expect(await (await start()).outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(mocks.spawn).toHaveBeenCalledTimes(1);
});

it('keeps started ownership if persistence of the terminal response fails', async () => {
  const query = mocks.query.getMockImplementation()!;
  mocks.query.mockImplementation(async (sql, values) => {
    if (sql.includes("SET state='completed',receipt")) throw Error('database connection lost');
    return query(sql, values);
  });
  const first = await start(); first.reply(result());
  expect(await first.outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(receipt.state).toBe('started');
  expect(await (await start()).outcome).toMatchObject({ error: { code: 'STOP_UNCONFIRMED' } });
  expect(mocks.spawn).toHaveBeenCalledTimes(1);
});

it('refuses an altered reserved profile before spawning or claiming the action', async () => {
  mocks.profile.mockResolvedValue({ 'agentos.validate': { ...profile, args: ['unreviewed.py'] } });
  expect(await (await start()).outcome).toMatchObject({ error: { code: 'ACTION_UNAVAILABLE' } });
  expect(mocks.spawn).not.toHaveBeenCalled();
  expect(receipt).toBeUndefined();
});

it('keeps ordinary read-profile failure and retry behavior', async () => {
  const first = await start('fixture.read'); first.child.emit('close', 1);
  expect(await first.outcome).toMatchObject({ error: { code: 'ACTION_FAILED' } });
  const second = await start('fixture.read'); second.reply({ ok: true, output: { read: true } });
  expect(await second.outcome).toMatchObject({ value: { output: { read: true } } });
  expect(mocks.spawn).toHaveBeenCalledTimes(2);
  expect(mocks.observe).not.toHaveBeenCalled();
});

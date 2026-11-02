import { beforeAll, beforeEach, afterEach, afterAll, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import { CycleEngine } from '../server/cycles/engine.js';
import { createCycle, runCycle, occurrenceDetail, cycleEvent } from '../server/cycles/store.js';
import { saveCycleArtifacts, readCycleArtifact } from '../server/cycles/runtime.js';
import { claimCycleTerminationInspections, inspectAndRecordCycleTermination, type TerminationClaim } from '../server/cycles/termination-evidence.js';

let engine: CycleEngine, calls: any[], cause: string, emitStarted: boolean;
beforeAll(async () => { await migrate(); });
beforeEach(async () => {
  await pool.query('TRUNCATE cycles,cycle_commands,accounts,goals,memories,events CASCADE');
  config.bridgeHost = 'fixture'; config.remoteExecution = true;
  engine = new CycleEngine(); vi.spyOn(engine, 'tick').mockResolvedValue();
  calls = []; cause = 'HARNESS_SANDBOX_OOM'; emitStarted = true;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown,max_concurrency)
    VALUES('a','Isolated','muse','muse-spark-1.3','["muse-spark-1.3"]',true,'a',true,1)`);
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string, request: any, options: any) => {
    if (method !== 'worker.execute') return method === 'memory.search' ? { items: [] } : { scores: {}, truncated: false };
    calls.push(request);
    if (emitStarted) options.onEvent('HARNESS_STARTED', { unit: 'agentos-run-' + request.runId, workspace: request.workspace });
    throw Object.assign(Error('Runtime failed; immutable source is preserved.'), { code: cause });
  });
});
afterEach(async () => { await engine.close(); vi.restoreAllMocks(); config.bridgeHost = ''; config.remoteExecution = false; });
afterAll(async () => { await pool.end(); });

async function failed() {
  const c = await createCycle({ name: 'Terminal proof isolated', project: 'fixture', trigger: { kind: 'manual' },
    steps: [{ key: 'qa', title: 'Validation', kind: 'agent', workspace: 'fixture/source', maxAttempts: 1,
      criteria: [{ id: 'proof', type: 'artifact', name: 'qa.json', label: 'Independent proof' }] }] }, randomUUID());
  const o = await runCycle(c.id, randomUUID(), {});
  await engine.execute(await engine.claim());
  const d = await occurrenceDetail(o.id);
  return { o, d, a: d.attempts[0] };
}
async function claim() {
  const claims = await claimCycleTerminationInspections(); expect(claims).toHaveLength(1); return claims[0];
}
function proof(c: TerminationClaim) {
  const a = c.authority, checkedAt = new Date().toISOString();
  return { version: 1, runId: a.runId, runFence: a.runFence, workspace: a.workspace,
    runtime: 'harness', state: 'failed', code: a.code, stateDigest: 'a'.repeat(64), checkedAt,
    process: { unit: 'agentos-run-' + a.runId, checkedAt, stopped: true, mainPid: 0, populated: false,
      activeState: 'failed', loadState: 'loaded', cgroupExists: false },
    termination: { source: 'systemd-journal', unit: 'agentos-run-' + a.runId + '.service', result: 'oom-kill' } };
}
const deliver = (value: any) => vi.fn(async (_file: string, _args: string[], _options: any) => ({ stdout: JSON.stringify(value) }));
async function expectUnchanged(f: Awaited<ReturnType<typeof failed>>) {
  const d = await occurrenceDetail(f.o.id);
  expect(d.attempts).toEqual(f.d.attempts); expect(d.steps).toEqual(f.d.steps);
  expect(d.occurrence).toEqual(f.d.occurrence); expect(calls).toHaveLength(1);
}

it('reconciles only the authentic stopped receipt, preserving OOM, state, artifacts and no retry or reservation', async () => {
  const f = await failed(); await saveCycleArtifacts(f.a.id, [{ name: 'partial.txt', content: 'preserved partial failure' }]);
  const before = await occurrenceDetail(f.o.id), oldArtifacts = before.artifacts;
  const claimValue = await claim(), execute = deliver(proof(claimValue));
  expect(await inspectAndRecordCycleTermination(claimValue, execute)).toBe(true);
  const after = await occurrenceDetail(f.o.id);
  expect({ ...after.attempts[0], receipt: before.attempts[0].receipt }).toEqual(before.attempts[0]);
  expect(after.attempts[0]).toMatchObject({ state: 'failed', error_code: 'HARNESS_SANDBOX_OOM', receipt: {
    stopped: true, kind: 'harness-terminal-evidence', originalErrorCode: 'HARNESS_SANDBOX_OOM' } });
  expect(after.steps).toEqual(before.steps); expect(after.occurrence).toEqual(before.occurrence);
  expect(after.artifacts).toEqual(oldArtifacts);
  expect((await readCycleArtifact(oldArtifacts[0].id)).bytes.toString()).toBe('preserved partial failure');
  expect(await engine.claim()).toBeNull(); expect(calls).toHaveLength(1);
  expect(calls[0].memoryReservation).toBeUndefined(); expect(calls[0].launchToken).toBeUndefined();
  expect((await pool.query('SELECT count(*)::int n FROM opencode_jobs')).rows[0].n).toBe(0);
  expect(execute.mock.calls[0][0]).toBe('ssh');
  const args = (execute.mock.calls[0] as any)[1];
  expect(args).toContain('/opt/agentos-bridge/inspect-harness-termination.mjs');
  expect(args.slice(-3)).toEqual([f.a.id, claimValue.authority.workspace, f.a.fence]);
  expect(args).not.toContain('/opt/agentos/current');
  expect(await claimCycleTerminationInspections()).toEqual([]);
  expect(await inspectAndRecordCycleTermination(claimValue, execute)).toBe(false);
  expect(execute).toHaveBeenCalledTimes(1);
  expect((await pool.query("SELECT count(*)::int n FROM cycle_events WHERE kind='TERMINATION_EVIDENCE_RECORDED'")).rows[0].n).toBe(1);
});

it('does not relabel a historical RUNTIME_EXITED even with typed OOM evidence', async () => {
  cause = 'RUNTIME_EXITED'; const f = await failed(), c = await claim();
  expect(await inspectAndRecordCycleTermination(c, deliver(proof(c)))).toBe(true);
  const d = await occurrenceDetail(f.o.id); expect(d.attempts[0].error_code).toBe('RUNTIME_EXITED');
  expect(d.steps[0].state).toBe('attention'); expect(calls).toHaveLength(1);
});

it.each(['RPC_ERROR', 'STOP_UNCONFIRMED', 'RESULT_FORMAT'])('never claims %s as confirmed OOM', async code => {
  cause = code; const f = await failed(); expect(await claimCycleTerminationInspections()).toEqual([]);
  expect((await occurrenceDetail(f.o.id)).attempts[0].receipt).toBeNull();
});
it('never claims a runtime which did not emit its bound launch event', async () => {
  emitStarted = false; const f = await failed(); expect(await claimCycleTerminationInspections()).toEqual([]);
  await expectUnchanged(f);
});
it('never claims a contradictory not-started event', async () => {
  const f = await failed(); await cycleEvent(pool, f.o.cycle_id, f.o.id, 'HARNESS_NOT_STARTED', { confirmed: true }, f.a.step_id, f.a.id);
  expect(await claimCycleTerminationInspections()).toEqual([]); await expectUnchanged(f);
});

it.each(['run', 'fence', 'workspace', 'code', 'state', 'digest', 'stale', 'future', 'pid', 'populated', 'unit', 'active', 'checkedAt', 'journal', 'journalUnit', 'journalSource'])
('rejects invalid %s proof without changing the failed actor', async field => {
  const f = await failed(), c = await claim(), p: any = proof(c);
  if (field === 'run') p.runId = randomUUID();
  if (field === 'fence') p.runFence = randomUUID();
  if (field === 'workspace') p.workspace = 'other/workspace';
  if (field === 'code') p.code = 'different';
  if (field === 'state') p.state = 'completed';
  if (field === 'digest') p.stateDigest = 'invalid';
  if (field === 'stale') p.checkedAt = p.process.checkedAt = new Date(Date.now() - 61000).toISOString();
  if (field === 'future') p.checkedAt = p.process.checkedAt = new Date(Date.now() + 10000).toISOString();
  if (field === 'pid') p.process.mainPid = 42;
  if (field === 'populated') p.process.populated = true;
  if (field === 'unit') p.process.unit = 'agentos-run-' + randomUUID();
  if (field === 'active') p.process.activeState = 'active';
  if (field === 'checkedAt') p.process.checkedAt = new Date(Date.now() - 1000).toISOString();
  if (field === 'journal') p.termination = null;
  if (field === 'journalUnit') p.termination.unit = 'agentos-run-' + randomUUID() + '.service';
  if (field === 'journalSource') p.termination.source = 'model-output';
  expect(await inspectAndRecordCycleTermination(c, deliver(p))).toBe(false); await expectUnchanged(f);
});

it('inspector errors and malformed output keep uncertainty, without duplicate evidence', async () => {
  const f = await failed(), c = await claim(), execute = vi.fn(async () => { throw Error('private diagnostic'); });
  expect(await inspectAndRecordCycleTermination(c, execute)).toBe(false);
  const duplicate = deliver({ workspace: 'forged' });
  expect(await inspectAndRecordCycleTermination(c, duplicate)).toBe(false); expect(duplicate).not.toHaveBeenCalled();
  await expectUnchanged(f);
  const events = (await pool.query("SELECT payload FROM cycle_events WHERE kind='TERMINATION_EVIDENCE_UNAVAILABLE'")).rows;
  expect(events).toHaveLength(1); expect(JSON.stringify(events)).not.toContain('private diagnostic');
});

it('does not hold the scheduler lock while the physical inspector is waiting', async () => {
  await failed(); const c = await claim();
  let release!: (value: { stdout: string }) => void;
  const execute = vi.fn(() => new Promise<{ stdout: string }>(resolve => { release = resolve; }));
  const pending = inspectAndRecordCycleTermination(c, execute);
  await vi.waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
  const other = await createCycle({ name: 'Independent ready work', project: 'fixture', trigger: { kind: 'manual' },
    steps: [{ key: 'work', title: 'Independent step', kind: 'agent', workspace: 'fixture/source', maxAttempts: 1,
      criteria: [{ id: 'file', label: 'Delivery', type: 'artifact', name: 'result.json' }] }] }, randomUUID());
  await runCycle(other.id, randomUUID(), {});
  expect(await engine.claim()).not.toBeNull();
  release({ stdout: JSON.stringify(proof(c)) });
  expect(await pending).toBe(true);
});

it.each(['fence', 'newAttempt', 'pause', 'manifest', 'receipt'])('CAS refuses %s changes during physical inspection', async change => {
  const f = await failed(), c = await claim();
  const execute = vi.fn(async () => {
    if (change === 'fence') await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE id=$1', [f.a.id, randomUUID()]);
    if (change === 'newAttempt') {
      await pool.query("UPDATE cycle_steps SET attempt=2,state='running' WHERE id=$1", [f.a.step_id]);
      await pool.query("INSERT INTO cycle_attempts(id,step_id,number,fence,owner,account_id) VALUES($1,$2,2,$3,'other','a')", [randomUUID(), f.a.step_id, randomUUID()]);
    }
    if (change === 'pause') await pool.query("UPDATE cycle_occurrences SET state='paused' WHERE id=$1", [f.o.id]);
    if (change === 'manifest') await pool.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{manifest,cycleInput,digest}','\"changed\"') WHERE id=$1", [f.a.id]);
    if (change === 'receipt') await pool.query("UPDATE cycle_attempts SET receipt='{\"preserve\":true}' WHERE id=$1", [f.a.id]);
    return { stdout: JSON.stringify(proof(c)) };
  });
  expect(await inspectAndRecordCycleTermination(c, execute)).toBe(false);
  expect((await pool.query("SELECT count(*)::int n FROM cycle_events WHERE kind='TERMINATION_EVIDENCE_RECORDED'")).rows[0].n).toBe(0);
  expect((await pool.query('SELECT error_code,receipt FROM cycle_attempts WHERE id=$1', [f.a.id])).rows[0]).toEqual({
    error_code: 'HARNESS_SANDBOX_OOM', receipt: change === 'receipt' ? { preserve: true } : null });
});

it('backs off durably after unavailable inspections and recovers automatically without heartbeat renewal', async () => {
  const f = await failed();
  for (let i = 0; i < 3; i++) {
    const c = await claim(); await inspectAndRecordCycleTermination(c, deliver({ error: 'STOP_UNCONFIRMED' }));
    expect(await claimCycleTerminationInspections()).toEqual([]);
    if (i > 0) {
      await pool.query("UPDATE cycle_events SET created_at=now()-interval '61 seconds' WHERE kind='TERMINATION_EVIDENCE_INSPECTION_REQUESTED'");
      expect(await claimCycleTerminationInspections()).toEqual([]);
    }
    await pool.query("UPDATE cycle_events SET created_at=now()-($1::int*interval '1 second') WHERE kind='TERMINATION_EVIDENCE_INSPECTION_REQUESTED'",
      [i === 0 ? 61 : i === 1 ? 301 : 1801]);
  }
  await expectUnchanged(f);
  const recovered = await claim();
  expect(await inspectAndRecordCycleTermination(recovered, deliver(proof(recovered)))).toBe(true);
  expect(await claimCycleTerminationInspections()).toEqual([]);
  expect((await occurrenceDetail(f.o.id)).attempts[0]).toMatchObject({ state: 'failed', error_code: cause, receipt: { stopped: true } });
});

it('fairly claims at most two and skips already-inspected or invalid frozen identities', async () => {
  const all = []; for (let i = 0; i < 5; i++) all.push(await failed());
  await pool.query("UPDATE cycle_attempts SET context='{}' WHERE id=$1", [all[0].a.id]);
  const first = await claimCycleTerminationInspections(20); expect(first).toHaveLength(2);
  const second = await claimCycleTerminationInspections(20); expect(second).toHaveLength(2);
  expect(new Set([...first, ...second].map(c => c.authority.runId)).size).toBe(4);
  expect(await claimCycleTerminationInspections()).toEqual([]);
});

it('does not let sixteen string-version manifests starve a valid later failure', async () => {
  const invalid = []; for (let i = 0; i < 16; i++) invalid.push((await failed()).a.id);
  await pool.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{manifest,definitionVersion}','\"1\"') WHERE id=ANY($1::uuid[])", [invalid]);
  const valid = await failed();
  const claims = await claimCycleTerminationInspections();
  expect(claims.map(c => c.authority.runId)).toEqual([valid.a.id]);
  expect((await pool.query("SELECT count(*)::int n FROM cycle_events WHERE kind='TERMINATION_EVIDENCE_INSPECTION_REQUESTED' AND attempt_id=ANY($1::uuid[])", [invalid])).rows[0].n).toBe(0);
});

it('rejects invalid per-sweep limits without creating inspection claims', async () => {
  await failed();
  for (const value of [NaN, Infinity, -1, 1.5, 0]) expect(await claimCycleTerminationInspections(value)).toEqual([]);
  expect(await claimCycleTerminationInspections(1)).toHaveLength(1);
});

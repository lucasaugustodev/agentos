import { beforeAll, beforeEach, afterEach, afterAll, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { hash } from '../server/memory.js';
import { accountBridge } from '../server/bridge.js';
import { createCycle, runCycle, occurrenceDetail } from '../server/cycles/store.js';
import { CycleEngine } from '../server/cycles/engine.js';
import { parseCycle } from '../server/cycles/contracts.js';
import { freezeDependencyEvidence } from '../server/cycles/dependency-evidence.js';
import { cpuReplayIO, observeCPUReplay, cpuReplayCanonical, cpuReplayArgv } from '../server/cycles/cpu-replay.js';
const actualInvoke = cpuReplayIO.invoke;

const checker = 'print("synthetic fixture only")\n', candidate = 'synthetic candidate\n', result = '{"passed":true}\n';
const ref = (name: string, content: string) => ({ name, content });
const bindings = [
  { reportArtifact: 'qa-verification.json', jsonPointer: '/cpuEvidence/checkerSHA256', dependencyArtifact: 'cpu-checks.py' },
  { reportArtifact: 'qa-verification.json', jsonPointer: '/cpuEvidence/resultSHA256', dependencyArtifact: 'cpu-result.json' },
];
const definition = (enabled = true, profile = 'python-cpu-argv-v1'): any => ({
  name: 'Isolated CPU replay observation', project: 'fixture', trigger: { kind: 'manual' },
  allowedActions: enabled ? ['native.cpu-replay.observe'] : [],
  steps: [
    { key: 'produce', title: 'Frozen producer', kind: 'agent', workspace: 'fixture/source', maxAttempts: 1,
      criteria: [{ id: 'cpu', label: 'CPU evidence', type: 'artifact', name: 'cpu-result.json' }] },
    { key: 'qa', title: 'Independent QA', kind: 'agent', workspace: 'fixture/source', dependsOn: ['produce'], maxAttempts: 3,
      parameters: { nativeQAContractVersion: 2, ...(enabled ? { cpuReplayObservationVersion: 1, cpuReplayProfile: profile } : {}) },
      dependencyEvidence: { version: 1, sourceStep: 'produce', bindings },
      criteria: [{ id: 'passed', label: 'Independent QA passed', type: 'json_field', name: 'qa-verification.json', field: 'passed', equals: true }] },
  ],
});
const registration = () => ({ path: '/isolated/registered.json', digest: 'a'.repeat(64), value: {
  version: 1 as const, enabled: true as const, executorPath: '/isolated/cpu-replay.py', executorSHA256: 'b'.repeat(64),
  stateDirectory: '/isolated/cpu-replay-state', user: 'agentos-build', projects: ['fixture'],
  profiles: ['python-cpu-argv-v1', 'python-cpu-candidate-base-v1'] as ('python-cpu-argv-v1' | 'python-cpu-candidate-base-v1')[],
  limits: { memoryMiB: 128, timeoutSeconds: 10, maxOutputBytes: 16384 },
} });
const encoded = (text = '') => ({ digest: hash(text), size: Buffer.byteLength(text), base64: Buffer.from(text).toString('base64') });
function receipt(request: any) {
  const r = registration();
  return { version: 1, kind: 'cpu-replay-observation', requestId: request.requestId, occurrenceId: request.occurrenceId,
    runFence: request.runFence, project: request.project, source: request.source, profile: request.profile, baseSource: request.baseSource,
    requestSHA256: hash(cpuReplayCanonical(request)), registrationSHA256: r.digest, executorSHA256: r.value.executorSHA256,
    observationOnly: true, acceptance: false, argv: cpuReplayArgv(request.profile), startedAt: '2026-01-01T00:00:00Z',
    finishedAt: '2026-01-01T00:00:01Z', exitCode: 0, signal: null, timedOut: false, oomKilled: false,
    memoryPeakBytes: 123456, rssPeakBytes: null, unit: `agentos-cpu-replay-${request.requestId}.service`,
    stopProof: { stopped: true, mainPID: 0, populated: false, unit: `agentos-cpu-replay-${request.requestId}.service` },
    outputs: { stdout: encoded('synthetic run'), stderr: encoded(), result: encoded('{"passed":true}') } };
}
let engine: CycleEngine, qaRequests: any[], modes: string[], observations: any[], mutate: (r: any) => void, qaPassed: boolean;
beforeAll(async () => { await migrate(); });
beforeEach(async () => {
  await pool.query('TRUNCATE cycles,cycle_commands,accounts,goals,memories,events CASCADE');
  engine = new CycleEngine(); vi.spyOn(engine, 'tick').mockResolvedValue();
  config.bridgeHost = 'fixture'; config.remoteExecution = true;
  await pool.query("INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown,max_concurrency) VALUES('a','Isolated','muse','muse-spark-1.3','[\"muse-spark-1.3\"]',true,'a',true,1)");
  qaRequests = []; modes = []; observations = []; mutate = () => {}; qaPassed = true;
  vi.spyOn(cpuReplayIO, 'registration').mockResolvedValue(registration());
  vi.spyOn(cpuReplayIO, 'invoke').mockImplementation(async (mode, _registered, request) => {
    modes.push(mode); observations.push(request);
    const durable = (await pool.query('SELECT state,input_hash FROM cycle_action_receipts WHERE occurrence_id=$1', [request.occurrenceId])).rows;
    expect(durable.length).toBeGreaterThan(0);
    expect(durable.some(r => r.input_hash === hash(cpuReplayCanonical(request)))).toBe(true);
    const r = receipt(request); mutate(r); return { ok: true, receipt: r };
  });
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string, request: any) => {
    if (method !== 'worker.execute') return method === 'memory.search' ? { items: [] } : { scores: {}, truncated: false };
    if (request.context.manifest.step === 'produce') return { summary: 'Synthetic producer', evidence: { stopped: true }, artifacts: [
      ref('cpu-checks.py', checker), ref('cpu-result.json', result), ref('candidate.py', candidate), ref('implementation.patch', 'synthetic patch\n'),
    ] };
    qaRequests.push(request);
    return { summary: 'Independent QA', evidence: { stopped: true }, artifacts: [ref('qa-verification.json', JSON.stringify({
      passed: qaPassed, cpuEvidence: { checkerSHA256: hash(checker), resultSHA256: hash(result) },
    }))] };
  });
});
afterEach(async () => { await engine.close(); vi.restoreAllMocks(); config.bridgeHost = ''; config.remoteExecution = false; });
afterAll(async () => { await pool.end(); });
async function prepare(enabled = true, profile = 'python-cpu-argv-v1', input = {}) {
  const c = await createCycle(definition(enabled, profile), randomUUID());
  const o = await runCycle(c.id, randomUUID(), input);
  await engine.execute(await engine.claim()); return { o, claim: await engine.claim() };
}
async function execute(enabled = true) {
  const p = await prepare(enabled); await engine.execute(p.claim); return occurrenceDetail(p.o.id);
}
it('is completely off by default: no registration read, intent, launcher or observation input', async () => {
  await execute(false);
  expect(cpuReplayIO.registration).not.toHaveBeenCalled(); expect(modes).toEqual([]);
  expect((await pool.query('SELECT * FROM cycle_action_receipts')).rows).toEqual([]);
  expect(qaRequests[0].context.manifest.cpuReplayObservation).toBeUndefined();
});
it.each([
  ['missing capability', (d: any) => { d.allowedActions = []; }],
  ['version', (d: any) => { d.steps[1].parameters.cpuReplayObservationVersion = 2; }],
  ['legacy QA', (d: any) => { d.steps[1].parameters.nativeQAContractVersion = 1; }],
  ['free argv', (d: any) => { d.steps[1].parameters.cpuReplayProfile = 'bash -c echo'; }],
  ['profile without opt-in', (d: any) => { delete d.steps[1].parameters.cpuReplayObservationVersion; }],
])('rejects an unregistered capability shape: %s', (_name, change) => {
  const d = definition(); change(d); expect(() => parseCycle(d)).toThrow();
});
it('stages a controller-owned receipt without adding a DAG step or replacing QA criteria', async () => {
  const d = await execute(); expect(d.steps).toHaveLength(2); expect(modes).toEqual(['execute', 'inspect']);
  expect(qaRequests).toHaveLength(1);
  expect(qaRequests[0].context.manifest.cpuReplayObservation).toMatchObject({ observationOnly: true, acceptance: false });
  expect(qaRequests[0].inputs.some((i: any) => i.name === 'cpu-replay-observation.json')).toBe(true);
  expect(qaRequests[0].prompt).toContain('Existing independent QA');
  expect(d.artifacts.some((a: any) => a.name === 'cpu-replay-observation.json')).toBe(false);
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('completed');
});
it('keeps a semantic failure failed despite a successful CPU receipt', async () => {
  qaPassed = false; const d = await execute();
  expect(d.attempts.find((a: any) => a.error_code === 'DELIVERY_INVALID')).toBeTruthy();
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
});
it('preserves negative exit/OOM/timeout observation and still asks QA for an independent diagnosis', async () => {
  mutate = r => { r.exitCode = null; r.signal = 9; r.timedOut = true; r.oomKilled = true; r.outputs.result = null; };
  qaPassed = false; await execute();
  expect(qaRequests[0].context.manifest.cpuReplayObservation).toMatchObject({ exitCode: null, signal: 9, oomKilled: true, acceptance: false });
});
it.each([
  ['fence', (r: any) => { r.runFence = randomUUID(); }],
  ['source', (r: any) => { r.source = { ...r.source, attemptId: randomUUID() }; }],
  ['request hash', (r: any) => { r.requestSHA256 = 'c'.repeat(64); }],
  ['registration', (r: any) => { r.registrationSHA256 = 'c'.repeat(64); }],
  ['argv', (r: any) => { r.argv.push(';echo success'); }],
  ['output digest', (r: any) => { r.outputs.stdout.digest = 'c'.repeat(64); }],
  ['no physical stop', (r: any) => { r.stopProof.populated = true; }],
  ['nonfinite metrics', (r: any) => { r.memoryPeakBytes = Infinity; }],
  ['acceptance claim', (r: any) => { r.acceptance = true; }],
])('does not start QA for an invalid executor receipt: %s', async (_name, change) => {
  mutate = change; const d = await execute(); expect(qaRequests).toHaveLength(0);
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('uncertain');
});
it('reconciles a lost response through inspect only, never redispatching the same intent', async () => {
  const { claim } = await prepare(), snap = await freezeDependencyEvidence(claim.o, claim.s, claim.a);
  vi.mocked(cpuReplayIO.invoke).mockRejectedValueOnce(new Error('transport lost'));
  const signal = new AbortController().signal;
  await expect(observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn())).rejects.toThrow('transport lost');
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('uncertain');
  const r = await observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn());
  expect(r.acceptance).toBe(false); expect(modes).toEqual(['inspect', 'inspect']);
  expect(vi.mocked(cpuReplayIO.invoke).mock.calls.map(c => c[0])).toEqual(['execute', 'inspect', 'inspect']);
});
it('exposes the initial admission limitation: busy is quarantined and cannot start QA', async () => {
  vi.mocked(cpuReplayIO.invoke).mockResolvedValueOnce({ ok: false, code: 'CPU_REPLAY_BUSY' });
  const d = await execute();
  expect(qaRequests).toHaveLength(0);
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('uncertain');
  expect(vi.mocked(cpuReplayIO.invoke).mock.calls.map(c => c[0])).toEqual(['execute']);
});
it('reuses a completed observation via protected inspection without running it twice', async () => {
  const { claim } = await prepare(), snap = await freezeDependencyEvidence(claim.o, claim.s, claim.a), signal = new AbortController().signal;
  await observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn());
  await observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn());
  expect(modes).toEqual(['execute', 'inspect', 'inspect', 'inspect']);
});
it('pins the original registration in the durable intent and cannot inspect with a replacement registration', async () => {
  const { claim } = await prepare(), snap = await freezeDependencyEvidence(claim.o, claim.s, claim.a), signal = new AbortController().signal;
  await observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn());
  vi.mocked(cpuReplayIO.registration).mockResolvedValue({ ...registration(), digest: 'c'.repeat(64) });
  await expect(observeCPUReplay(claim.o, claim.s, claim.a, snap, signal, vi.fn())).rejects.toMatchObject({ code: 'CPU_REPLAY_INPUT_CHANGED' });
  expect(modes).toEqual(['execute', 'inspect']);
});
it('does not spawn when cancellation arrives during the awaited registration reread', async () => {
  let release!: (value: ReturnType<typeof registration>) => void;
  vi.mocked(cpuReplayIO.registration).mockImplementationOnce(() => new Promise(resolve => { release = resolve; }));
  const spawn = vi.spyOn(cpuReplayIO, 'spawn').mockImplementation((): never => { throw new Error('must not spawn'); });
  const controller = new AbortController();
  const pending = actualInvoke('execute', registration(), {}, controller.signal);
  controller.abort(); release(registration());
  await expect(pending).rejects.toMatchObject({ code: 'CPU_REPLAY_CANCELLED' });
  expect(spawn).not.toHaveBeenCalled();
});
it('does not commit observation across a changed QA fence', async () => {
  const { claim } = await prepare();
  vi.mocked(cpuReplayIO.invoke).mockImplementationOnce(async (mode, reg, request) => {
    const r = receipt(request); await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE id=$1', [claim.a.id, randomUUID()]);
    return { ok: true, receipt: r };
  });
  await engine.execute(claim); expect(qaRequests).toHaveLength(0);
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('started');
});
it('preserves a pause or changed occurrence input that arrives while the tool runs', async () => {
  const { claim } = await prepare();
  vi.mocked(cpuReplayIO.invoke).mockImplementationOnce(async (_mode, _reg, request) => {
    await pool.query("UPDATE cycle_occurrences SET state='paused',input=$2 WHERE id=$1", [claim.o.id, { changed: true }]);
    return { ok: true, receipt: receipt(request) };
  });
  await engine.execute(claim); expect(qaRequests).toHaveLength(0);
  expect((await occurrenceDetail(claim.o.id)).occurrence.state).toBe('paused');
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows[0].state).toBe('started');
});
it('an uncertain prior tool in the occurrence prevents a fresh attempt from dispatching another tool', async () => {
  const { claim } = await prepare();
  await pool.query("INSERT INTO cycle_action_receipts(action_key,occurrence_id,action,state,input_hash) VALUES($1,$2,'native.cpu-replay.observe','uncertain',$3)",
    [`${claim.o.id}:old-tool:cpu-replay-v1`, claim.o.id, 'd'.repeat(64)]);
  await engine.execute(claim); expect(modes).toEqual([]); expect(qaRequests).toHaveLength(0);
  expect((await pool.query('SELECT state FROM cycle_action_receipts')).rows.map(x => x.state)).toEqual(['uncertain']);
});
it('does not dispatch after cancellation or consume another project registration', async () => {
  const p = await prepare(), snap = await freezeDependencyEvidence(p.claim.o, p.claim.s, p.claim.a);
  const c = new AbortController(); c.abort();
  await expect(observeCPUReplay(p.claim.o, p.claim.s, p.claim.a, snap, c.signal, vi.fn())).rejects.toMatchObject({ code: 'CPU_REPLAY_CANCELLED' });
  expect(modes).toEqual([]);
  vi.mocked(cpuReplayIO.registration).mockResolvedValue({ ...registration(), value: { ...registration().value, projects: ['other'] } });
  await expect(observeCPUReplay(p.claim.o, p.claim.s, p.claim.a, snap, new AbortController().signal, vi.fn())).rejects.toMatchObject({ code: 'CPU_REPLAY_SCOPE' });
});
it('does not interpret invalid/missing base material as an optional positional input', async () => {
  const p = await prepare(true, 'python-cpu-candidate-base-v1'); await engine.execute(p.claim);
  expect(qaRequests).toHaveLength(0); expect(modes).toEqual([]);
  expect((await occurrenceDetail(p.o.id)).attempts.some((a: any) => a.error_code === 'CPU_REPLAY_BASE_BINDING')).toBe(true);
});
it('binds positional base bytes to an actual same-project source artifact and occurrence', async () => {
  const sourceDefinition = definition(false); sourceDefinition.steps = [sourceDefinition.steps[0]];
  const c = await createCycle(sourceDefinition, randomUUID()), source = await runCycle(c.id, randomUUID(), {});
  await engine.execute(await engine.claim()); const d = await occurrenceDetail(source.id);
  const base = d.artifacts.find((a: any) => a.name === 'candidate.py');
  const input = { experiment: { problem: 'fixture_board', sourceArtifact: {
    id: base.id, name: base.name, digest: base.digest, size: base.size, occurrenceId: source.id, problem: 'fixture_board' },
    materials: [{ name: 'base-candidate.py', sha256: hash(candidate), bytes: Buffer.byteLength(candidate), base64: Buffer.from(candidate).toString('base64') }] } };
  const p = await prepare(true, 'python-cpu-candidate-base-v1', input); await engine.execute(p.claim);
  expect(modes).toEqual(['execute', 'inspect']);
  expect(observations[0].artifacts.map((a: any) => a.name)).toEqual(['candidate.py', 'cpu-checks.py', 'cpu-result.json', 'implementation.patch', 'base-candidate.py']);
  expect(observations[0].baseSource).toMatchObject({ artifact: { id: base.id, digest: base.digest }, source: { occurrenceId: source.id } });
});

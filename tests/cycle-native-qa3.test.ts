import { beforeAll, beforeEach, afterEach, afterAll, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { hash } from '../server/memory.js';
import { accountBridge } from '../server/bridge.js';
import { createCycle, runCycle, occurrenceDetail } from '../server/cycles/store.js';
import { CycleEngine } from '../server/cycles/engine.js';
import { parseCycle } from '../server/cycles/contracts.js';
import { readCycleArtifact } from '../server/cycles/runtime.js';
import { cpuReplayIO, cpuReplayCanonical, cpuReplayArgv, buildCPUReplayRequest } from '../server/cycles/cpu-replay.js';
import { freezeDependencyEvidence } from '../server/cycles/dependency-evidence.js';
import { nativeQA3Evidence } from '../server/cycles/native-qa3.js';
import { makeApp } from '../server/app.js';

const base = 'value = 1\n', candidate = 'value = 2\n', checker = '# Synthetic checker bytes; never executed by these DB tests.\n';
const patch = '--- a/candidate.py\n+++ b/candidate.py\n@@ -1 +1 @@\n-value = 1\n+value = 2\n';
const encoded = (text: string | Buffer = '') => ({ digest: hash(text), size: Buffer.byteLength(text), base64: Buffer.from(text).toString('base64') });
const cpuResult = () => ({ passed: true, candidateSHA256: hash(candidate), checkerSHA256: hash(checker),
  cases: [{ name: 'bounded shape', passed: true, detail: { maximumError: 0, checked: true } }],
  negativeControls: [{ name: 'incorrect candidate', detected: true, detail: { reason: 'mismatch' } }] });
const semantic = () => ({ version: 3, decision: 'approve_for_check', scopePreserved: true,
  canonicalPrecisionPreserved: true, evaluatorIndependent: true, upstreamAttributionPreserved: true,
  checkerReview: { casesMeaningful: true, negativeControlsDiscriminating: true, noTautologicalPass: true,
    rationale: 'Synthetic independent review of the original checker and mediated output.' },
  blockingFindings: [], findings: [], limitations: ['CPU emulation only'],
  gpuValidated: false, performanceGainProven: false, officialActionsPerformed: false });
const definition = (): any => ({ name: 'Native QA3 isolated fixture', project: 'fixture', trigger: { kind: 'manual' },
  allowedActions: ['native.cpu-replay.observe'], steps: [
    { key: 'produce', title: 'Frozen producer', kind: 'agent', workspace: 'fixture/source', maxAttempts: 1,
      criteria: [{ id: 'candidate', label: 'Candidate', type: 'artifact', name: 'candidate.py' }] },
    { key: 'independent-qa', title: 'Semantic independent QA', kind: 'agent', workspace: 'fixture/source', dependsOn: ['produce'], maxAttempts: 1,
      parameters: { nativeQAContractVersion: 3, cpuReplayObservationVersion: 1, cpuReplayProfile: 'python-cpu-candidate-base-v1' },
      dependencyEvidence: { version: 2, kind: 'controller-cpu-replay', sourceStep: 'produce', reportArtifact: 'qa-verification.json' },
      criteria: [{ id: 'semantic', label: 'Original semantic report', type: 'artifact', name: 'qa-verification.json' }] },
  ] });
const registration = () => ({ path: '/isolated/original-registration.json', digest: 'a'.repeat(64), value: {
  version: 1 as const, enabled: true as const, executorPath: '/isolated/original-cpu-replay.py', executorSHA256: 'b'.repeat(64),
  stateDirectory: '/isolated/replays', user: 'agentos-build', projects: ['fixture'],
  profiles: ['python-cpu-candidate-base-v1'] as ('python-cpu-candidate-base-v1')[],
  limits: { memoryMiB: 128, timeoutSeconds: 10, maxOutputBytes: 16384 },
} });
let engine: CycleEngine, qaRequests: any[], modes: string[], allReceipts: Map<string, any>, qaText: string,
  receiptMutation: (r: any) => void, verifyMutation: (r: any) => void, beforeQA: () => Promise<void>, originalToken: string,
  emitAlias: boolean, qaBytes: Buffer | undefined;
function completeReceipt(request: any) {
  const r = { version: 1, kind: 'cpu-replay-observation', requestId: request.requestId, occurrenceId: request.occurrenceId,
    runFence: request.runFence, project: request.project, source: request.source, profile: request.profile, baseSource: request.baseSource,
    requestSHA256: hash(cpuReplayCanonical(request)), registrationSHA256: request.registrationSHA256, executorSHA256: request.executorSHA256,
    observationOnly: true, acceptance: false, argv: cpuReplayArgv(request.profile), startedAt: '2026-01-01T00:00:00Z',
    finishedAt: '2026-01-01T00:00:01Z', exitCode: 0, signal: null, timedOut: false, oomKilled: false, cancelled: false,
    outputLimitExceeded: false, outputsComplete: true, memoryPeakBytes: 123456, rssPeakBytes: null,
    unit: `agentos-cpu-replay-${request.requestId}.service`,
    stopProof: { stopped: true, mainPID: 0, populated: false, unit: `agentos-cpu-replay-${request.requestId}.service` },
    outputs: { stdout: encoded('synthetic execution'), stderr: encoded(), result: encoded(JSON.stringify(cpuResult())) } };
  receiptMutation(r); return r;
}
beforeAll(async () => { await migrate(); });
beforeEach(async () => {
  await pool.query('TRUNCATE cycles,cycle_commands,accounts,goals,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
  engine = new CycleEngine(); vi.spyOn(engine, 'tick').mockResolvedValue();
  config.bridgeHost = 'fixture'; config.remoteExecution = true; originalToken = config.apiToken; config.apiToken = 'isolated-qa3-test-token';
  await pool.query("INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown,max_concurrency) VALUES('a','Isolated','muse','muse-spark-1.3','[\"muse-spark-1.3\"]',true,'a',true,1)");
  qaRequests = []; modes = []; allReceipts = new Map(); qaText = JSON.stringify(semantic());
  receiptMutation = () => {}; verifyMutation = () => {}; beforeQA = async () => {}; emitAlias = false; qaBytes = undefined;
  vi.spyOn(cpuReplayIO, 'registration').mockResolvedValue(registration());
  vi.spyOn(cpuReplayIO, 'verificationRegistration').mockImplementation(async binding => {
    expect(binding).toEqual({ path: registration().path, digest: registration().digest,
      executorPath: registration().value.executorPath, executorSHA256: registration().value.executorSHA256,
      profile: 'python-cpu-candidate-base-v1' });
    return registration();
  });
  vi.spyOn(cpuReplayIO, 'invoke').mockImplementation(async (mode, _registered, request) => {
    modes.push(mode);
    expect((await pool.query('SELECT input_hash FROM cycle_action_receipts WHERE action_key=$1',
      [`${request.occurrenceId}:${request.requestId}:cpu-replay-v1`])).rows[0]?.input_hash).toBe(hash(cpuReplayCanonical(request)));
    if (mode === 'execute') { expect(allReceipts.has(request.requestId)).toBe(false); allReceipts.set(request.requestId, completeReceipt(request)); }
    const receipt = structuredClone(allReceipts.get(request.requestId));
    if (!receipt) return { ok: false, code: 'CPU_REPLAY_UNCONFIRMED' };
    const response: any = { ok: true, receipt };
    if (mode === 'verify') {
      response.physicalInspection = { version: 1, observedAt: new Date().toISOString(),
        receiptSHA256: hash(cpuReplayCanonical(receipt)), stopProof: receipt.stopProof };
      verifyMutation(response);
    }
    return response;
  });
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string, request: any) => {
    if (method !== 'worker.execute') return method === 'memory.search' ? { items: [] } : { scores: {}, truncated: false };
    if (request.context.manifest.step === 'produce') {
      const external = JSON.parse(Buffer.from(request.inputs.find((x: any) => x.name === 'cycle-input.json').base64, 'base64').toString());
      return { summary: 'Synthetic frozen producer', evidence: { stopped: true }, artifacts: [
        { name: 'candidate.py', content: external.seed ? base : candidate },
        { name: 'implementation.patch', content: patch }, { name: 'cpu-checks.py', content: checker },
        { name: 'cpu-result.json', content: JSON.stringify({ historical: true, passed: true }) },
      ] };
    }
    qaRequests.push(request); await beforeQA();
    return { summary: 'Original semantic review', evidence: { stopped: true }, artifacts: [{ name: 'qa-verification.json',
      ...(qaBytes ? { base64: qaBytes.toString('base64') } : { content: qaText }) },
      ...(emitAlias ? [{ name: 'candidate.py', content: candidate }] : [])] };
  });
});
afterEach(async () => { await engine.close(); vi.restoreAllMocks(); config.bridgeHost = ''; config.remoteExecution = false; config.apiToken = originalToken; });
afterAll(async () => { await pool.end(); });
async function prepare() {
  const seedDef = definition(); seedDef.allowedActions = []; seedDef.steps = [seedDef.steps[0]];
  const seedCycle = await createCycle(seedDef, randomUUID()), seed = await runCycle(seedCycle.id, randomUUID(), { seed: true });
  await engine.execute(await engine.claim());
  const origin = await occurrenceDetail(seed.id), ref = origin.artifacts.find((f: any) => f.name === 'candidate.py');
  const sourceArtifact = { id: ref.id, name: ref.name, digest: ref.digest, size: ref.size, occurrenceId: seed.id, problem: 'fixture_problem' };
  const cycle = await createCycle(definition(), randomUUID()), o = await runCycle(cycle.id, randomUUID(), { experiment: {
    problem: 'fixture_problem', sourceArtifact,
    materials: [{ name: 'base-candidate.py', sha256: ref.digest, bytes: ref.size, base64: Buffer.from(base).toString('base64') }],
  } });
  await engine.execute(await engine.claim()); return { o, claim: await engine.claim(), origin };
}
async function execute() { const p = await prepare(); await engine.execute(p.claim); await engine.recover(); return { ...p, detail: await occurrenceDetail(p.o.id) }; }
const ownAttempt = (d: any, id: string) => d.attempts.find((a: any) => a.id === id);
async function exportFixture(name: string, p: any, evidence: any) {
  if (process.env.AGENTOS_NATIVE_QA3_PROOF !== '1') return;
  await writeFile(path.join(config.dataDir, name), JSON.stringify({ version: 1,
    detail: p.detail, origin: p.origin, evidence, qaRequests, modelExecuteCount: qaRequests.length,
    artifacts: await Promise.all([...p.detail.artifacts, ...p.origin.artifacts].map(async (f: any) => ({ ...f,
      base64: (await readCycleArtifact(f.id)).bytes.toString('base64') }))),
  }), { mode: 0o600 });
}

it('completes explicit QA3 with a mediated receipt and the original semantic report without copied hashes or cases', async () => {
  const p = await execute(), a = ownAttempt(p.detail, p.claim.a.id);
  expect(a.state).toBe('completed'); expect(a.context.nativeQA3Validation).toMatchObject({ status: 'matched', candidateExecutedByController: true });
  expect(qaRequests).toHaveLength(1); expect(modes).toEqual(['execute', 'inspect', 'verify', 'verify']);
  expect(qaRequests[0].prompt).not.toContain('Independently run validation');
  expect(qaRequests[0].prompt).toContain('Do not rerun tests merely to transcribe evidence');
  const report = p.detail.artifacts.find((f: any) => f.attempt_id === a.id && f.name === 'qa-verification.json');
  expect((await readCycleArtifact(report.id)).bytes.toString()).toBe(qaText);
  expect(qaText).not.toContain('cpuEvidence'); expect(qaText).not.toContain('candidateSHA256');
  const evidence = await nativeQA3Evidence(p.o.id, a.id);
  expect(evidence.state).toBe('evidence'); expect(evidence.receiptSHA256).toBe(a.context.nativeQA3Validation.receiptSHA256);
  expect(evidence.request.artifacts.every((x: any) => !('base64' in x))).toBe(true);
  expect(evidence.semanticReport?.digest).toBe(hash(qaText)); expect(evidence.validation.status).toBe('matched');
  if (process.env.AGENTOS_NATIVE_QA3_PROOF === '1') await writeFile(path.join(config.dataDir, 'native-qa3-fixture.json'), JSON.stringify({
    version: 1, detail: p.detail, origin: p.origin, evidence, qaRequests,
    artifacts: await Promise.all([...p.detail.artifacts, ...p.origin.artifacts].map(async (f: any) => ({ ...f,
      base64: (await readCycleArtifact(f.id)).bytes.toString('base64') }))),
  }), { mode: 0o600 });
});
it('retains a rejected original report and bound diagnostic without retrying producer or QA', async () => {
  qaText = JSON.stringify({ ...semantic(), decision: 'reject', blockingFindings: ['Tautological checker'],
    checkerReview: { ...semantic().checkerReview, noTautologicalPass: false } });
  const p = await execute(), a = ownAttempt(p.detail, p.claim.a.id);
  expect(a).toMatchObject({ state: 'failed', error_code: 'NATIVE_QA3_SEMANTIC_REJECT', receipt: { stopped: true } });
  expect(a.context.nativeQA3Validation.status).toBe('rejected'); expect(await engine.claim()).toBeNull();
  const proof = await nativeQA3Evidence(p.o.id, a.id); expect(proof.validation.semanticDecision).toBe('reject');
  expect((await readCycleArtifact(proof.semanticReport!.id)).bytes.toString()).toBe(qaText);
  await exportFixture('native-qa3-semantic-reject-fixture.json', p, proof);
});
it.each([
  ['missing case', (r: any) => { const cpu = cpuResult(); cpu.cases = []; r.outputs.result = encoded(JSON.stringify(cpu)); }],
  ['wrong candidate', (r: any) => { r.outputs.result = encoded(JSON.stringify({ ...cpuResult(), candidateSHA256: 'f'.repeat(64) })); }],
  ['wrong checker', (r: any) => { r.outputs.result = encoded(JSON.stringify({ ...cpuResult(), checkerSHA256: 'f'.repeat(64) })); }],
  ['duplicate case', (r: any) => { const cpu = cpuResult(); cpu.cases.push(cpu.cases[0]); r.outputs.result = encoded(JSON.stringify(cpu)); }],
  ['missing control', (r: any) => { r.outputs.result = encoded(JSON.stringify({ ...cpuResult(), negativeControls: [] })); }],
  ['false control', (r: any) => { r.outputs.result = encoded(JSON.stringify({ ...cpuResult(), negativeControls: [{ name: 'negative', detected: false }] })); }],
  ['NaN', (r: any) => { r.outputs.result = encoded('{"passed":true,"value":NaN}'); }],
  ['duplicate JSON key', (r: any) => { r.outputs.result = encoded('{"passed":false,"passed":true}'); }],
  ['overflow', (r: any) => { r.outputs.result = encoded('{"passed":true,"value":1e999}'); }],
  ['invalid UTF8', (r: any) => { r.outputs.result = encoded(Buffer.from([0xff, 0xfe])); }],
  ['malformed JSON', (r: any) => { r.outputs.result = encoded('{'); }],
])('rejects mediated CPU %s despite semantic approval', async (_name, mutate) => {
  receiptMutation = mutate; const p = await execute(), a = ownAttempt(p.detail, p.claim.a.id);
  expect(a).toMatchObject({ state: 'failed', error_code: 'NATIVE_QA3_CPU_RESULT_INVALID' });
  expect(a.context.nativeQA3PreparationFailure).toMatchObject({ status: 'rejected', code: 'NATIVE_QA3_CPU_RESULT_INVALID' });
  expect(a.receipt).toMatchObject({ kind: 'cpu-preparation-terminal', modelDispatched: false });
  expect(a.receipt.stopped).toBeUndefined();
  expect(qaRequests).toHaveLength(0); expect(await engine.claim()).toBeNull();
});
it.each(['oomKilled', 'timedOut', 'cancelled', 'outputLimitExceeded'])('keeps %s as a technical failure with semantic diagnostic preserved', async flag => {
  receiptMutation = r => { r[flag] = true; };
  const p = await execute(); expect(ownAttempt(p.detail, p.claim.a.id).error_code).toBe('NATIVE_QA3_CPU_EXECUTION_FAILED');
  expect(qaRequests).toHaveLength(0);
  const proof = await nativeQA3Evidence(p.o.id, p.claim.a.id);
  expect(proof.modelDispatched).toBe(false); expect(proof.handoff).toBeNull(); expect(proof.semanticReport).toBeNull();
  expect(proof.preparationFailure.code).toBe('NATIVE_QA3_CPU_EXECUTION_FAILED');
  expect(p.detail.occurrence.state).toBe('attention');
  expect(p.detail.steps.find((s: any) => s.key === 'independent-qa').state).toBe('attention');
  if (flag === 'oomKilled') await exportFixture('native-qa3-preparation-failure-fixture.json', p, proof);
});
it.each([
  ['nonzero', (r: any) => { r.exitCode = 1; }], ['signal', (r: any) => { r.exitCode = null; r.signal = 9; }],
  ['missing result', (r: any) => { r.outputs.result = null; }], ['incomplete output', (r: any) => { r.outputsComplete = false; }],
])('does not accept %s as a CPU success', async (_name, change) => {
  receiptMutation = change; const p = await execute(); expect(ownAttempt(p.detail, p.claim.a.id).error_code).toBe('NATIVE_QA3_CPU_EXECUTION_FAILED');
});
it.each([
  ['claim GPU', () => JSON.stringify({ ...semantic(), gpuValidated: true })],
  ['copy hashes', () => JSON.stringify({ ...semantic(), cpuEvidence: { resultSHA256: 'a'.repeat(64) } })],
  ['duplicate semantic key', () => JSON.stringify(semantic()).replace('"version":3', '"version":2,"version":3')],
  ['nonfinite semantic', () => JSON.stringify(semantic()).replace('"version":3', '"version":3,"x":1e999')],
  ['NaN semantic', () => JSON.stringify(semantic()).replace('"version":3', '"version":3,"x":NaN')],
  ['malformed semantic JSON', () => '{'],
])('preserves and rejects invalid original semantic report: %s', async (_name, raw) => {
  qaText = raw(); const p = await execute();
  expect(ownAttempt(p.detail, p.claim.a.id)).toMatchObject({ state: 'failed', error_code: 'NATIVE_QA3_REPORT_INVALID',
    context: { nativeQA3Validation: { status: 'rejected', code: 'NATIVE_QA3_REPORT_INVALID', semanticDecision: null } } });
  const artifact = p.detail.artifacts.find((f: any) => f.attempt_id === p.claim.a.id);
  expect((await readCycleArtifact(artifact.id)).bytes.toString()).toBe(qaText);
  if (_name === 'duplicate semantic key') await exportFixture('native-qa3-malformed-report-fixture.json', p,
    await nativeQA3Evidence(p.o.id, p.claim.a.id));
});
it('types invalid UTF8 semantic JSON without rewriting the original bytes', async () => {
  qaBytes = Buffer.from([0xff, 0xfe]); const p = await execute(), a = ownAttempt(p.detail, p.claim.a.id);
  expect(a.error_code).toBe('NATIVE_QA3_REPORT_INVALID'); expect(a.context.nativeQA3Validation.code).toBe('NATIVE_QA3_REPORT_INVALID');
  const ref = p.detail.artifacts.find((f: any) => f.attempt_id === a.id && f.name === 'qa-verification.json');
  expect((await readCycleArtifact(ref.id)).bytes).toEqual(qaBytes);
});
it('GET authenticates and invokes only readonly verify without changing persisted evidence', async () => {
  const p = await execute(), before = (await pool.query('SELECT * FROM cycle_action_receipts')).rows;
  const app = makeApp(), url = `/api/cycle-runs/${p.o.id}/attempts/${p.claim.a.id}/cpu-replay`;
  try {
    const count = modes.length;
    expect((await app.inject({ method: 'GET', url, headers: { host: 'localhost' } })).statusCode).toBe(401);
    expect(modes).toHaveLength(count);
    const response = await app.inject({ method: 'GET', url, headers: { authorization: `Bearer ${config.apiToken}` } });
    expect(response.statusCode).toBe(200); expect(response.json().state).toBe('evidence');
    expect(modes.slice(count)).toEqual(['verify']);
    expect((await pool.query('SELECT * FROM cycle_action_receipts')).rows).toEqual(before);
  } finally { await app.close(); }
});
it.each([
  ['missing protected receipt', (out: any) => { out.ok = false; }],
  ['altered protected receipt', (out: any) => { out.receipt.memoryPeakBytes++; }],
  ['stale physical stop', (out: any) => { out.physicalInspection.observedAt = '2020-01-01T00:00:00Z'; }],
  ['wrong physical unit', (out: any) => { out.physicalInspection.stopProof = { ...out.physicalInspection.stopProof, unit: 'other.service' }; }],
])('GET fails closed on %s and never executes a replacement', async (_name, mutate) => {
  const p = await execute(); verifyMutation = mutate; const before = modes.filter(x => x === 'execute').length;
  await expect(nativeQA3Evidence(p.o.id, p.claim.a.id)).rejects.toThrow();
  expect(modes.filter(x => x === 'execute')).toHaveLength(before);
});
it('rejects cross occurrence, changed action binding and fabricated model receipt', async () => {
  const p = await execute(); await expect(nativeQA3Evidence(p.origin.occurrence.id, p.claim.a.id)).rejects.toThrow();
  await pool.query("UPDATE cycle_action_receipts SET input_hash=$2 WHERE occurrence_id=$1", [p.o.id, 'f'.repeat(64)]);
  await expect(nativeQA3Evidence(p.o.id, p.claim.a.id)).rejects.toThrow();
});
it('rejects changed handoff bytes after semantic QA without rewriting them', async () => {
  beforeQA = async () => { await pool.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{manifest,nativeQA3,receipt,digest}',to_jsonb($1::text)) WHERE context#>>'{manifest,nativeQA3,contractVersion}'='3'", ['f'.repeat(64)]); };
  const p = await execute(); expect(ownAttempt(p.detail, p.claim.a.id).error_code).toBe('NATIVE_QA3_HANDOFF_CHANGED');
});
it('uses the original persisted registration for verification after the current registration changes', async () => {
  const p = await execute(); vi.mocked(cpuReplayIO.registration).mockResolvedValue({ ...registration(), digest: 'c'.repeat(64) });
  const proof = await nativeQA3Evidence(p.o.id, p.claim.a.id);
  expect(proof.registration.digest).toBe('a'.repeat(64)); expect(proof.request.registrationSHA256).toBe('a'.repeat(64));
});
async function aliasBaseRequest(parent: any) {
  const alias = parent.detail.artifacts.find((f: any) => f.attempt_id === parent.claim.a.id && f.name === 'candidate.py');
  const cycle = await createCycle(definition(), randomUUID());
  const o = await runCycle(cycle.id, randomUUID(), { experiment: { problem: 'fixture_problem',
    sourceArtifact: { id: alias.id, name: alias.name, digest: alias.digest, size: alias.size,
      occurrenceId: parent.o.id, problem: 'fixture_problem' },
    materials: [{ name: 'base-candidate.py', sha256: alias.digest, bytes: alias.size, base64: Buffer.from(candidate).toString('base64') }],
  } });
  await engine.execute(await engine.claim());
  const claim = await engine.claim(), snapshot = await freezeDependencyEvidence(claim.o, claim.s, claim.a);
  return { alias, claim, snapshot, build: () => buildCPUReplayRequest(claim.o, claim.s, claim.a, snapshot, registration()) };
}
it('binds a completed QA alias to the explicit stopped producer without replacing the actual base reference or actor', async () => {
  emitAlias = true; const parent = await execute(), before = modes.filter(x => x === 'execute').length;
  const child = await aliasBaseRequest(parent), built = await child.build();
  expect(built.request.baseSource).toMatchObject({ artifact: { id: child.alias.id, digest: hash(candidate) },
    source: { occurrenceId: parent.o.id, attemptId: parent.claim.a.id, runFence: parent.claim.a.fence } });
  expect(modes.filter(x => x === 'execute')).toHaveLength(before);
});
it.each([
  ['qa inflight', 'CPU_REPLAY_BASE_BINDING'], ['producer inflight', 'DEPENDENCY_SOURCE_CHANGED'],
  ['different project', 'CPU_REPLAY_BASE_BINDING'], ['changed fence', 'DEPENDENCY_SOURCE_CHANGED'],
  ['missing snapshot', 'CPU_REPLAY_BASE_ALIAS_UNPROVEN'], ['changed producer artifact', 'DEPENDENCY_SOURCE_CHANGED'],
  ['changed alias reference', 'CPU_REPLAY_BASE_BINDING'],
])('rejects an unproven QA base alias: %s', async (kind, code) => {
  emitAlias = true; const parent = await execute();
  const child = await aliasBaseRequest(parent);
  const producer = parent.detail.steps.find((s: any) => s.key === 'produce');
  if (kind === 'qa inflight') await pool.query("UPDATE cycle_attempts SET state='running' WHERE id=$1", [parent.claim.a.id]);
  if (kind === 'producer inflight') await pool.query("UPDATE cycle_attempts SET state='running' WHERE step_id=$1", [producer.id]);
  if (kind === 'different project') await pool.query("UPDATE cycle_occurrences SET definition=jsonb_set(definition,'{project}','\"other\"'::jsonb) WHERE id=$1", [parent.o.id]);
  if (kind === 'changed fence') await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE step_id=$1', [producer.id, randomUUID()]);
  if (kind === 'missing snapshot') await pool.query("UPDATE cycle_attempts SET context=context #- '{manifest,dependencyEvidence}' WHERE id=$1", [parent.claim.a.id]);
  if (kind === 'changed producer artifact') await pool.query("UPDATE cycle_artifacts SET digest=$2 WHERE attempt_id=$1 AND name='candidate.py'", [parent.detail.attempts.find((a: any) => a.step_id === producer.id).id, 'e'.repeat(64)]);
  if (kind === 'changed alias reference') child.claim.o.input.experiment.sourceArtifact.id = parent.origin.artifacts.find((f: any) => f.name === 'candidate.py').id;
  await expect(child.build()).rejects.toMatchObject({ code });
});
it.each([
  ['implicit v2', (d: any) => { d.steps[1].parameters.nativeQAContractVersion = 2; }],
  ['old bindings', (d: any) => { d.steps[1].dependencyEvidence = { version: 1, sourceStep: 'produce', bindings: [] }; }],
  ['QA retries', (d: any) => { d.steps[1].maxAttempts = 2; }],
  ['producer retries', (d: any) => { d.steps[0].maxAttempts = 2; }],
  ['extra action', (d: any) => { d.allowedActions.push('anything.else'); }],
  ['missing executor optin', (d: any) => { delete d.steps[1].parameters.cpuReplayObservationVersion; }],
  ['other profile', (d: any) => { d.steps[1].parameters.cpuReplayProfile = 'python-cpu-argv-v1'; }],
])('requires explicit NativeQA3 authority: %s', (_name, change) => {
  const d = definition(); change(d); expect(() => parseCycle(d)).toThrow();
});

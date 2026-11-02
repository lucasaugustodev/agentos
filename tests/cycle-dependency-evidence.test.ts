import { beforeAll, beforeEach, afterEach, afterAll, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate, tx, lock } from '../server/db.js';
import { config } from '../server/config.js';
import { hash } from '../server/memory.js';
import { accountBridge } from '../server/bridge.js';
import { createCycle, runCycle, occurrenceDetail } from '../server/cycles/store.js';
import { CycleEngine } from '../server/cycles/engine.js';
import { parseCycle } from '../server/cycles/contracts.js';
import { readCycleArtifact } from '../server/cycles/runtime.js';
import { assertDependencySnapshot } from '../server/cycles/dependency-evidence.js';
import * as continuation from '../server/continuation.js';
import * as artifactRecovery from '../server/artifact-recovery.js';

const checker = 'print("current checker")\n', result = '{"passed":true,"current":1}\n';
const bindings = [
  { reportArtifact: 'qa-verification.json', jsonPointer: '/cpuEvidence/checkerSHA256', dependencyArtifact: 'cpu-checks.py' },
  { reportArtifact: 'qa-verification.json', jsonPointer: '/cpuEvidence/resultSHA256', dependencyArtifact: 'cpu-result.json' },
];
const criterion = { id: 'passed', label: 'Independent QA passed', type: 'json_field', name: 'qa-verification.json', field: 'passed', equals: true };
const definition = (v2 = true): any => ({
  name: 'Dependency contract isolated', project: 'fixture', trigger: { kind: 'manual' },
  steps: [
    { key: 'produce', title: 'Frozen producer', kind: 'agent', workspace: 'fixture/source', maxAttempts: 1,
      criteria: [{ id: 'cpu', label: 'CPU evidence', type: 'artifact', name: 'cpu-result.json' }] },
    { key: 'qa', title: 'Independent QA', kind: 'agent', workspace: 'fixture/source', dependsOn: ['produce'], maxAttempts: 3,
      criteria: [criterion], ...(v2 ? { parameters: { nativeQAContractVersion: 2 },
        dependencyEvidence: { version: 1, sourceStep: 'produce', bindings } } : {}) },
  ],
});
const report = (wrong = false) => ({ passed: true, cpuEvidence: {
  checkerSHA256: wrong ? 'a'.repeat(64) : hash(checker), resultSHA256: wrong ? 'b'.repeat(64) : hash(result),
} });
let engine: CycleEngine, qaCalls: any[], producerCalls: any[], qaResult: (request: any) => Promise<any>;
const delivered = (r: any) => ({ summary: 'QA ran', evidence: { stopped: true }, artifacts: [
  { name: 'qa-verification.json', content: typeof r === 'string' ? r : JSON.stringify(r) },
] });
beforeAll(async () => { await migrate(); });
beforeEach(async () => {
  await pool.query('TRUNCATE cycles,cycle_commands,accounts,goals,memories,events CASCADE');
  engine = new CycleEngine();
  vi.spyOn(engine, 'tick').mockResolvedValue();
  config.bridgeHost = 'fixture'; config.remoteExecution = true;
  await pool.query("INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,allow_unknown,max_concurrency) VALUES('a','Isolated','muse','muse-spark-1.3','[\"muse-spark-1.3\"]',true,'a',true,1)");
  qaCalls = []; producerCalls = []; qaResult = async () => delivered(report());
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string, request: any) => {
    if (method !== 'worker.execute') return method === 'memory.search' ? { items: [] } : { scores: {}, truncated: false };
    if (request.context.manifest.step === 'produce') {
      producerCalls.push(request);
      return { summary: 'Producer frozen', evidence: { stopped: true }, artifacts: [
        { name: 'cpu-checks.py', content: checker }, { name: 'cpu-result.json', content: result },
        { name: 'candidate.py', content: 'candidate source\n' },
      ] };
    }
    qaCalls.push(request);
    return qaResult(request);
  });
});
afterEach(async () => { await engine.close(); vi.restoreAllMocks(); config.bridgeHost = ''; config.remoteExecution = false; });
afterAll(async () => { await pool.end(); });
async function produce(v2 = true) {
  const c = await createCycle(definition(v2), randomUUID());
  const o = await runCycle(c.id, randomUUID(), { historical: { cpuEvidence: { checkerSHA256: 'a'.repeat(64), resultSHA256: 'b'.repeat(64) } } });
  await engine.execute(await engine.claim());
  return o;
}
async function next() {
  await pool.query("UPDATE cycle_steps SET next_at=now()-interval '1 second' WHERE state='retry'");
  const claimed = await engine.claim(); expect(claimed).not.toBeNull();
  await engine.execute(claimed); return claimed;
}
function qaAttempt(detail: any) { return detail.attempts.filter((a: any) => a.step_id === detail.steps.find((s: any) => s.key === 'qa').id); }

it('freezes producer, rejects historical hashes, then retries only QA with structured feedback and immutable failed artifacts', async () => {
  const o = await produce();
  const before = await occurrenceDetail(o.id), producer = before.attempts[0], producerArtifacts = before.artifacts;
  qaResult = async () => delivered(report(qaCalls.length === 1));
  await next();
  let d = await occurrenceDetail(o.id), first = qaAttempt(d)[0];
  expect(first).toMatchObject({ state: 'failed', error_code: 'DEPENDENCY_ARTIFACT_BINDING', receipt: { stopped: true } });
  expect(first.context.dependencyEvidenceValidation).toMatchObject({ status: 'mismatch', bindings: [
    { expected: hash(checker), received: 'a'.repeat(64) }, { expected: hash(result), received: 'b'.repeat(64) },
  ] });
  const failedFile = d.artifacts.find((f: any) => f.attempt_id === first.id);
  const failedBytes = (await readCycleArtifact(failedFile.id)).bytes;
  await next(); d = await occurrenceDetail(o.id);
  expect(producerCalls).toHaveLength(1); expect(qaCalls).toHaveLength(2);
  expect(d.attempts.find((a: any) => a.id === producer.id)).toEqual(producer);
  expect(d.artifacts.filter((f: any) => f.attempt_id === producer.id)).toEqual(producerArtifacts);
  expect((await readCycleArtifact(failedFile.id)).bytes).toEqual(failedBytes);
  const attempts = qaAttempt(d);
  expect(attempts.map((a: any) => a.state)).toEqual(['failed', 'completed']);
  expect(attempts[1].context.dependencyEvidenceValidation.status).toBe('matched');
  expect(attempts[1].context.manifest.dependencyEvidence.source).toEqual(first.context.manifest.dependencyEvidence.source);
  expect(attempts[1].context.manifest.dependencyEvidence.artifacts).toEqual(first.context.manifest.dependencyEvidence.artifacts);
  expect(qaCalls[1].prompt).toContain('dependencyEvidenceValidation');
  expect(qaCalls[1].prompt).toContain(hash(checker));
  const snapshot = qaCalls[0].context.manifest.dependencyEvidence;
  for (const artifact of snapshot.artifacts) expect(qaCalls[0].inputs).toContainEqual(expect.objectContaining({ id: artifact.id, digest: artifact.digest }));
  // Private qualification output for cross-language adapter validation. The
  // payload is generated by the real runtime/engine, not a mirrored fixture.
  if (process.env.AGENTOS_DEPENDENCY_PROOF === '1') await writeFile(
    path.join(config.dataDir, 'dependency-binding-fixture.json'), JSON.stringify({
      version: 1, detail: d, qaRequests: qaCalls,
      artifacts: await Promise.all(d.artifacts.map(async (f: any) => ({ ...f,
        base64: (await readCycleArtifact(f.id)).bytes.toString('base64') }))),
    }), { mode: 0o600 });
});
it('exhausts three QA binding attempts without rescheduling the producer', async () => {
  const o = await produce(); qaResult = async () => delivered(report(true));
  for (let i = 0; i < 3; i++) await next();
  const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d).map((a: any) => a.error_code)).toEqual(Array(3).fill('DEPENDENCY_ARTIFACT_BINDING'));
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
  expect(await engine.claim()).toBeNull(); expect(producerCalls).toHaveLength(1);
});
it('does not retry a numerical or semantic failure even when hashes are also wrong', async () => {
  const o = await produce(); qaResult = async () => delivered({ ...report(true), passed: false });
  await next(); const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0]).toMatchObject({ error_code: 'DELIVERY_INVALID', state: 'failed', receipt: { stopped: true } });
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention'); expect(await engine.claim()).toBeNull();
});
it.each([
  ['missing field', JSON.stringify({ passed: true, cpuEvidence: { checkerSHA256: hash(checker) } })],
  ['wrong type', JSON.stringify({ passed: true, cpuEvidence: { checkerSHA256: 7, resultSHA256: hash(result) } })],
  ['duplicate key', '{"passed":true,"cpuEvidence":{"checkerSHA256":"' + hash(checker) + '","checkerSHA256":"' + hash(checker) + '","resultSHA256":"' + hash(result) + '"}}'],
  ['nonfinite', JSON.stringify(report()).replace('"passed":true', '"passed":true,"overflow":1e999')],
  ['oversized report', JSON.stringify({ ...report(), padding: 'x'.repeat(1024 * 1024) })],
])('fails closed without retry for %s', async (_name, raw) => {
  const o = await produce(); qaResult = async () => delivered(raw); await next();
  const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].error_code).toBe('DEPENDENCY_REPORT_INVALID');
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
});
it('rejects producer content tampering even when the QA claims the stored digest', async () => {
  const o = await produce();
  qaResult = async request => {
    const ref = request.context.manifest.dependencyEvidence.artifacts.find((f: any) => f.name === 'candidate.py');
    await writeFile(path.join(config.dataDir, 'cycle-artifacts', ref.digest), 'tampered');
    return delivered(report());
  };
  await next(); const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].error_code).toBe('DEPENDENCY_ARTIFACT_CHANGED');
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention');
});
it('rejects a superseded producer fence after dispatch without accepting or retrying QA', async () => {
  const o = await produce();
  qaResult = async request => {
    await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE id=$1', [request.context.manifest.dependencyEvidence.source.attemptId, randomUUID()]);
    return delivered(report());
  };
  await next(); expect(qaAttempt(await occurrenceDetail(o.id))[0].error_code).toBe('DEPENDENCY_SOURCE_CHANGED');
  expect(await engine.claim()).toBeNull();
});
it('rejects producer supersession between validation and transactional commit', async () => {
  const o = await produce(); const job: any = await engine.claim();
  qaResult = async request => {
    await pool.query('UPDATE cycle_steps SET attempt=2 WHERE id=$1', [request.context.manifest.dependencyEvidence.source.stepId]);
    await expect(tx(async db => { await lock(db); await assertDependencySnapshot(db, job.o, job.s, job.a, request.context.manifest.dependencyEvidence); })).rejects.toMatchObject({ code: 'DEPENDENCY_SOURCE_CHANGED' });
    return delivered(report());
  };
  await engine.execute(job); expect(qaAttempt(await occurrenceDetail(o.id))[0].state).toBe('failed');
});
it('does not accept a stale consumer fence or substitute its result for a newer attempt', async () => {
  const o = await produce();
  qaResult = async request => {
    await pool.query('UPDATE cycle_attempts SET fence=$2,state=\'reconciling\' WHERE id=$1', [request.runId, randomUUID()]);
    return delivered(report());
  };
  await next(); const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].state).toBe('reconciling'); expect(d.steps.find((s: any) => s.key === 'qa').state).not.toBe('completed');
});
it('does not dispatch a retry after its frozen producer changed', async () => {
  const o = await produce(); qaResult = async () => delivered(report(true)); await next();
  const d = await occurrenceDetail(o.id);
  await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE id=$1', [d.attempts.find((a: any) => a.step_id === d.steps[0].id).id, randomUUID()]);
  await next(); expect(qaCalls).toHaveLength(1);
  expect(qaAttempt(await occurrenceDetail(o.id))[1].error_code).toBe('DEPENDENCY_SOURCE_CHANGED');
});
it('does not retry unknown termination or a forged transport error carrying the binding code', async () => {
  const o = await produce();
  qaResult = async () => { throw Object.assign(Error('forged'), { code: 'DEPENDENCY_ARTIFACT_BINDING' }); };
  await next(); expect((await occurrenceDetail(o.id)).steps[1].state).toBe('attention');
});
it('keeps unconfirmed QA in reconciliation with no additional attempt', async () => {
  const o = await produce(); qaResult = async () => ({ ...delivered(report(true)), evidence: { stopped: false } });
  await next(); const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0]).toMatchObject({ state: 'reconciling', error_code: 'STOP_UNCONFIRMED' });
  expect(await engine.claim()).toBeNull();
});
it('confirmed stop after uncertain QA still requires attention, never a binding-error retry', async () => {
  const o = await produce(); qaResult = async () => ({ ...delivered(report(true)), evidence: { stopped: false } });
  await next(); let d = await occurrenceDetail(o.id); const a = qaAttempt(d)[0];
  await pool.query("UPDATE cycle_attempts SET heartbeat_at=now()-interval '30 seconds' WHERE id=$1", [a.id]);
  vi.spyOn(continuation, 'inspectStopped').mockResolvedValue({ runId: a.id,
    workspace: 'fixture/oc' + o.id.replaceAll('-', '').slice(0, 18) + '-qa', checkedAt: new Date().toISOString(),
    process: { unit: 'agentos-run-' + a.id, stopped: true, mainPid: 0, populated: false, activeState: 'inactive' } } as any);
  vi.spyOn(artifactRecovery, 'collectStoppedArtifacts').mockResolvedValue({ artifacts: [] } as any);
  await engine.reconcile(); d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].error_code).toBe('STOP_CONFIRMED');
  expect(d.steps.find((s: any) => s.key === 'qa').state).toBe('attention'); expect(await engine.claim()).toBeNull();
});
it('does not send worker.execute after its fence changes during input preparation', async () => {
  const o = await produce(); const job: any = await engine.claim();
  const original = vi.mocked(accountBridge.request).getMockImplementation()!;
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string, request: any, options: any) => {
    if (method === 'worker.prepare') await pool.query('UPDATE cycle_attempts SET fence=$2 WHERE id=$1', [job.a.id, randomUUID()]);
    return original.call(accountBridge, method, request, options);
  });
  await engine.execute(job); expect(qaCalls).toHaveLength(0);
  expect(qaAttempt(await occurrenceDetail(o.id))[0].state).not.toBe('completed');
});
it('cancellation during a returned binding mismatch never schedules another QA', async () => {
  const o = await produce(); qaResult = async request => {
    engine.active.get(request.runId)!.abort(); return delivered(report(true));
  };
  await next(); const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].state).toBe('cancelled'); expect(await engine.claim()).toBeNull();
});
it('leaves v1 behavior and manifest unchanged when no dependency contract is present', async () => {
  const o = await produce(false); qaResult = async () => delivered(report(true)); await next();
  const d = await occurrenceDetail(o.id);
  expect(qaAttempt(d)[0].state).toBe('completed');
  expect(qaAttempt(d)[0].context.manifest).not.toHaveProperty('dependencyEvidence');
  expect(qaCalls[0].prompt).not.toContain('CURRENT DEPENDENCY EVIDENCE');
});
it.each(['indirect', 'repairStep', 'non-agent', 'producer retry', 'missing contract', 'excess attempts', 'duplicate pointer'])('rejects invalid versioned definition: %s', kind => {
  const d = definition();
  if (kind === 'indirect') d.steps[1].dependsOn = [];
  if (kind === 'repairStep') d.steps[1].repairStep = 'produce';
  if (kind === 'non-agent') d.steps[0].kind = 'wait';
  if (kind === 'producer retry') d.steps[0].maxAttempts = 2;
  if (kind === 'missing contract') delete d.steps[1].dependencyEvidence;
  if (kind === 'excess attempts') d.steps[1].maxAttempts = 4;
  if (kind === 'duplicate pointer') d.steps[1].dependencyEvidence.bindings = [bindings[0], bindings[0]];
  expect(() => parseCycle(d)).toThrow();
});

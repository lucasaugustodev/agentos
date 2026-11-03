import { isDeepStrictEqual } from 'node:util';
import { z } from 'zod';
import { pool, tx, lock } from '../db.js';
import { hash } from '../memory.js';
import { DomainError } from '../contracts.js';
import { parseCycle } from './contracts.js';
import { assertDependencySnapshot, assertFrozenDependencySource, dependencyArtifactBytes,
  nativeQA3, reportJSON, type DependencySnapshot } from './dependency-evidence.js';
import { buildCPUReplayRequest, cpuReplayCanonical, cpuReplayIO, validateCPUReplayReceipt, cpuPreparationOwned } from './cpu-replay.js';

export const nativeQA3Canonicalization = 'cpu-replay-ascii-integers-v1';
const text = z.string().trim().min(1).max(8000);
export const nativeQA3ReportSchema = z.object({
  version: z.literal(3), decision: z.enum(['approve_for_check', 'reject']),
  scopePreserved: z.boolean(), canonicalPrecisionPreserved: z.boolean(), evaluatorIndependent: z.boolean(),
  upstreamAttributionPreserved: z.boolean(),
  checkerReview: z.object({ casesMeaningful: z.boolean(), negativeControlsDiscriminating: z.boolean(),
    noTautologicalPass: z.boolean(), rationale: text }).strict(),
  blockingFindings: z.array(text).max(100), findings: z.array(text).max(100), limitations: z.array(text).min(1).max(100),
  gpuValidated: z.literal(false), performanceGainProven: z.literal(false), officialActionsPerformed: z.literal(false),
}).strict();
const fail = (code: string): never => { throw new DomainError(code, 'NativeQA v3 evidence is unavailable, rejected or changed; no CHECK acceptance or automatic replay.'); };
const canonicalSHA = (value: any) => hash(cpuReplayCanonical(value));
const artifactRef = (r: any) => r ? { id: r.id, name: r.name, digest: r.digest, size: r.size } : null;
export class NativeQA3PreparationError extends DomainError {
  constructor(code: string, public terminal: any) {
    super(code, 'The controlled CPU execution failed its technical gate. No QA model was dispatched; the original receipt remains available to supervision.');
  }
}

export function nativeQA3Handoff(receipt: any, reference: any, registration: any) {
  return { version: 1, contractVersion: 3, requestId: receipt.requestId,
    requestSHA256: receipt.requestSHA256, receiptSHA256: canonicalSHA(receipt),
    receiptCanonicalization: nativeQA3Canonicalization, receipt: reference, registration };
}

async function capture(occurrenceId: string, attemptId: string) {
  return tx(async db => {
    await lock(db);
    const o = (await db.query('SELECT * FROM cycle_occurrences WHERE id=$1', [occurrenceId])).rows[0];
    const a = (await db.query('SELECT * FROM cycle_attempts WHERE id=$1', [attemptId])).rows[0];
    const s = a && (await db.query('SELECT * FROM cycle_steps WHERE id=$1 AND occurrence_id=$2', [a.step_id, occurrenceId])).rows[0];
    if (!o || !s || !a || !nativeQA3(s.definition)) return fail('NATIVE_QA3_SCOPE');
    const parsed = parseCycle(o.definition);
    if (!isDeepStrictEqual(parsed.steps.find(p => p.key === s.key), s.definition) ||
        s.definition.dependencyEvidence?.version !== 2 || a.number !== 1 || s.attempt !== 1)
      return fail('NATIVE_QA3_SCOPE');
    const preparation = a.context?.cpuReplayPreparation;
    const snapshot: DependencySnapshot = a.context?.manifest?.dependencyEvidence ?? preparation?.snapshot;
    await assertFrozenDependencySource(db, o, s, a, snapshot);
    if (!preparation?.registration || preparation.snapshot?.contractSHA256 !== snapshot.contractSHA256)
      return fail('NATIVE_QA3_REGISTRATION_MISSING');
    const actionKey = `${o.id}:${a.id}:cpu-replay-v1`;
    const action = (await db.query('SELECT action_key,occurrence_id,action,state,input_hash,receipt FROM cycle_action_receipts WHERE action_key=$1', [actionKey])).rows[0];
    if (!action || action.occurrence_id !== o.id || action.action !== 'native.cpu-replay.observe') return fail('NATIVE_QA3_ACTION_MISSING');
    const qaArtifact = artifactRef((await db.query('SELECT id,name,digest,size FROM cycle_artifacts WHERE attempt_id=$1 AND name=$2',
      [a.id, s.definition.dependencyEvidence.reportArtifact])).rows[0]);
    // Exclude heartbeat/usage counters from the authority CAS, not the frozen
    // manifest, human state, step state, report, model stop or action evidence.
    const identity = { o: { id: o.id, version: o.version, definition: o.definition, input: o.input, state: o.state },
      s: { id: s.id, key: s.key, definition: s.definition, state: s.state, attempt: s.attempt },
      a: { id: a.id, number: a.number, fence: a.fence, state: a.state, context: a.context, receipt: a.receipt }, action, qaArtifact };
    return { o, s, a, snapshot, preparation, action, qaArtifact, identity };
  });
}

/** Authenticated route helper: GET does not start/recover/cancel a tool or mutate
 * receipts. verify is a separate, read-only launcher mode, not inspect. */
export async function nativeQA3Evidence(occurrenceId: string, attemptId: string) {
  const captured = await capture(occurrenceId, attemptId);
  const { o, s, a, snapshot, preparation, action, qaArtifact } = captured;
  const registration = await cpuReplayIO.verificationRegistration(preparation.registration);
  const { request } = await buildCPUReplayRequest(o, s, a, snapshot, registration);
  const requestSHA256 = canonicalSHA(request);
  if (action.input_hash !== requestSHA256 || preparation.requestSHA256 !== requestSHA256)
    return fail('NATIVE_QA3_REQUEST_CHANGED');
  for (const ref of snapshot.artifacts) await dependencyArtifactBytes(ref);
  const { artifacts, ...metadata } = request;
  const common = { version: 1, kind: 'native-qa3-controller-evidence', occurrenceId, attemptId, runFence: a.fence,
    definitionVersion: o.version, request: { ...metadata, artifacts: artifacts.map(({ base64: _bytes, ...ref }) => ref) },
    requestSHA256, requestCanonicalization: nativeQA3Canonicalization,
    registration: preparation.registration, snapshot,
    action: { key: action.action_key, state: action.state, inputHash: action.input_hash },
    modelDispatched: preparation.phase === 'qa_dispatching',
    cycleInput: a.context?.manifest?.cycleInput ?? null,
    preparationFailure: a.context?.nativeQA3PreparationFailure ?? null,
    semanticReport: qaArtifact, validation: a.context?.nativeQA3Validation ?? null };
  if (action.state !== 'completed') {
    if (!isDeepStrictEqual((await capture(occurrenceId, attemptId)).identity, captured.identity)) return fail('NATIVE_QA3_AUTHORITY_CHANGED');
    const result = { ...common, state: 'diagnostic', receipt: null, receiptSHA256: null,
      handoff: null, physicalInspection: null, physicalConfirmed: false,
      diagnostic: { code: a.error_code ?? action.receipt?.code ?? 'CPU_REPLAY_UNCONFIRMED',
        actionState: action.state, terminal: action.receipt ?? null } };
    if (Buffer.byteLength(JSON.stringify(result)) > 2 * 1024 * 1024) return fail('NATIVE_QA3_EVIDENCE_SIZE');
    return result;
  }
  const response = await cpuReplayIO.invoke('verify', registration, request, new AbortController().signal);
  if (response?.ok !== true || !isDeepStrictEqual(response.receipt, action.receipt)) return fail('NATIVE_QA3_RECEIPT_UNCONFIRMED');
  const receipt = validateCPUReplayReceipt(response.receipt, request, registration);
  const receiptSHA256 = canonicalSHA(receipt), physical = response.physicalInspection;
  const now = Date.now(), inspectedAt = Date.parse(physical?.observedAt);
  if (physical?.version !== 1 || physical.receiptSHA256 !== receiptSHA256 || !Number.isFinite(inspectedAt) ||
      inspectedAt > now + 1000 || now - inspectedAt > 15000 || physical.stopProof?.stopped !== true ||
      physical.stopProof.unit !== receipt.unit || physical.stopProof.mainPID !== 0 || physical.stopProof.populated !== false)
    return fail('NATIVE_QA3_STOP_UNCONFIRMED');
  const handoff = a.context?.manifest?.nativeQA3;
  if (handoff) {
    const raw = Buffer.from(cpuReplayCanonical(receipt));
    const expectedRef = { id: a.id, name: 'cpu-replay-observation.json', digest: hash(raw), size: raw.length,
      path: `/home/agent/inputs/${a.id}/cpu-replay-observation.json` };
    if (!isDeepStrictEqual(handoff, nativeQA3Handoff(receipt, expectedRef, preparation.registration)))
      return fail('NATIVE_QA3_HANDOFF_CHANGED');
  }
  if (qaArtifact) await dependencyArtifactBytes(qaArtifact);
  if (!isDeepStrictEqual(await cpuReplayIO.verificationRegistration(preparation.registration), registration))
    return fail('CPU_REPLAY_REGISTRATION_CHANGED');
  if (!isDeepStrictEqual((await capture(occurrenceId, attemptId)).identity, captured.identity)) return fail('NATIVE_QA3_AUTHORITY_CHANGED');
  const result = { ...common, state: 'evidence', receipt, receiptSHA256,
    receiptCanonicalization: nativeQA3Canonicalization, handoff: handoff ?? null,
    physicalInspection: physical, physicalConfirmed: true,
    diagnostic: a.context?.nativeQA3PreparationFailure ?? null };
  if (Buffer.byteLength(JSON.stringify(result)) > 2 * 1024 * 1024) return fail('NATIVE_QA3_EVIDENCE_SIZE');
  return result;
}

export function nativeQA3TechnicalResult(evidence: any) {
  const r = evidence.receipt;
  if (evidence.state !== 'evidence' || evidence.physicalConfirmed !== true || r?.exitCode !== 0 || r.signal !== null ||
      r.timedOut !== false || r.oomKilled !== false || r.cancelled !== false || r.outputsComplete !== true ||
      r.outputLimitExceeded !== false || !r.outputs?.result) return fail('NATIVE_QA3_CPU_EXECUTION_FAILED');
  let cpu: any;
  try { cpu = reportJSON(Buffer.from(r.outputs.result.base64, 'base64')); }
  catch { return fail('NATIVE_QA3_CPU_RESULT_INVALID'); }
  const refs = evidence.request.artifacts;
  if (!cpu || typeof cpu !== 'object' || Array.isArray(cpu) || cpu.passed !== true ||
      cpu.candidateSHA256 !== refs.find((x: any) => x.name === 'candidate.py')?.digest ||
      cpu.checkerSHA256 !== refs.find((x: any) => x.name === 'cpu-checks.py')?.digest)
    return fail('NATIVE_QA3_CPU_RESULT_INVALID');
  for (const [name, flag] of [['cases', 'passed'], ['negativeControls', 'detected']]) {
    const rows = cpu[name];
    if (!Array.isArray(rows) || !rows.length || rows.length > 10000 ||
        rows.some(row => !row || typeof row !== 'object' || Array.isArray(row) ||
          typeof row.name !== 'string' || !row.name.trim() || row[flag] !== true) ||
        new Set(rows.map(row => row.name)).size !== rows.length) return fail('NATIVE_QA3_CPU_RESULT_INVALID');
  }
  return cpu;
}

/** Reject mechanically unsuccessful execution before paying for a QA model.
 * A failed CPU receipt is not a harness termination receipt. */
export async function prepareNativeQA3(o: any, s: any, a: any) {
  const evidence = await nativeQA3Evidence(o.id, a.id);
  try { nativeQA3TechnicalResult(evidence); }
  catch (error) {
    const code = error instanceof DomainError ? error.code : 'NATIVE_QA3_CPU_RESULT_INVALID';
    const terminal = { kind: 'cpu-preparation-terminal', modelDispatched: false,
      cpu: { requestId: a.id, requestSHA256: evidence.requestSHA256,
        receiptSHA256: evidence.receiptSHA256, stopProof: evidence.physicalInspection?.stopProof } };
    const failure = { version: 1, status: 'rejected', code, modelDispatched: false,
      snapshotContractSHA256: evidence.snapshot.contractSHA256, source: evidence.snapshot.source,
      requestSHA256: evidence.requestSHA256, receiptSHA256: evidence.receiptSHA256,
      receiptCanonicalization: nativeQA3Canonicalization, terminal };
    await tx(async db => {
      await lock(db); await assertDependencySnapshot(db, o, s, a, evidence.snapshot);
      if (!await cpuPreparationOwned(db, a)) return fail('CPU_REPLAY_LEASE_LOST');
      const live = (await db.query('SELECT state FROM cycle_occurrences WHERE id=$1 FOR SHARE', [o.id])).rows[0];
      if (!live || !['running', 'queued'].includes(live.state)) return fail('NATIVE_QA3_AUTHORITY_CHANGED');
      await db.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{nativeQA3PreparationFailure}',$3::jsonb) WHERE id=$1 AND fence=$2 AND state='running'",
        [a.id, a.fence, JSON.stringify(failure)]);
    });
    throw new NativeQA3PreparationError(code, terminal);
  }
}

export async function checkNativeQA3(o: any, s: any, a: any) {
  const evidence = await nativeQA3Evidence(o.id, a.id);
  const snapshot = evidence.snapshot;
  let reason: string | null = null;
  if (!evidence.modelDispatched || !evidence.handoff || !evidence.semanticReport) return fail('NATIVE_QA3_HANDOFF_MISSING');
  let semantic: z.infer<typeof nativeQA3ReportSchema> | undefined;
  // Preserve protected-artifact/byte-binding failures as their real causes;
  // normalize only the explicitly versioned report parser/schema boundary.
  const semanticBytes = await dependencyArtifactBytes(evidence.semanticReport);
  try {
    semantic = nativeQA3ReportSchema.parse(reportJSON(semanticBytes));
  } catch { reason = 'NATIVE_QA3_REPORT_INVALID'; }
  if (semantic) {
    if (semantic.decision !== 'approve_for_check' || !semantic.scopePreserved || !semantic.canonicalPrecisionPreserved ||
        !semantic.evaluatorIndependent || !semantic.upstreamAttributionPreserved || semantic.blockingFindings.length ||
        !semantic.checkerReview.casesMeaningful || !semantic.checkerReview.negativeControlsDiscriminating ||
        !semantic.checkerReview.noTautologicalPass) reason = 'NATIVE_QA3_SEMANTIC_REJECT';
    try { nativeQA3TechnicalResult(evidence); }
    catch (error) { reason = error instanceof DomainError ? error.code : 'NATIVE_QA3_CPU_RESULT_INVALID'; }
  }
  const validation = { version: 1, contractVersion: 3, status: reason ? 'rejected' : 'matched',
    code: reason, snapshotContractSHA256: snapshot.contractSHA256, source: snapshot.source,
    requestSHA256: evidence.requestSHA256, receiptSHA256: evidence.receiptSHA256,
    receiptCanonicalization: nativeQA3Canonicalization, handoff: evidence.handoff,
    semanticReport: evidence.semanticReport, semanticDecision: semantic?.decision ?? null,
    candidateExecutedByController: true, gpuValidated: false, performanceGainProven: false };
  await tx(async db => {
    await lock(db); await assertDependencySnapshot(db, o, s, a, snapshot);
    const action = (await db.query('SELECT state,input_hash,receipt FROM cycle_action_receipts WHERE action_key=$1 FOR SHARE', [evidence.action.key])).rows[0];
    const current = (await db.query('SELECT context FROM cycle_attempts WHERE id=$1 AND fence=$2 FOR SHARE', [a.id, a.fence])).rows[0];
    const live = (await db.query('SELECT version,definition,input,state FROM cycle_occurrences WHERE id=$1 FOR SHARE', [o.id])).rows[0];
    if (!action || action.state !== 'completed' || action.input_hash !== evidence.requestSHA256 ||
        !isDeepStrictEqual(action.receipt, evidence.receipt) ||
        !isDeepStrictEqual(current?.context?.manifest?.nativeQA3, evidence.handoff) ||
        !live || live.version !== o.version || !isDeepStrictEqual(live.definition, o.definition) ||
        !isDeepStrictEqual(live.input, o.input) || !['running', 'queued'].includes(live.state)) return fail('NATIVE_QA3_AUTHORITY_CHANGED');
    await db.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{nativeQA3Validation}',$3::jsonb) WHERE id=$1 AND fence=$2 AND state='running'",
      [a.id, a.fence, JSON.stringify(validation)]);
  });
  if (reason) return fail(reason);
  return { snapshot, validation };
}

export async function assertNativeQA3Commit(db: any, o: any, s: any, a: any, gate: any) {
  await assertDependencySnapshot(db, o, s, a, gate.snapshot);
  const current = (await db.query('SELECT context FROM cycle_attempts WHERE id=$1 AND fence=$2 FOR SHARE', [a.id, a.fence])).rows[0];
  const action = (await db.query('SELECT state,input_hash,receipt FROM cycle_action_receipts WHERE action_key=$1 FOR SHARE',
    [`${o.id}:${a.id}:cpu-replay-v1`])).rows[0];
  const report = artifactRef((await db.query('SELECT id,name,digest,size FROM cycle_artifacts WHERE attempt_id=$1 AND name=$2 FOR SHARE',
    [a.id, s.definition.dependencyEvidence.reportArtifact])).rows[0]);
  const live = (await db.query('SELECT version,definition,input,state FROM cycle_occurrences WHERE id=$1 FOR SHARE', [o.id])).rows[0];
  if (gate.validation.status !== 'matched' || !isDeepStrictEqual(current?.context?.nativeQA3Validation, gate.validation) ||
      !isDeepStrictEqual(current?.context?.manifest?.nativeQA3, gate.validation.handoff) ||
      !isDeepStrictEqual(report, gate.validation.semanticReport) || action?.state !== 'completed' ||
      action.input_hash !== gate.validation.requestSHA256 || canonicalSHA(action.receipt) !== gate.validation.receiptSHA256 ||
      !live || live.version !== o.version || !isDeepStrictEqual(live.definition, o.definition) ||
      !isDeepStrictEqual(live.input, o.input) || !['running', 'queued'].includes(live.state))
    return fail('NATIVE_QA3_AUTHORITY_CHANGED');
}

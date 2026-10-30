// Read-only recovery of an ended worker's delivery. Never starts a model or tool.
import { readFile, realpath, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { collectArtifacts, parseArtifactResult } from './artifact-collector.mjs';
import { inspectUnit } from './process-stop.mjs';
import { workspaceOperation } from './workspaces.mjs';
import { protocolDelivery } from './protocol-delivery.mjs';
import { preserveRuntimeArtifactClaims } from './runtime-artifacts.mjs';
import { collectDeliveryManifest } from './delivery-manifest.mjs';

const hash = b => createHash('sha256').update(b).digest('hex');
/** Completed runs persist the exact, already-collected bytes in result.json.
 * response.txt belongs to the failure path and is not a completion contract. */
export function verifyCompletedNativeResult(state, result, candidate, identity) {
  if (!identity || state?.runId !== identity.runId || state?.workspace !== identity.workspace ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(state?.runId ?? '') ||
      !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(state?.workspace ?? '') ||
      state.runtime !== 'opencode' || state.state !== 'completed' || state.stopped !== true || state.exitCode !== 0 ||
      (result?.runId != null && result.runId !== identity.runId) ||
      (result?.workspace != null && result.workspace !== identity.workspace) ||
      result?.evidence?.runtime !== 'opencode' || result.evidence.stopped !== true || result.evidence.exitCode !== 0 ||
      result.evidence.finishReason !== 'stop') throw Error('NATIVE_COMPLETION_UNCONFIRMED');
  if (typeof result.summary !== 'string' || !Array.isArray(result.artifacts) || !result.artifacts.length ||
      result.artifacts.length > 32 || !Array.isArray(state.artifacts) || state.artifacts.length !== result.artifacts.length)
    throw Error('COMPLETED_ARTIFACTS_CHANGED');
  const seen = new Set(), manifest = new Map();
  for (const artifact of state.artifacts) {
    if (!artifact || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(artifact.name ?? '') ||
        !/^[a-f0-9]{64}$/.test(artifact.digest ?? '') || manifest.has(artifact.name))
      throw Error('COMPLETED_ARTIFACTS_CHANGED');
    manifest.set(artifact.name, artifact.digest);
  }
  let total = 0;
  for (const artifact of result.artifacts) {
    if (!artifact || typeof artifact.base64 !== 'string' || seen.has(artifact.name) ||
        manifest.get(artifact.name) !== artifact.digest) throw Error('COMPLETED_ARTIFACTS_CHANGED');
    seen.add(artifact.name);
    const bytes = Buffer.from(artifact.base64, 'base64');
    if (bytes.toString('base64') !== artifact.base64 || hash(bytes) !== artifact.digest) throw Error('ARTIFACT_CORRUPT');
    total += bytes.length;
    if (bytes.length > 8*1024*1024 || total > 24*1024*1024) throw Error('ARTIFACT_TOO_LARGE');
  }
  const originalCandidate = result.artifacts.find(artifact => artifact.name === 'candidate.json');
  const originalPatch = result.artifacts.find(artifact => artifact.name === 'changes.patch');
  if (!originalCandidate || !originalPatch || !/^[a-f0-9]{64}$/.test(candidate?.candidateDigest ?? '') ||
      !/^[a-f0-9]{64}$/.test(candidate?.baseDigest ?? '') || !Array.isArray(candidate?.files) || typeof candidate?.patch !== 'string')
    throw Error('SOURCE_CHANGED_DURING_COLLECTION');
  let recorded;
  try { recorded = JSON.parse(Buffer.from(originalCandidate.base64, 'base64').toString('utf8')); }
  catch { throw Error('ARTIFACT_CORRUPT'); }
  if (!recorded || recorded.candidateDigest !== candidate.candidateDigest || recorded.baseDigest !== candidate.baseDigest ||
      JSON.stringify(recorded.files) !== JSON.stringify(candidate.files) ||
      originalPatch.digest !== hash(Buffer.from(candidate.patch)))
    throw Error('SOURCE_CHANGED_DURING_COLLECTION');
  return { summary: result.summary, evidence: result.evidence,
    artifacts: result.artifacts.map(({ name, mime, digest, base64 }) => ({ name, mime, digest, base64 })) };
}

export async function readCompletedNativeResult(directory, state, candidate, identity) {
  if ((await stat(`${directory}/result.json`)).size > 36*1024*1024) throw Error('RESPONSE_TOO_LARGE');
  let result;
  try { result = JSON.parse(await readFile(`${directory}/result.json`, 'utf8')); }
  catch (error) { if (error instanceof SyntaxError) throw Error('ARTIFACT_CORRUPT'); throw error; }
  return verifyCompletedNativeResult(state, result, candidate, identity);
}

export function finalSessionResponse(rows) {
  const lastEnd = rows.findLast(r => r.type === 'turn/end');
  const lastMessage = rows.findLast(r => r.type === 'assistant/message');
  if (lastEnd?.data?.reason?.kind !== 'completed' || !lastMessage ||
      lastMessage.seq > lastEnd.seq || lastMessage.data?.turn !== lastEnd.data?.turn)
    throw Error('DELIVERY_NOT_FINISHED');
  return (lastMessage.data.message?.content ?? []).filter(b => b.type === 'text').map(b => b.text).join('\n');
}

export async function recoverArtifacts(id, readOnlySource = false, testFile, exactCompletedNative = false, options = {}) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id)) throw Error('INVALID_RUN');
  const inspect = options.inspect ?? inspectUnit, operation = options.operation ?? workspaceOperation;
  const dir = `${options.runRoot ?? '/var/lib/agentos-bridge/worker-runs'}/${id}`;
  const state = JSON.parse(await readFile(`${dir}/state.json`, 'utf8'));
  if (state.runId !== id || !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(state.workspace)) throw Error('RUN_IDENTITY_MISMATCH');
  const nativeDelivery = state.runtime === 'opencode' && typeof testFile === 'string';
  const manifestEligible = state.deliveryContract && state.deliveryContract.runId === id && state.stopped === true &&
    (state.runtime === 'opencode' ? state.exitCode === 0 && state.finishReason === 'stop' :
      state.runtime === 'muse' ? state.exitCode === 0 && state.finishReason === 'completed' :
      state.runtime === 'harness' && state.turnReason?.kind === 'completed');
  if (exactCompletedNative && (!nativeDelivery || state.state !== 'completed' || state.exitCode !== 0))
    throw Error('DELIVERY_NOT_RECOVERABLE');
  if (!(nativeDelivery && state.state === 'completed') && !(manifestEligible && state.state === 'completed') &&
      (state.state !== 'failed' || !/^(ARTIFACT_PATH|ARTIFACT_NAME|ARTIFACT_NAME_COLLISION|ARTIFACT_MISSING|RESULT_FORMAT|RESERVED_ARTIFACT_NAME)$/.test(state.code)))
    throw Error('DELIVERY_NOT_RECOVERABLE');
  const before = await inspect(`agentos-run-${id}`);
  if (!before.stopped) throw Error('STOP_UNCONFIRMED');
  const workspace = `${options.workspaceRoot ?? '/srv/agentos/workspaces'}/${state.workspace}`;
  if (await realpath(workspace) !== workspace) throw Error('INVALID_WORKSPACE');
  const sourceInventory=state.sourceContractVersion===1?{includeBaseFiles:true}:{};
  const initial = await operation({ action: 'inspect', workspace: state.workspace,...sourceInventory });
  if (exactCompletedNative) {
    const identity = { runId: id, workspace: state.workspace };
    const value = await readCompletedNativeResult(dir, state, initial, identity);
    const candidate = await operation({ action: 'inspect', workspace: state.workspace,...sourceInventory });
    // A new execution or external write must not turn these old bytes into a
    // delivery for a different candidate. Both workspace reads retain SOURCE_BUSY.
    verifyCompletedNativeResult(state, value, candidate, identity);
    if (readOnlySource && candidate.baseDigest !== candidate.candidateDigest) throw Error('SOURCE_CHANGED_DURING_VALIDATION');
    const after = await inspect(`agentos-run-${id}`);
    if (!after.stopped) throw Error('STOP_UNCONFIRMED');
    return { runId: id, workspace: state.workspace, checkedAt: after.checkedAt,
      process: after, candidate: { ...candidate, patch: undefined }, originalError: state.code,
      summary: value.summary, artifacts: value.artifacts, reservedClaims: [], collectorVersion: 'artifact-native-persisted-v1' };
  }
  if (state.deliveryContract && !manifestEligible) throw Error('DELIVERY_NOT_FINISHED');
  const manifestDelivery = manifestEligible ? await collectDeliveryManifest(workspace, `${dir}/home`, state.deliveryContract) : null;
  let text, value = manifestDelivery?.value;
  if (value) { /* Bound manifest has already verified every byte and attempt identity. */ }
  else if (nativeDelivery && state.state === 'completed') value = await protocolDelivery(workspace, testFile);
  else if (state.runtime === 'opencode') text = await readFile(`${dir}/response.txt`, 'utf8');
  else {
    const journal = `${dir}/home/sessions/--workspace--/${id}/session.v3.jsonl`;
    if ((await stat(journal)).size > 64 * 1024 * 1024) throw Error('JOURNAL_TOO_LARGE');
    text = finalSessionResponse((await readFile(journal, 'utf8')).split('\n').filter(Boolean).map(s => JSON.parse(s)));
  }
  if (!value) {
    try { value = parseArtifactResult(text); }
    catch (error) {
      if (!nativeDelivery || error.code !== 'RESULT_FORMAT') throw error;
      value = await protocolDelivery(workspace, testFile);
    }
  }
  const artifacts = manifestDelivery?.artifacts ?? await collectArtifacts(value, workspace);
  const candidate = await operation({ action: 'inspect', workspace: state.workspace,...sourceInventory });
  if (initial.candidateDigest !== candidate.candidateDigest) throw Error('SOURCE_CHANGED_DURING_COLLECTION');
  if (readOnlySource && candidate.baseDigest !== candidate.candidateDigest) throw Error('SOURCE_CHANGED_DURING_VALIDATION');
  const reservedClaims = preserveRuntimeArtifactClaims(artifacts);
  for (const [name, content] of [
    ['candidate.json', JSON.stringify({ ...candidate, patch: undefined })],
    ['changes.patch', candidate.patch],
  ]) {
    if (artifacts.some(a => a.name === name)) throw Error('RESERVED_ARTIFACT_NAME');
    const bytes = Buffer.from(content);
    artifacts.push({ name, mime: name.endsWith('.json') ? 'application/json' : 'text/plain',
      digest: hash(bytes), base64: bytes.toString('base64') });
  }
  const sizes = artifacts.map(a => Buffer.byteLength(a.base64, 'base64'));
  if (sizes.some(n => n > 8*1024*1024) || sizes.reduce((a,b)=>a+b,0) > 24*1024*1024) throw Error('ARTIFACT_TOO_LARGE');
  const after = await inspect(`agentos-run-${id}`);
  if (!after.stopped) throw Error('STOP_UNCONFIRMED');
  return { runId: id, workspace: state.workspace, checkedAt: after.checkedAt,
    process: after, candidate: { ...candidate, patch: undefined },
    originalError: state.code, summary: value.summary, artifacts, reservedClaims,
    ...(manifestDelivery ? { manifest: { version: 1, runId: id, digest: manifestDelivery.manifestDigest } } : {}),
    collectorVersion: manifestDelivery ? 'artifact-manifest-v1' : 'artifact-fenced-json-v5' };
}

if (process.argv[1] && import.meta.url === pathToFileURL(await realpath(process.argv[1])).href) {
  try { console.log(JSON.stringify(await recoverArtifacts(process.argv[2], process.argv[3] === '--read-only', process.argv[4], process.argv[5] === '--completed-native'))); }
  catch (e) { console.log(JSON.stringify({ error: e.code ?? e.message, reason: e.reason, index: e.index })); process.exitCode = 1; }
}

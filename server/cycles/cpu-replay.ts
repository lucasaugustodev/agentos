import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { z } from 'zod';
import { pool, tx, lock } from '../db.js';
import { hash } from '../memory.js';
import { DomainError } from '../contracts.js';
import { assertDependencySnapshot, assertFrozenDependencySource, dependencyArtifactBytes, type DependencySnapshot } from './dependency-evidence.js';
import { cycleEvent } from './store.js';

const action = 'native.cpu-replay.observe';
const uuid = z.string().uuid(), sha = z.string().regex(/^[a-f0-9]{64}$/);
const absolute = z.string().refine(p => path.posix.isAbsolute(p) && path.posix.normalize(p) === p && !p.startsWith('//'));
const profileSchema = z.enum(['python-cpu-argv-v1', 'python-cpu-candidate-base-v1']);
const registrationSchema = z.object({
  version: z.literal(1), enabled: z.literal(true), executorPath: absolute, executorSHA256: sha,
  stateDirectory: absolute, user: z.string().regex(/^[a-z_][a-z0-9_-]{0,31}$/).refine(x => x !== 'root'),
  projects: z.array(z.string().regex(/^[a-z0-9_-]{1,80}$/)).min(1).max(30),
  profiles: z.array(profileSchema).min(1).max(2),
  limits: z.object({ memoryMiB: z.number().int().min(64).max(512),
    timeoutSeconds: z.number().int().min(1).max(300),
    maxOutputBytes: z.number().int().min(1024).max(262144) }).strict(),
}).strict();
export type CPUReplayRegistration = z.infer<typeof registrationSchema>;
export type LoadedRegistration = { value: CPUReplayRegistration; path: string; digest: string };
const registrationBindingSchema = z.object({ path: absolute, digest: sha, executorPath: absolute,
  executorSHA256: sha, profile: profileSchema }).strict();
export function cpuReplayRegistrationBinding(r: LoadedRegistration, profile: string) {
  return registrationBindingSchema.parse({ path: r.path, digest: r.digest,
    executorPath: r.value.executorPath, executorSHA256: r.value.executorSHA256, profile });
}
const artifactNames = ['candidate.py', 'cpu-checks.py', 'cpu-result.json'];
export function cpuReplayArgv(profile: string) {
  if (profile === 'python-cpu-candidate-base-v1')
    return ['/usr/bin/python3', '-I', '-B', '/output/cpu-checks.py', '/output/candidate.py', '/inputs/base-candidate.py'];
  if (profile === 'python-cpu-argv-v1')
    return ['/usr/bin/python3', '-I', '-B', '/inputs/cpu-checks.py', '--candidate', '/inputs/candidate.py', '--output', '/output/cpu-replay-result.json'];
  return fail('CPU_REPLAY_PROFILE_INVALID');
}
const fail = (code: string): never => { throw new DomainError(code, 'CPU replay observation is unavailable or unconfirmed; no acceptance or automatic re-execution.'); };
export class CPUReplayPending extends DomainError {
  constructor(public waiting: boolean, public originalCode: string) {
    super('CPU_REPLAY_PENDING', 'CPU preparation is owned by this same attempt; waiting or inspecting, without a new attempt or acceptance.');
  }
}
const stable = (v: any): string => Array.isArray(v) ? '[' + v.map(stable).join(',') + ']'
  : v !== null && typeof v === 'object' ? '{' + Object.keys(v).sort().map(k => JSON.stringify(k) + ':' + stable(v[k])).join(',') + '}'
  : JSON.stringify(v);
function authorityHash(o: any, s: any, a: any) {
  return hash(stable([o.id, o.version, o.definition, o.input, s.id, s.definition, a.id, a.number, a.fence, a.account_id, a.model]));
}
export async function cpuPreparationOwned(db: any, a: any) {
  if (!a.cpuReplayLease) return true;
  const row = (await db.query('SELECT state,owner,fence,context FROM cycle_attempts WHERE id=$1 FOR SHARE', [a.id])).rows[0];
  return row?.state === 'running' && row.owner === a.owner && row.fence === a.fence &&
    row.context?.cpuReplayPreparation?.token === a.cpuReplayLease;
}
function noDispatch(response: any, request: any, registration: LoadedRegistration) {
  const proof = response?.admission;
  return response?.ok === false && response.code === 'CPU_REPLAY_WAITING' && proof &&
    Object.keys(proof).sort().join(',') === 'dispatched,executorSHA256,occurrenceId,registrationSHA256,requestId,requestSHA256,runFence,version' &&
    proof.version === 1 && proof.dispatched === false && proof.requestId === request.requestId &&
    proof.occurrenceId === request.occurrenceId && proof.runFence === request.runFence &&
    proof.requestSHA256 === hash(cpuReplayCanonical(request)) && proof.registrationSHA256 === registration.digest &&
    proof.executorSHA256 === registration.value.executorSHA256;
}
function terminalIncomplete(response: any, request: any, registration: LoadedRegistration) {
  const t = response?.terminal;
  return response?.ok === false && response.code === 'CPU_REPLAY_RESULT_LOST' && t?.version === 1 &&
    Object.keys(t).sort().join(',') === ['version', 'kind', 'requestId', 'occurrenceId', 'runFence', 'requestSHA256',
      'registrationSHA256', 'executorSHA256', 'unit', 'stopProof', 'observedAt', 'dispatchIntentPresent',
      'outputsUnavailable', 'executionOutcomeKnown', 'acceptance'].sort().join(',') &&
    t.kind === 'cpu-replay-incomplete' && t.requestId === request.requestId && t.occurrenceId === request.occurrenceId &&
    t.runFence === request.runFence && t.requestSHA256 === hash(cpuReplayCanonical(request)) &&
    t.registrationSHA256 === registration.digest && t.executorSHA256 === registration.value.executorSHA256 &&
    t.unit === `agentos-cpu-replay-${request.requestId}.service` && typeof t.dispatchIntentPresent === 'boolean' &&
    t.outputsUnavailable === true && t.executionOutcomeKnown === false && t.acceptance === false &&
    t.stopProof?.stopped === true && t.stopProof.mainPID === 0 && t.stopProof.populated === false &&
    t.stopProof.unit === t.unit && Number.isFinite(Date.parse(t.observedAt));
}
function cancelledBeforeDispatch(response: any, request: any, registration: LoadedRegistration) {
  const proof = response?.cancellation;
  if (response?.ok !== false || response.code !== 'CPU_REPLAY_CANCELLED_NO_DISPATCH' || proof?.cancelled !== true ||
      proof.unit !== `agentos-cpu-replay-${request.requestId}.service` || Object.keys(proof).length !== 10) return false;
  const { cancelled, unit, ...admission } = proof;
  return noDispatch({ ok: false, code: 'CPU_REPLAY_WAITING', admission }, request, registration);
}

// Only ASCII identity/base64 fields and finite integers enter this protocol.
// Explicit key sorting binds Python's canonical JSON without relying on jsonb ordering.
export function cpuReplayCanonical(value: any): string {
  if (Array.isArray(value)) return '[' + value.map(cpuReplayCanonical).join(',') + ']';
  if (value !== null && typeof value === 'object') return '{' + Object.keys(value).sort().map(k =>
    JSON.stringify(k) + ':' + cpuReplayCanonical(value[k])).join(',') + '}';
  if (typeof value === 'number' && !Number.isSafeInteger(value)) return fail('CPU_REPLAY_REQUEST_INVALID');
  if (typeof value === 'string' && /[^\x20-\x7e]/.test(value)) return fail('CPU_REPLAY_REQUEST_INVALID');
  if (value === undefined) return fail('CPU_REPLAY_REQUEST_INVALID');
  return JSON.stringify(value);
}
async function protectedBytes(file: string, maxBytes: number) {
  if (!absolute.safeParse(file).success) return fail('CPU_REPLAY_REGISTRATION_INVALID');
  let parent = path.dirname(file);
  for (;;) {
    const info = await lstat(parent);
    if (!info.isDirectory() || info.isSymbolicLink() || info.uid !== 0 || (info.mode & 0o022))
      return fail('CPU_REPLAY_REGISTRATION_UNPROTECTED');
    if (parent === '/') break;
    parent = path.dirname(parent);
  }
  const fd = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await fd.stat();
    if (!info.isFile() || info.uid !== 0 || (info.mode & 0o022) || info.nlink !== 1 || info.size > maxBytes)
      return fail('CPU_REPLAY_REGISTRATION_UNPROTECTED');
    const bytes = await fd.readFile();
    if (bytes.length !== info.size || bytes.length > maxBytes) return fail('CPU_REPLAY_REGISTRATION_CHANGED');
    return bytes;
  } finally { await fd.close(); }
}

export const cpuReplayIO = {
  spawn,
  async registration(file = process.env.AGENTOS_CPU_REPLAY_REGISTRATION): Promise<LoadedRegistration> {
    if (!file) return fail('CPU_REPLAY_DISABLED');
    try {
      const raw = await protectedBytes(file, 16384);
      const value = registrationSchema.parse(JSON.parse(raw.toString('utf8')));
      if (hash(await protectedBytes(value.executorPath, 1024 * 1024)) !== value.executorSHA256)
        return fail('CPU_REPLAY_EXECUTOR_CHANGED');
      return { value, path: file, digest: hash(raw) };
    } catch (error) {
      if (error instanceof DomainError) throw error;
      return fail('CPU_REPLAY_REGISTRATION_INVALID');
    }
  },
  async verificationRegistration(input: unknown): Promise<LoadedRegistration> {
    const binding = registrationBindingSchema.parse(input);
    let permitted = false;
    if (binding.path === process.env.AGENTOS_CPU_REPLAY_REGISTRATION) {
      const current = await cpuReplayIO.registration(binding.path);
      permitted = isDeepStrictEqual(cpuReplayRegistrationBinding(current, binding.profile), binding);
    }
    if (!permitted && process.env.AGENTOS_CPU_REPLAY_VERIFICATION_REGISTRATIONS) {
      const allow = z.object({ version: z.literal(1), registrations: z.array(registrationBindingSchema).max(32) }).strict()
        .parse(JSON.parse((await protectedBytes(process.env.AGENTOS_CPU_REPLAY_VERIFICATION_REGISTRATIONS, 32768)).toString('utf8')));
      permitted = allow.registrations.some(entry => isDeepStrictEqual(entry, binding));
    }
    if (!permitted) return fail('CPU_REPLAY_VERIFICATION_NOT_REGISTERED');
    const original = await cpuReplayIO.registration(binding.path);
    if (!isDeepStrictEqual(cpuReplayRegistrationBinding(original, binding.profile), binding)) return fail('CPU_REPLAY_REGISTRATION_CHANGED');
    return original;
  },
  async invoke(mode: 'execute' | 'inspect' | 'cancel' | 'verify', registered: LoadedRegistration, request: any, signal: AbortSignal): Promise<any> {
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const current = await cpuReplayIO.registration(mode === 'verify' ? registered.path : undefined);
    if (!isDeepStrictEqual(current, registered)) return fail('CPU_REPLAY_REGISTRATION_CHANGED');
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    return new Promise((resolve, reject) => {
      // No shell, inherited credentials or model-selected executable/arguments.
      const child = cpuReplayIO.spawn('/usr/bin/python3', ['-I', '-B', registered.value.executorPath, mode,
        registered.path, registered.digest], { shell: false, windowsHide: true,
        env: { PATH: '/usr/bin:/bin', LANG: 'C.UTF-8', PYTHONDONTWRITEBYTECODE: '1' },
        stdio: ['pipe', 'pipe', 'pipe'] });
      let stdout = Buffer.alloc(0), settled = false;
      const finish = (error?: Error, result?: any) => {
        if (settled) return; settled = true;
        clearTimeout(timer); signal.removeEventListener('abort', abort);
        error ? reject(error) : resolve(result);
      };
      const unavailable = () => new DomainError('CPU_REPLAY_UNCONFIRMED', 'CPU executor response was lost. Reconcile the persisted execution; do not repeat it.');
      const abort = () => { child.kill('SIGTERM'); finish(unavailable()); };
      const timer = setTimeout(abort, mode === 'verify' ? 15000 : (registered.value.limits.timeoutSeconds + 45) * 1000);
      signal.addEventListener('abort', abort, { once: true });
      if (signal.aborted) { abort(); return; }
      child.stdin.on('error', () => finish(unavailable()));
      child.stdout.on('data', b => {
        stdout = Buffer.concat([stdout, b]);
        if (stdout.length > 2 * 1024 * 1024) abort();
      });
      // Drain without returning untrusted stderr or credentials to diagnostic fields.
      child.stderr.on('data', () => {});
      child.on('error', () => finish(unavailable()));
      child.on('close', code => {
        if (code !== 0) return finish(unavailable());
        try { finish(undefined, JSON.parse(stdout.toString('utf8'))); }
        catch { finish(unavailable()); }
      });
      child.stdin.end(cpuReplayCanonical(request));
    });
  },
};

function outputBytes(value: any, maxBytes: number) {
  if (!value || typeof value !== 'object' || Object.keys(value).sort().join(',') !== 'base64,digest,size' ||
      !sha.safeParse(value.digest).success || !Number.isSafeInteger(value.size) || value.size < 0 || value.size > maxBytes ||
      typeof value.base64 !== 'string') return fail('CPU_REPLAY_RECEIPT_INVALID');
  const bytes = Buffer.from(value.base64, 'base64');
  if (bytes.toString('base64') !== value.base64 || bytes.length !== value.size || hash(bytes) !== value.digest)
    return fail('CPU_REPLAY_RECEIPT_INVALID');
}
export function validateCPUReplayReceipt(receipt: any, request: any, registration: LoadedRegistration) {
  if (!receipt || receipt.version !== 1 || receipt.kind !== 'cpu-replay-observation' ||
      receipt.observationOnly !== true || receipt.acceptance !== false || receipt.requestId !== request.requestId ||
      receipt.occurrenceId !== request.occurrenceId || receipt.runFence !== request.runFence || receipt.project !== request.project ||
      receipt.profile !== request.profile || !isDeepStrictEqual(receipt.baseSource, request.baseSource) ||
      !isDeepStrictEqual(receipt.source, request.source) || receipt.requestSHA256 !== hash(cpuReplayCanonical(request)) ||
      receipt.registrationSHA256 !== registration.digest || receipt.executorSHA256 !== registration.value.executorSHA256 ||
      request.registrationSHA256 !== registration.digest || request.executorSHA256 !== registration.value.executorSHA256 ||
      receipt.unit !== `agentos-cpu-replay-${request.requestId}.service` ||
      !isDeepStrictEqual(receipt.argv, cpuReplayArgv(request.profile)))
    return fail('CPU_REPLAY_RECEIPT_INVALID');
  if (typeof receipt.startedAt !== 'string' || typeof receipt.finishedAt !== 'string' ||
      !Number.isFinite(Date.parse(receipt.startedAt)) || !Number.isFinite(Date.parse(receipt.finishedAt)) ||
      Date.parse(receipt.finishedAt) < Date.parse(receipt.startedAt) ||
      !(receipt.exitCode === null || Number.isInteger(receipt.exitCode) && receipt.exitCode >= 0 && receipt.exitCode <= 255) ||
      !(receipt.signal === null || Number.isInteger(receipt.signal) && receipt.signal > 0 && receipt.signal <= 64) ||
      typeof receipt.timedOut !== 'boolean' || typeof receipt.oomKilled !== 'boolean' ||
      ![receipt.memoryPeakBytes, receipt.rssPeakBytes].every(v => v === null || Number.isSafeInteger(v) && v >= 0) ||
      receipt.exitCode !== null && receipt.signal !== null) return fail('CPU_REPLAY_RECEIPT_INVALID');
  const proof = receipt.stopProof;
  if (!proof || proof.stopped !== true || proof.mainPID !== 0 || proof.populated !== false || proof.unit !== receipt.unit)
    return fail('CPU_REPLAY_UNCONFIRMED');
  if (!receipt.outputs || Object.keys(receipt.outputs).sort().join(',') !== 'result,stderr,stdout')
    return fail('CPU_REPLAY_RECEIPT_INVALID');
  for (const name of ['stdout', 'stderr']) outputBytes(receipt.outputs[name], registration.value.limits.maxOutputBytes);
  if (receipt.outputs.result !== null) outputBytes(receipt.outputs.result, registration.value.limits.maxOutputBytes);
  return receipt;
}

async function assertReplayAuthority(db: any, o: any, s: any, a: any, snapshot: DependencySnapshot) {
  if (!await cpuPreparationOwned(db, a)) return fail('CPU_REPLAY_LEASE_LOST');
  await assertDependencySnapshot(db, o, s, a, snapshot);
  const live = (await db.query('SELECT version,definition,input,state FROM cycle_occurrences WHERE id=$1 FOR SHARE', [o.id])).rows[0];
  const step = (await db.query('SELECT definition FROM cycle_steps WHERE id=$1 FOR SHARE', [s.id])).rows[0];
  if (!live || live.version !== o.version || !isDeepStrictEqual(live.definition, o.definition) ||
      !step || !isDeepStrictEqual(step.definition, s.definition) || !isDeepStrictEqual(live.input, o.input) ||
      !(a.cpuReplayCancelOnly ? live.state === 'cancelling' : ['running', 'queued'].includes(live.state)))
    return fail('CPU_REPLAY_AUTHORITY_CHANGED');
}
async function frozenBase(o: any, db: any = pool, allowQAAlias = false) {
  const experiment = o.input?.experiment, ref = experiment?.sourceArtifact;
  const candidates = Array.isArray(experiment?.materials) ? experiment.materials.filter((m: any) => m.name === 'base-candidate.py') : [];
  if (candidates.length !== 1 || !ref || ref.name !== 'candidate.py' || !uuid.safeParse(ref.id).success ||
      !uuid.safeParse(ref.occurrenceId).success || !sha.safeParse(ref.digest).success ||
      !Number.isSafeInteger(ref.size) || ref.size <= 0 || ref.size > 1024 * 1024 ||
      typeof experiment.problem !== 'string' || ref.problem !== experiment.problem) return fail('CPU_REPLAY_BASE_BINDING');
  const material = candidates[0], bytes = Buffer.from(material.base64 ?? '', 'base64');
  if (bytes.toString('base64') !== material.base64 || material.bytes !== ref.size || bytes.length !== ref.size ||
      material.sha256 !== ref.digest || hash(bytes) !== ref.digest) return fail('CPU_REPLAY_BASE_BINDING');
  const row = (await db.query(`SELECT ar.id,ar.name,ar.digest,ar.size,a.id attempt_id,a.fence,a.state,a.receipt,
    a.number,a.context,s.id step_id,s.attempt,s.state step_state,s.definition step_definition,
    s.key,o.id occurrence_id,o.version definition_version,o.definition->>'project' project FROM cycle_artifacts ar
    JOIN cycle_attempts a ON a.id=ar.attempt_id JOIN cycle_steps s ON s.id=a.step_id
    JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE ar.id=$1 FOR SHARE OF ar,a,s,o`, [ref.id])).rows[0];
  const artifact = { id: ref.id, name: ref.name, digest: ref.digest, size: ref.size };
  if (!row || row.occurrence_id !== ref.occurrenceId || row.project !== o.definition.project ||
      row.state !== 'completed' || row.receipt?.stopped !== true ||
      !isDeepStrictEqual({ id: row.id, name: row.name, digest: row.digest, size: row.size }, artifact) ||
      !(await dependencyArtifactBytes(artifact)).equals(bytes)) return fail('CPU_REPLAY_BASE_BINDING');
  if (row.key !== 'produce') {
    const snapshot = row.context?.manifest?.dependencyEvidence;
    const qaVersion = row.step_definition?.parameters?.nativeQAContractVersion;
    const validation = qaVersion === 3 ? row.context?.nativeQA3Validation : row.context?.dependencyEvidenceValidation;
    if (!allowQAAlias || ![2, 3].includes(qaVersion) || row.step_state !== 'completed' || row.attempt !== row.number ||
        row.context?.manifest?.definitionVersion !== row.definition_version ||
        row.context?.manifest?.cycleInput?.attemptId !== row.attempt_id ||
        row.context?.manifest?.cycleInput?.occurrenceId !== row.occurrence_id ||
        row.context?.manifest?.cycleInput?.runFence !== row.fence || snapshot?.source?.key !== 'produce' ||
        validation?.status !== 'matched' || !isDeepStrictEqual(validation.source, snapshot.source))
      return fail('CPU_REPLAY_BASE_ALIAS_UNPROVEN');
    await assertFrozenDependencySource(db, { id: row.occurrence_id },
      { id: row.step_id, key: row.key, definition: row.step_definition },
      { id: row.attempt_id, number: row.number, fence: row.fence }, snapshot);
    const canonical = snapshot.artifacts.filter((f: any) => f.name === 'candidate.py');
    if (canonical.length !== 1 || canonical[0].digest !== artifact.digest || canonical[0].size !== artifact.size ||
        !(await dependencyArtifactBytes(canonical[0])).equals(bytes)) return fail('CPU_REPLAY_BASE_ALIAS_UNPROVEN');
  }
  return { input: { ...artifact, name: 'base-candidate.py', base64: bytes.toString('base64') },
    proof: { artifact, source: { occurrenceId: row.occurrence_id, attemptId: row.attempt_id, runFence: row.fence },
      inputSHA256: hash(JSON.stringify(o.input ?? {})) } };
}

export async function buildCPUReplayRequest(o: any, s: any, a: any, snapshot: DependencySnapshot, registration: LoadedRegistration) {
  if (s.definition.parameters.cpuReplayObservationVersion !== 1 || ![2, 3].includes(s.definition.parameters.nativeQAContractVersion) ||
      !o.definition.allowedActions.includes(action)) return fail('CPU_REPLAY_SCOPE');
  const profile = profileSchema.parse(s.definition.parameters.cpuReplayProfile);
  if (!registration.value.projects.includes(o.definition.project) || !registration.value.profiles.includes(profile)) return fail('CPU_REPLAY_SCOPE');
  if (!uuid.safeParse(a.id).success || !uuid.safeParse(a.fence).success || !uuid.safeParse(o.id).success)
    return fail('CPU_REPLAY_REQUEST_INVALID');
  const artifacts = [];
  for (const name of profile === 'python-cpu-candidate-base-v1' ? [...artifactNames, 'implementation.patch'] : artifactNames) {
    const refs = snapshot.artifacts.filter(r => r.name === name);
    if (refs.length !== 1 || refs[0].size <= 0 || refs[0].size > 1024 * 1024) return fail('CPU_REPLAY_INPUT_INVALID');
    artifacts.push({ ...refs[0], base64: (await dependencyArtifactBytes(refs[0])).toString('base64') });
  }
  const base = profile === 'python-cpu-candidate-base-v1' ? await frozenBase(o, pool, s.definition.parameters.nativeQAContractVersion === 3) : null;
  if (base) artifacts.push(base.input);
  const request = { version: 1, requestId: a.id, occurrenceId: o.id, runFence: a.fence,
    definitionVersion: o.version, project: o.definition.project, source: snapshot.source, artifacts,
    profile, baseSource: base?.proof ?? null,
    registrationSHA256: registration.digest, executorSHA256: registration.value.executorSHA256 };
  return { request, base };
}
export async function observeCPUReplay(o: any, s: any, a: any, snapshot: DependencySnapshot,
  signal: AbortSignal, onEvent: (kind: string, payload: any) => Promise<void>) {
  const registration = await cpuReplayIO.registration();
  const { request, base } = await buildCPUReplayRequest(o, s, a, snapshot, registration);
  const inputHash = hash(cpuReplayCanonical(request)), actionKey = `${o.id}:${a.id}:cpu-replay-v1`;
  const state = await tx(async db => {
    await lock(db);
    const own = (await db.query('SELECT owner,fence,state,context FROM cycle_attempts WHERE id=$1 FOR UPDATE', [a.id])).rows[0];
    const priorPreparation = own?.context?.cpuReplayPreparation;
    if (!own || own.owner !== a.owner || own.fence !== a.fence || own.state !== 'running') return fail('CPU_REPLAY_LEASE_LOST');
    if (priorPreparation) {
      if (!a.cpuReplayLease || priorPreparation.token !== a.cpuReplayLease || priorPreparation.phase !== 'preparing')
        return fail('CPU_REPLAY_LEASE_LOST');
      if (priorPreparation.requestSHA256 !== inputHash || priorPreparation.authoritySHA256 !== authorityHash(o, s, a) ||
          priorPreparation.registrationSHA256 !== registration.digest || priorPreparation.executorSHA256 !== registration.value.executorSHA256 ||
          !isDeepStrictEqual(priorPreparation.snapshot, snapshot)) return fail('CPU_REPLAY_INPUT_CHANGED');
    } else {
      a.cpuReplayLease = randomUUID();
      const preparation = { version: 1, phase: 'preparing', token: a.cpuReplayLease,
        requestSHA256: inputHash, registrationSHA256: registration.digest, executorSHA256: registration.value.executorSHA256,
        authoritySHA256: authorityHash(o, s, a), snapshot, polls: 0,
        ...(s.definition.parameters.nativeQAContractVersion === 3
          ? { registration: cpuReplayRegistrationBinding(registration, request.profile) } : {}) };
      await db.query("UPDATE cycle_attempts SET context=COALESCE(context,'{}'::jsonb)||$2::jsonb WHERE id=$1",
        [a.id, JSON.stringify({ cpuReplayPreparation: preparation })]);
    }
    await assertReplayAuthority(db, o, s, a, snapshot);
    if (base && !isDeepStrictEqual(base, await frozenBase(o, db, s.definition.parameters.nativeQAContractVersion === 3))) return fail('CPU_REPLAY_BASE_BINDING');
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const prior = (await db.query('SELECT * FROM cycle_action_receipts WHERE action_key=$1', [actionKey])).rows[0];
    if (prior) {
      if (prior.input_hash !== inputHash) return fail('CPU_REPLAY_INPUT_CHANGED');
      return 'inspect' as const;
    }
    if (a.cpuReplayCancelOnly) return fail('CPU_REPLAY_UNCONFIRMED');
    const uncertainty = await db.query("SELECT 1 FROM cycle_action_receipts WHERE occurrence_id=$1 AND action=$2 AND state<>'completed' LIMIT 1", [o.id, action]);
    if (uncertainty.rowCount) return fail('CPU_REPLAY_UNCONFIRMED');
    await db.query("INSERT INTO cycle_action_receipts(action_key,occurrence_id,action,state,input_hash) VALUES($1,$2,$3,'started',$4)",
      [actionKey, o.id, action, inputHash]);
    return 'execute' as const;
  });
  // Persisted DB ownership precedes any IPC or executable invocation.
  await onEvent('CPU_REPLAY_REQUESTED', { requestId: a.id, requestSHA256: inputHash, observationOnly: true, mode: a.cpuReplayCancelOnly ? 'cancel' : state });
  let receipt: any;
  try {
    const beforeIPC = async () => tx(async db => { await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
      if (signal.aborted) return fail('CPU_REPLAY_CANCELLED'); });
    await beforeIPC();
    let response = await cpuReplayIO.invoke(a.cpuReplayCancelOnly ? 'cancel' : state, registration, request, signal);
    // Re-entry is inspect-first. Only a current authenticated own-no-intent
    // proof can admit execute of THIS SAME request; an execution intent wins.
    if (!a.cpuReplayCancelOnly && state === 'inspect' && noDispatch(response, request, registration)) {
      await beforeIPC();
      response = await cpuReplayIO.invoke('execute', registration, request, signal);
    }
    if (a.cpuReplayCancelOnly && cancelledBeforeDispatch(response, request, registration)) {
      await beforeIPC();
      const confirmed = await cpuReplayIO.invoke('inspect', registration, request, signal);
      if (!cancelledBeforeDispatch(confirmed, request, registration) || !isDeepStrictEqual(confirmed, response))
        throw new CPUReplayPending(false, 'CPU_REPLAY_UNCONFIRMED');
      await tx(async db => {
        await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
        await db.query("UPDATE cycle_action_receipts SET state='cancelled_no_dispatch',receipt=$3,updated_at=now() WHERE action_key=$1 AND input_hash=$2",
          [actionKey, inputHash, JSON.stringify(response.cancellation)]);
      });
      return response.cancellation;
    }
    if (terminalIncomplete(response, request, registration)) {
      await beforeIPC();
      const confirmed = await cpuReplayIO.invoke('inspect', registration, request, signal);
      if (!terminalIncomplete(confirmed, request, registration) || !isDeepStrictEqual(confirmed.terminal, response.terminal))
        throw new CPUReplayPending(false, 'CPU_REPLAY_UNCONFIRMED');
      await tx(async db => {
        await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
        await db.query("UPDATE cycle_action_receipts SET state='stopped_incomplete',receipt=$3,updated_at=now() WHERE action_key=$1 AND input_hash=$2",
          [actionKey, inputHash, JSON.stringify(response.terminal)]);
      });
      if (a.cpuReplayCancelOnly) return response.terminal;
      return fail('CPU_REPLAY_RESULT_LOST');
    }
    if (response?.ok !== true) throw new CPUReplayPending(noDispatch(response, request, registration), response?.code ?? 'CPU_REPLAY_UNCONFIRMED');
    receipt = validateCPUReplayReceipt(response.receipt, request, registration);
    // An independent inspect call reads the protected receipt and current physical stop.
    // A forged response or a merely exited launcher cannot authorize continuation.
    await beforeIPC();
    const confirmed = await cpuReplayIO.invoke('inspect', registration, request, signal);
    if (confirmed?.ok !== true) throw new CPUReplayPending(false, 'CPU_REPLAY_UNCONFIRMED');
    if (!isDeepStrictEqual(confirmed.receipt, receipt)) return fail('CPU_REPLAY_RECEIPT_INVALID');
    validateCPUReplayReceipt(confirmed.receipt, request, registration);
  } catch (error) {
    await tx(async db => {
      await lock(db);
      if (await cpuPreparationOwned(db, a)) await db.query("UPDATE cycle_action_receipts SET state=$4,receipt=$3::jsonb,updated_at=now() WHERE action_key=$1 AND input_hash=$2 AND state IN('started','waiting')",
        [actionKey, inputHash, JSON.stringify({ code: error instanceof CPUReplayPending ? error.originalCode : 'CPU_REPLAY_UNCONFIRMED', requestId: a.id }),
          error instanceof CPUReplayPending && error.waiting ? 'waiting' : 'uncertain']);
    }).catch(() => {});
    if (error instanceof CPUReplayPending || error instanceof DomainError &&
        !['CPU_REPLAY_TIMEOUT', 'CPU_REPLAY_UNCONFIRMED', 'CPU_REPLAY_CANCELLED'].includes(error.code)) throw error;
    throw new CPUReplayPending(false, error instanceof DomainError ? error.code : 'CPU_REPLAY_TRANSPORT_LOST');
  }
  await tx(async db => {
    await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
    if (base && !isDeepStrictEqual(base, await frozenBase(o, db, s.definition.parameters.nativeQAContractVersion === 3))) return fail('CPU_REPLAY_BASE_BINDING');
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const changed = await db.query("UPDATE cycle_action_receipts SET state='completed',receipt=$3::jsonb,updated_at=now() WHERE action_key=$1 AND input_hash=$2 RETURNING action_key",
      [actionKey, inputHash, JSON.stringify(receipt)]);
    if (changed.rowCount !== 1) return fail('CPU_REPLAY_UNCONFIRMED');
  });
  await onEvent('CPU_REPLAY_COMPLETED', { requestId: a.id, requestSHA256: inputHash, receiptSHA256: hash(JSON.stringify(receipt)),
    exitCode: receipt.exitCode, signal: receipt.signal, timedOut: receipt.timedOut, oomKilled: receipt.oomKilled,
    memoryPeakBytes: receipt.memoryPeakBytes, rssPeakBytes: receipt.rssPeakBytes, observationOnly: true, acceptance: false });
  return receipt;
}

/** These are preparation continuations, not new attempts and not model retries. */
export async function deferCPUPreparation(o: any, s: any, a: any, error: CPUReplayPending) {
  return tx(async db => {
    await lock(db);
    if (!await cpuPreparationOwned(db, a)) return false;
    const row = (await db.query('SELECT context FROM cycle_attempts WHERE id=$1 FOR UPDATE', [a.id])).rows[0];
    const p = row.context?.cpuReplayPreparation;
    if (p?.phase !== 'preparing') return false;
    const polls = Math.min(1000000, (p.polls ?? 0) + 1), delay = error.waiting ? 3 : polls === 1 ? 5 : polls === 2 ? 15 : 60;
    await db.query("UPDATE cycle_attempts SET state='reconciling',context=jsonb_set(context,'{cpuReplayPreparation}',$2::jsonb),heartbeat_at=now() WHERE id=$1",
      [a.id, JSON.stringify({ ...p, phase: 'waiting', polls, lastCode: error.originalCode })]);
    await db.query("UPDATE cycle_steps SET state='reconciling',reason=$2,next_at=now()+$3*interval '1 second' WHERE id=$1 AND attempt=$4",
      [s.id, error.waiting ? 'Waiting for the registered CPU executor.' : 'Inspecting the original CPU replay request.', delay, a.number]);
    await db.query("UPDATE cycle_occurrences SET state='reconciling',reason=$2 WHERE id=$1 AND state NOT IN('paused','cancelling','cancelled')",
      [o.id, 'CPU preparation pending; the same attempt owns its execution.']);
    await cycleEvent(db, o.cycle_id, o.id, 'CPU_REPLAY_PREPARATION_DEFERRED',
      { requestId: a.id, runFence: a.fence, code: error.originalCode, waiting: error.waiting, delaySeconds: delay, newAttempt: false }, s.id, a.id);
    return true;
  });
}

export async function claimCPUPreparations(owner: string, limit = 2) {
  const cap = Number.isSafeInteger(limit) ? Math.min(2, Math.max(0, limit)) : 0;
  if (!cap) return [];
  return tx(async db => {
    await lock(db);
    // Maintenance blocks NEW attempts in CycleEngine.claim(). These are already
    // admitted owners: stopping their continuation would deadlock the drain,
    // whose guard correctly waits for their terminal physical state.
    const rows = (await db.query(`SELECT a.id FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id
      JOIN cycle_occurrences o ON o.id=s.occurrence_id LEFT JOIN accounts ac ON ac.id=a.account_id
      WHERE a.state='reconciling' AND s.state='reconciling' AND a.number=s.attempt
        AND o.state IN('running','queued','reconciling','cancelling') AND (ac.enabled=true OR o.state='cancelling')
        AND a.context#>>'{cpuReplayPreparation,phase}' IN('preparing','waiting')
        AND (s.next_at IS NULL OR s.next_at<=now())
        AND NOT EXISTS(SELECT 1 FROM cycle_attempts n WHERE n.step_id=s.id AND n.number>a.number)
      ORDER BY s.next_at NULLS FIRST,a.heartbeat_at,a.id LIMIT $1 FOR UPDATE OF a,s,o`, [cap])).rows;
    const jobs = [];
    for (const row of rows) {
      const a = (await db.query('SELECT * FROM cycle_attempts WHERE id=$1', [row.id])).rows[0];
      const s = (await db.query('SELECT * FROM cycle_steps WHERE id=$1', [a.step_id])).rows[0];
      const o = (await db.query('SELECT * FROM cycle_occurrences WHERE id=$1', [s.occurrence_id])).rows[0];
      const p = a.context.cpuReplayPreparation;
      if (p.version !== 1 || !uuid.safeParse(p.token).success || p.authoritySHA256 !== authorityHash(o, s, a)) {
        // Invalid durable authority is not retried and cannot starve valid rows.
        await db.query("UPDATE cycle_steps SET next_at=now()+interval '30 minutes' WHERE id=$1", [s.id]); continue;
      }
      const token = randomUUID();
      const changed = (await db.query("UPDATE cycle_attempts SET state='running',owner=$2,heartbeat_at=now(),context=jsonb_set(context,'{cpuReplayPreparation}',$3::jsonb) WHERE id=$1 RETURNING *",
        [a.id, owner, JSON.stringify({ ...p, phase: 'preparing', token })])).rows[0];
      await db.query("UPDATE cycle_steps SET state='running',next_at=NULL WHERE id=$1", [s.id]);
      await db.query("UPDATE cycle_occurrences SET state='running' WHERE id=$1 AND state<>'cancelling'", [o.id]);
      const account = (await db.query('SELECT * FROM accounts WHERE id=$1', [a.account_id])).rows[0];
      await cycleEvent(db, o.cycle_id, o.id, 'CPU_REPLAY_PREPARATION_RESUMED',
        { requestId: a.id, runFence: a.fence, token, inspectFirst: true, newAttempt: false }, s.id, a.id);
      jobs.push({ o, s, a: { ...changed, cpuReplayLease: token, cpuReplayCancelOnly: o.state === 'cancelling' }, account });
    }
    return jobs;
  });
}

export async function cancelCPUPreparation(o: any, s: any, a: any, signal: AbortSignal, emit: (kind: string, payload: any) => Promise<void>) {
  const snapshot: DependencySnapshot = a.context?.cpuReplayPreparation?.snapshot;
  const evidence = await observeCPUReplay(o, s, a, snapshot, signal, emit);
  await tx(async db => {
    await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
    await db.query("UPDATE cycle_attempts SET state='cancelled',finished_at=now(),receipt=$2 WHERE id=$1",
      [a.id, JSON.stringify({ kind: 'cpu-preparation-cancelled', modelDispatched: false, cpu: evidence })]);
    await db.query("UPDATE cycle_steps SET state='cancelled',reason='Preparation cancelled; no QA model was dispatched.' WHERE id=$1", [s.id]);
    await cycleEvent(db, o.cycle_id, o.id, 'CPU_REPLAY_PREPARATION_CANCELLED',
      { requestId: a.id, runFence: a.fence, modelDispatched: false, acceptance: false }, s.id, a.id);
  });
}

/** Single model-dispatch boundary: recovery after this marker belongs ONLY to
 * the existing harness reconciler. CPU preparation must never resend the model. */
export async function markCPUQADispatch(o: any, s: any, a: any, snapshot: DependencySnapshot, signal: AbortSignal) {
  await tx(async db => {
    await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const changed = await db.query(`UPDATE cycle_attempts SET context=jsonb_set(context,'{cpuReplayPreparation,phase}','"qa_dispatching"'::jsonb)
      WHERE id=$1 AND context#>>'{cpuReplayPreparation,phase}'='preparing'
        AND context#>>'{cpuReplayPreparation,token}'=$2 RETURNING id`, [a.id, a.cpuReplayLease]);
    if (changed.rowCount !== 1) return fail('CPU_REPLAY_LEASE_LOST');
    await cycleEvent(db, o.cycle_id, o.id, 'CPU_REPLAY_QA_DISPATCH_REQUESTED',
      { requestId: a.id, runFence: a.fence, token: a.cpuReplayLease, modelResult: false }, s.id, a.id);
  });
}

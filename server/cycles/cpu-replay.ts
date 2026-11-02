import { spawn } from 'node:child_process';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { z } from 'zod';
import { pool, tx, lock } from '../db.js';
import { hash } from '../memory.js';
import { DomainError } from '../contracts.js';
import { assertDependencySnapshot, dependencyArtifactBytes, type DependencySnapshot } from './dependency-evidence.js';

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
type LoadedRegistration = { value: CPUReplayRegistration; path: string; digest: string };
const artifactNames = ['candidate.py', 'cpu-checks.py', 'cpu-result.json'];
export function cpuReplayArgv(profile: string) {
  if (profile === 'python-cpu-candidate-base-v1')
    return ['/usr/bin/python3', '-I', '-B', '/output/cpu-checks.py', '/output/candidate.py', '/inputs/base-candidate.py'];
  if (profile === 'python-cpu-argv-v1')
    return ['/usr/bin/python3', '-I', '-B', '/inputs/cpu-checks.py', '--candidate', '/inputs/candidate.py', '--output', '/output/cpu-replay-result.json'];
  return fail('CPU_REPLAY_PROFILE_INVALID');
}
const fail = (code: string): never => { throw new DomainError(code, 'CPU replay observation is unavailable or unconfirmed; no acceptance or automatic re-execution.'); };

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
  async registration(): Promise<LoadedRegistration> {
    const file = process.env.AGENTOS_CPU_REPLAY_REGISTRATION;
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
  async invoke(mode: 'execute' | 'inspect', registered: LoadedRegistration, request: any, signal: AbortSignal): Promise<any> {
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const current = await cpuReplayIO.registration();
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
      const timer = setTimeout(abort, (registered.value.limits.timeoutSeconds + 45) * 1000);
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
  await assertDependencySnapshot(db, o, s, a, snapshot);
  const live = (await db.query('SELECT version,definition,input,state FROM cycle_occurrences WHERE id=$1 FOR SHARE', [o.id])).rows[0];
  const step = (await db.query('SELECT definition FROM cycle_steps WHERE id=$1 FOR SHARE', [s.id])).rows[0];
  if (!live || live.version !== o.version || !isDeepStrictEqual(live.definition, o.definition) ||
      !step || !isDeepStrictEqual(step.definition, s.definition) || !isDeepStrictEqual(live.input, o.input) ||
      !['running', 'queued'].includes(live.state))
    return fail('CPU_REPLAY_AUTHORITY_CHANGED');
}
async function frozenBase(o: any, db: any = pool) {
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
    s.key,o.id occurrence_id,o.definition->>'project' project FROM cycle_artifacts ar
    JOIN cycle_attempts a ON a.id=ar.attempt_id JOIN cycle_steps s ON s.id=a.step_id
    JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE ar.id=$1 FOR SHARE OF ar,a,s,o`, [ref.id])).rows[0];
  const artifact = { id: ref.id, name: ref.name, digest: ref.digest, size: ref.size };
  if (!row || row.occurrence_id !== ref.occurrenceId || row.project !== o.definition.project || row.key !== 'produce' ||
      row.state !== 'completed' || row.receipt?.stopped !== true ||
      !isDeepStrictEqual({ id: row.id, name: row.name, digest: row.digest, size: row.size }, artifact) ||
      !(await dependencyArtifactBytes(artifact)).equals(bytes)) return fail('CPU_REPLAY_BASE_BINDING');
  return { input: { ...artifact, name: 'base-candidate.py', base64: bytes.toString('base64') },
    proof: { artifact, source: { occurrenceId: row.occurrence_id, attemptId: row.attempt_id, runFence: row.fence },
      inputSHA256: hash(JSON.stringify(o.input ?? {})) } };
}

export async function observeCPUReplay(o: any, s: any, a: any, snapshot: DependencySnapshot,
  signal: AbortSignal, onEvent: (kind: string, payload: any) => Promise<void>) {
  if (s.definition.parameters.cpuReplayObservationVersion !== 1 || s.definition.parameters.nativeQAContractVersion !== 2 ||
      !o.definition.allowedActions.includes(action)) return fail('CPU_REPLAY_SCOPE');
  const registration = await cpuReplayIO.registration();
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
  const base = profile === 'python-cpu-candidate-base-v1' ? await frozenBase(o) : null;
  if (base) artifacts.push(base.input);
  const request = { version: 1, requestId: a.id, occurrenceId: o.id, runFence: a.fence,
    definitionVersion: o.version, project: o.definition.project, source: snapshot.source, artifacts,
    profile, baseSource: base?.proof ?? null,
    registrationSHA256: registration.digest, executorSHA256: registration.value.executorSHA256 };
  const inputHash = hash(cpuReplayCanonical(request)), actionKey = `${o.id}:${a.id}:cpu-replay-v1`;
  const state = await tx(async db => {
    await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
    if (base && !isDeepStrictEqual(base, await frozenBase(o, db))) return fail('CPU_REPLAY_BASE_BINDING');
    if (signal.aborted) return fail('CPU_REPLAY_CANCELLED');
    const prior = (await db.query('SELECT * FROM cycle_action_receipts WHERE action_key=$1', [actionKey])).rows[0];
    if (prior) {
      if (prior.input_hash !== inputHash) return fail('CPU_REPLAY_INPUT_CHANGED');
      return 'inspect' as const;
    }
    const uncertainty = await db.query("SELECT 1 FROM cycle_action_receipts WHERE occurrence_id=$1 AND action=$2 AND state<>'completed' LIMIT 1", [o.id, action]);
    if (uncertainty.rowCount) return fail('CPU_REPLAY_UNCONFIRMED');
    await db.query("INSERT INTO cycle_action_receipts(action_key,occurrence_id,action,state,input_hash) VALUES($1,$2,$3,'started',$4)",
      [actionKey, o.id, action, inputHash]);
    return 'execute' as const;
  });
  // Persisted DB ownership precedes any IPC or executable invocation.
  await onEvent('CPU_REPLAY_REQUESTED', { requestId: a.id, requestSHA256: inputHash, observationOnly: true, mode: state });
  let receipt: any;
  try {
    const response = await cpuReplayIO.invoke(state, registration, request, signal);
    if (response?.ok !== true) return fail('CPU_REPLAY_UNCONFIRMED');
    receipt = validateCPUReplayReceipt(response.receipt, request, registration);
    // An independent inspect call reads the protected receipt and current physical stop.
    // A forged response or a merely exited launcher cannot authorize continuation.
    const confirmed = await cpuReplayIO.invoke('inspect', registration, request, signal);
    if (confirmed?.ok !== true || !isDeepStrictEqual(confirmed.receipt, receipt)) return fail('CPU_REPLAY_UNCONFIRMED');
    validateCPUReplayReceipt(confirmed.receipt, request, registration);
  } catch (error) {
    await pool.query("UPDATE cycle_action_receipts SET state='uncertain',receipt=$3::jsonb,updated_at=now() WHERE action_key=$1 AND input_hash=$2 AND state='started'",
      [actionKey, inputHash, JSON.stringify({ code: 'CPU_REPLAY_UNCONFIRMED', requestId: a.id })]).catch(() => {});
    throw error;
  }
  await tx(async db => {
    await lock(db); await assertReplayAuthority(db, o, s, a, snapshot);
    if (base && !isDeepStrictEqual(base, await frozenBase(o, db))) return fail('CPU_REPLAY_BASE_BINDING');
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

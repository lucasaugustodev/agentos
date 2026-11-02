import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { promisify, isDeepStrictEqual } from 'node:util';
import { config } from '../config.js';
import { bridgeScript } from '../bridge-path.js';
import { tx, lock, type DB } from '../db.js';
import { cycleEvent } from './store.js';

const exec = promisify(execFile);
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const sha = (v: unknown) => createHash('sha256').update(JSON.stringify(v)).digest('hex');
const requestKind = 'TERMINATION_EVIDENCE_INSPECTION_REQUESTED';
const allowed = ['HARNESS_SANDBOX_OOM', 'RUNTIME_EXITED'];
export type TerminationExecutor = (file: string, args: string[], options: {
  timeout: number; maxBuffer: number; windowsHide: boolean;
}) => Promise<{ stdout: string }>;

const selection = `SELECT a.*,s.key,s.attempt step_attempt,s.state step_state,s.definition step_definition,
  o.id occurrence_id,o.cycle_id,o.version occurrence_version,o.definition occurrence_definition,o.state occurrence_state,
  (SELECT count(*)::int FROM cycle_events e WHERE e.attempt_id=a.id AND e.kind='${requestKind}') inspections,
  (SELECT max(created_at) FROM cycle_events e WHERE e.attempt_id=a.id AND e.kind='${requestKind}') last_inspection
  FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id JOIN cycle_occurrences o ON o.id=s.occurrence_id
  WHERE a.state='failed' AND a.error_code IN ('HARNESS_SANDBOX_OOM','RUNTIME_EXITED')
    AND (a.receipt IS NULL OR a.receipt='null'::jsonb) AND a.account_id IS NOT NULL
    AND a.number=s.attempt AND s.state='attention' AND s.definition->>'kind'='agent' AND o.state='attention'
    AND a.finished_at IS NOT NULL AND a.context#>>'{manifest,source}'='cycle'
    AND a.context#>>'{manifest,cycleId}'=o.cycle_id::text
    AND a.context#>>'{manifest,occurrenceId}'=o.id::text AND a.context#>>'{manifest,step}'=s.key
    AND jsonb_typeof(a.context#>'{manifest,definitionVersion}')='number'
    AND a.context#>>'{manifest,definitionVersion}'=o.version::text
    AND a.context#>>'{manifest,cycleInput,id}'=a.id::text
    AND a.context#>>'{manifest,cycleInput,attemptId}'=a.id::text
    AND a.context#>>'{manifest,cycleInput,runFence}'=a.fence::text
    AND a.context#>>'{manifest,cycleInput,occurrenceId}'=o.id::text
    AND (o.definition->>'project'||'/oc'||left(replace(o.id::text,'-',''),18)||'-'||s.key) ~ '^[a-z0-9_-]+/[a-z0-9_-]+$'
    AND EXISTS(SELECT 1 FROM cycle_events e WHERE e.attempt_id=a.id AND e.kind='HARNESS_STARTED'
      AND e.payload->>'unit'='agentos-run-'||a.id::text AND e.payload->>'workspace'=
      o.definition->>'project'||'/oc'||left(replace(o.id::text,'-',''),18)||'-'||s.key)
    AND NOT EXISTS(SELECT 1 FROM cycle_attempts n WHERE n.step_id=a.step_id AND
      (n.number>a.number OR n.state IN('running','cancelling','reconciling')))
    AND NOT EXISTS(SELECT 1 FROM cycle_events e WHERE e.attempt_id=a.id AND e.kind IN('HARNESS_NOT_STARTED','CANCEL_REQUESTED'))`;

function identity(row: any) {
  const manifest = row.context?.manifest, input = manifest?.cycleInput;
  const workspace = row.occurrence_definition?.project + '/oc' +
    row.occurrence_id.replaceAll('-', '').slice(0, 18) + '-' + row.key;
  if (!uuid.test(row.id) || !uuid.test(row.fence) || !uuid.test(row.occurrence_id) ||
      !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(workspace) || !allowed.includes(row.error_code) ||
      manifest?.source !== 'cycle' || manifest.cycleId !== row.cycle_id ||
      manifest.occurrenceId !== row.occurrence_id || manifest.step !== row.key ||
      manifest.definitionVersion !== row.occurrence_version || input?.id !== row.id ||
      input.attemptId !== row.id || input.runFence !== row.fence || input.occurrenceId !== row.occurrence_id ||
      !Number.isFinite(Date.parse(row.finished_at))) return null;
  return { runId: row.id, runFence: row.fence, workspace, occurrenceId: row.occurrence_id,
    cycleId: row.cycle_id, stepId: row.step_id, attemptNumber: row.number, code: row.error_code,
    accountId: row.account_id, model: row.model, owner: row.owner, finishedAt: row.finished_at,
    manifest, definition: row.step_definition, occurrenceDefinition: row.occurrence_definition };
}

export type TerminationClaim = { token: string; authority: NonNullable<ReturnType<typeof identity>> };

/** A separate bounded evidence sweep. It neither renews actor heartbeats nor
 * changes failed/attention to retry. Persistent backoff is 1m, 5m, then 30m;
 * a recovered inspector can confirm later. Success is excluded permanently. */
export async function claimCycleTerminationInspections(limit = 2): Promise<TerminationClaim[]> {
  const cap = Number.isSafeInteger(limit) ? Math.min(2, Math.max(0, limit)) : 0;
  if (!config.remoteExecution || !config.bridgeHost || cap === 0) return [];
  return tx(async db => {
    await lock(db);
    const rows = (await db.query(selection + `
      AND NOT EXISTS(SELECT 1 FROM cycle_events e WHERE e.attempt_id=a.id AND e.kind='${requestKind}'
        AND e.created_at>now()-(CASE
          WHEN (SELECT count(*) FROM cycle_events r WHERE r.attempt_id=a.id AND r.kind='${requestKind}')=1 THEN 60
          WHEN (SELECT count(*) FROM cycle_events r WHERE r.attempt_id=a.id AND r.kind='${requestKind}')=2 THEN 300
          ELSE 1800 END)*interval '1 second')
      ORDER BY last_inspection NULLS FIRST,a.finished_at,a.id
      LIMIT 16 FOR UPDATE OF a,s,o`)).rows;
    const claims: TerminationClaim[] = [];
    for (const row of rows) {
      if (claims.length >= cap) break;
      const backoff = row.inspections <= 1 ? 60000 : row.inspections === 2 ? 300000 : 1800000;
      if (row.last_inspection && Date.now() - Date.parse(row.last_inspection) < backoff) continue;
      const authority = identity(row);
      if (!authority) continue;
      // This event is emitted by the authenticated runner, not a workspace file.
      const started = (await db.query(`SELECT 1 FROM cycle_events WHERE attempt_id=$1 AND kind='HARNESS_STARTED'
        AND payload->>'unit'=$2 AND payload->>'workspace'=$3 LIMIT 1`,
      [row.id, 'agentos-run-' + row.id, authority.workspace])).rowCount;
      if (!started) continue;
      const token = randomUUID();
      await cycleEvent(db, row.cycle_id, row.occurrence_id, requestKind,
        { token, runId: row.id, runFence: row.fence, workspace: authority.workspace, attempt: row.inspections + 1 }, row.step_id, row.id);
      claims.push({ token, authority });
    }
    return claims;
  });
}

function proofMatches(proof: any, a: TerminationClaim['authority'], started: number) {
  const p = proof?.process, term = proof?.termination;
  const stamp = Date.parse(proof?.checkedAt), age = Date.now() - stamp;
  return proof?.version === 1 && proof.runId === a.runId && proof.runFence === a.runFence &&
    proof.workspace === a.workspace && proof.runtime === 'harness' && proof.state === 'failed' && proof.code === a.code &&
    /^[0-9a-f]{64}$/.test(proof.stateDigest ?? '') && Number.isFinite(age) && age >= -5000 && age <= 60000 &&
    stamp >= started - 5000 && stamp >= Date.parse(a.finishedAt) - 5000 &&
    p?.checkedAt === proof.checkedAt && p.unit === 'agentos-run-' + a.runId && p.stopped === true &&
    p.mainPid === 0 && p.populated === false && ['inactive', 'failed'].includes(p.activeState) &&
    term?.source === 'systemd-journal' && term.unit === p.unit + '.service' && term.result === 'oom-kill';
}

async function current(db: DB, claim: TerminationClaim) {
  const row = (await db.query(selection + ' AND a.id=$1 FOR UPDATE OF a,s,o', [claim.authority.runId])).rows[0];
  if (!row || !isDeepStrictEqual(identity(row), claim.authority)) return null;
  const last = (await db.query(`SELECT payload FROM cycle_events WHERE attempt_id=$1 AND kind=$2 ORDER BY id DESC LIMIT 1`,
    [row.id, requestKind])).rows[0]?.payload;
  return last?.token === claim.token && last.runFence === row.fence ? row : null;
}

/** The only input proof comes from our immutable bridge inspector over SSH.
 * There is no API accepting a worker/model JSON as terminal evidence. The
 * inspector reads state twice, checks process/cgroup twice, and reads the typed
 * systemd journal. Its checkedAt is inspection time, not a fabricated OOM time. */
export async function inspectAndRecordCycleTermination(claim: TerminationClaim, execute: TerminationExecutor = exec) {
  const a = claim.authority;
  const stillCurrent = await tx(async db => {
    await lock(db);
    if (!await current(db, claim)) return false;
    return !(await db.query(`SELECT 1 FROM cycle_events WHERE attempt_id=$1 AND kind IN
      ('TERMINATION_EVIDENCE_RECORDED','TERMINATION_EVIDENCE_UNAVAILABLE') AND payload->>'token'=$2 LIMIT 1`,
    [a.runId, claim.token])).rowCount;
  });
  if (!stillCurrent) return false;
  const started = Date.now();
  let proof: any, failure = 'INSPECTION_UNAVAILABLE';
  try {
    const { stdout } = await execute('ssh', ['-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
      config.bridgeHost, '/opt/deepseek-harness/node/bin/node', bridgeScript('inspect-harness-termination.mjs'),
      a.runId, a.workspace, a.runFence], { timeout: 20000, maxBuffer: 262144, windowsHide: true });
    proof = JSON.parse(stdout);
    if (!proofMatches(proof, a, started)) { proof = null; failure = 'TERMINATION_PROOF_INVALID'; }
  } catch { proof = null; }
  return tx(async db => {
    await lock(db);
    const row = await current(db, claim);
    if (!row) return false;
    const already = (await db.query(`SELECT 1 FROM cycle_events WHERE attempt_id=$1 AND kind IN
      ('TERMINATION_EVIDENCE_RECORDED','TERMINATION_EVIDENCE_UNAVAILABLE') AND payload->>'token'=$2 LIMIT 1`,
    [a.runId, claim.token])).rowCount;
    if (already) return false;
    if (!proof || !proofMatches(proof, a, started)) {
      await cycleEvent(db, a.cycleId, a.occurrenceId, 'TERMINATION_EVIDENCE_UNAVAILABLE',
        { token: claim.token, runId: a.runId, runFence: a.runFence, code: failure, originalErrorCode: a.code }, a.stepId, a.runId);
      return false;
    }
    const receipt = { version: 1, kind: 'harness-terminal-evidence', stopped: true,
      originalErrorCode: a.code, source: 'trusted_harness_termination_inspector', inspection: proof };
    const changed = await db.query(`UPDATE cycle_attempts SET receipt=$3 WHERE id=$1 AND fence=$2 AND state='failed'
      AND error_code=$4 AND (receipt IS NULL OR receipt='null'::jsonb) RETURNING id`,
    [a.runId, a.runFence, JSON.stringify(receipt), a.code]);
    if (!changed.rowCount) return false;
    await cycleEvent(db, a.cycleId, a.occurrenceId, 'TERMINATION_EVIDENCE_RECORDED',
      { token: claim.token, runId: a.runId, runFence: a.runFence, workspace: a.workspace,
        originalErrorCode: a.code, checkedAt: proof.checkedAt, stateDigest: proof.stateDigest, proofSHA256: sha(proof),
        automaticRetry: false, memoryChanged: false }, a.stepId, a.runId);
    return true;
  });
}

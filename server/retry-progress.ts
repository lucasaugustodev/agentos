import { randomUUID } from 'node:crypto';
import { pool, tx, lock, event, type DB } from './db.js';
import { hash } from './memory.js';
import {providerAvailabilityFailure} from '../shared/recovery.js';
import {readTaskObligation} from './work-obligations.js';
import {supervisionGenerationReady} from './supervision-generations.js';
import {assertStopped} from './continuation.js';
import {materialVerificationFailure} from './verification-progress.js';

/** These executor codes describe provider transport, not a refuted solution. */
export function retryAvailabilityFailure(code?:string|null) {
  return providerAvailabilityFailure(code) || ['MODEL_TRANSPORT_ERROR','MODEL_REQUEST_TIMEOUT'].includes(code??'');
}

export async function retryProgressSignature(db: DB, task: any, run: any, candidateDigest: string) {
  const dependencies = (
    await db.query(
      `SELECT DISTINCT a.name,a.digest FROM dependencies d
    JOIN tasks p ON p.id=d.depends_on JOIN runs r ON r.task_id=p.id AND r.attempt=p.attempt_count
    JOIN artifacts a ON a.run_id=r.id WHERE d.task_id=$1 AND p.state='accepted'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)
    AND a.name<>'task-instructions-history.json' ORDER BY a.name,a.digest`,
      [task.id],
    )
  ).rows;
  const memories = (
    await db.query('SELECT content FROM memories WHERE id=ANY($1::uuid[])', [
      task.required_memories ?? [],
    ])
  ).rows
    .map((m) => hash(m.content))
    .sort();
  const inputs = (task.execution?.inputs ?? [])
    .map((input: any) => [input.name, input.digest])
    .sort((a: any[], b: any[]) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return hash(
    JSON.stringify({
      version: 1,
      code: run.error_code ?? 'VALIDATION_FAILED',
      candidateDigest,
      inputs,
      dependencies,
      memories,
    }),
  );
}

/** Runs under the admission lock. A new supervisor UUID is not evidence of progress. */
export async function assertRetryProgress(db: DB, task: any, run: any, proof: any) {
  if(run.state==='failed' && retryAvailabilityFailure(run.error_code))return;
  if(task.execution?.readOnlySource && run.state==='submitted' && await materialVerificationFailure(db,run.id))
    throw Error('VERIFICATION_REPAIR_REQUIRED');
  if (!proof.candidate?.candidateDigest) return;
  const signature = await retryProgressSignature(db, task, run, proof.candidate.candidateDigest);
  const history = (
    await db.query(
      `SELECT payload FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'
    AND entity_id=$1 ORDER BY id DESC LIMIT 6`,
      [task.id],
    )
  ).rows;
  const obligation=await readTaskObligation(db,task.id,task.goal_id);
  const equivalent=obligation ? (await db.query(`SELECT payload FROM events
    WHERE entity_id=$1 AND kind='OBLIGATION_RETRY_ADMITTED' ORDER BY id DESC LIMIT 6`,
    [obligation.reservationKey])).rows : [];
  if ([history,equivalent].some(rows=>rows.length===6 && rows.every(e=>e.payload.signature===signature))) {
    const error = new Error('REPEATED_NO_PROGRESS');
    Object.assign(error, { progressSignature: signature });
    throw error;
  }
  await event(db, 'EXECUTION_RETRY_ADMITTED', task.id, {
    sourceRunId: run.id,
    signature,
    candidateDigest: proof.candidate.candidateDigest,
  });
  if(obligation)await event(db,'OBLIGATION_RETRY_ADMITTED',obligation.reservationKey,{
    taskId:task.id,goalId:task.goal_id,sourceRunId:run.id,signature,
    obligationKey:obligation.obligationKey,candidateDigest:proof.candidate.candidateDigest,
  });
}

export async function hasUnchangedBlockedRetry(db: DB, taskId: string, runId: string) {
  const blocked = (
    await db.query(
      `SELECT payload FROM events WHERE kind='SUPERVISION_PROGRESS_BLOCKED'
    AND entity_id=$1 AND payload->>'sourceRunId'=$2 ORDER BY id DESC LIMIT 1`,
      [taskId, runId],
    )
  ).rows[0]?.payload;
  if (!blocked) return true;
  const candidate = (
    await db.query(
      `SELECT payload->>'candidateDigest' digest FROM events
    WHERE kind='EXECUTION_RETRY_ADMITTED' AND entity_id=$1 AND payload->>'signature'=$2
    ORDER BY id DESC LIMIT 1`,
      [taskId, blocked.signature],
    )
  ).rows[0]?.digest;
  if (!candidate) return true;
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1', [taskId])).rows[0];
  const run = (await db.query('SELECT * FROM runs WHERE id=$1 AND task_id=$2', [runId, taskId]))
    .rows[0];
  return (
    !task || !run || (await retryProgressSignature(db, task, run, candidate)) === blocked.signature
  );
}

/** Request semantic replanning without admitting work or touching a live execution. */
export async function requestProgressReplan(taskId: string, runId: string, signature: string,
  options:{stopProof?:any}={}) {
  if(!/^[a-f0-9]{64}$/.test(signature))return false;
  return tx(async (db) => {
    await lock(db);
    const task = (
      await db.query(
        `SELECT t.* FROM tasks t JOIN goals g ON g.id=t.goal_id
      JOIN settings s ON s.id='supervision:'||g.id
      LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE t.id=$1 AND g.state='active' AND s.value->>'enabled'='true'
      AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')`,
        [taskId],
      )
    ).rows[0];
    if (
      !task ||
      [
        'accepted',
        'cancelled',
        'superseded',
        'queued',
        'running',
        'cancelling',
        'reconciling',
      ].includes(task.state)
    )
      return false;
    const latest = (
      await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [taskId])
    ).rows[0];
    if (latest?.id !== runId || latest.attempt!==task.attempt_count ||
      !['failed', 'submitted'].includes(latest.state) || retryAvailabilityFailure(latest.error_code)) return false;
    if (
      (
        await db.query(
          `SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')
      UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2`,
          [taskId, runId],
        )
      ).rowCount
    )
      return false;
    const already = (
      await db.query(
        `SELECT 1 FROM events WHERE kind='SUPERVISION_PROGRESS_BLOCKED'
      AND entity_id=$1 AND payload->>'sourceRunId'=$2 AND payload->>'signature'=$3`,
        [taskId, runId, signature],
      )
    ).rowCount;
    const reason =
      'REPEATED_NO_PROGRESS: seis retomadas com a mesma falha, candidata e insumos. Delegue um diagnóstico discriminante ou forneça novo insumo; outra decisão textual não libera a mesma repetição.';
    const requestKey='retry-progress:'+signature;
    const existing = (await db.query(`SELECT id,state FROM supervision_cases
      WHERE task_id=$1 AND source_run_id=$2 AND request_key=$3`,[taskId,runId,requestKey])).rows[0];
    if (
      (
        await db.query(
          `SELECT 1 FROM supervision_cases WHERE task_id=$1 AND id IS DISTINCT FROM $2::uuid
      AND state IN('open','decided','delegated','waiting_evidence')`,
          [taskId, existing?.id??null],
        )
      ).rowCount
    )
      return false;
    if (existing) return false; // Every recorded generation, including applied, is immutable here.
    if(!await supervisionGenerationReady(db))return false;
    const parent=(await db.query(`SELECT id,state FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2
      ORDER BY created_at DESC,id DESC LIMIT 1`,[taskId,runId])).rows[0];
    if(parent) {
      if(parent.state!=='applied' || !options.stopProof)return false;
      try {assertStopped(options.stopProof,latest,task);}catch{return false;}
    }
    const caseId = randomUUID();
    await db.query(
        `INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,request_key,parent_case_id)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          caseId,
          task.goal_id,
          taskId,
          runId,
          reason,
          JSON.stringify([{ runId, code: 'REPEATED_NO_PROGRESS', signature }]),
          requestKey,parent?.id??null,
        ],
      );
    await db.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1", [taskId]);
    if (!already)
      await event(db, 'SUPERVISION_PROGRESS_BLOCKED', taskId, {
        caseId,
        sourceRunId: runId,
        signature,
        requestKey,parentCaseId:parent?.id??null,
        reason,
      });
    return true;
  });
}

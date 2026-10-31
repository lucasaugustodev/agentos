import { lock, type DB } from './db.js';

/** Call inside the claim transaction. The drain reservation takes this same lock.
 * Receipt probes never use this gate: admitted effects must still be reconciled. */
export async function candidateValidationMayStart(db: DB) {
  await lock(db);
  const row = (await db.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0];
  return row?.value?.draining !== true;
}

/** Proposed/frozen records have no outstanding command. Preserve them for later;
 * counting deferred work as active would deadlock a drain that denies new work.
 * Unknown/malformed states fail closed. Physical CI units are an additional gate. */
export async function activeCandidateValidationCount(db: DB): Promise<number> {
  await lock(db);
  const row = (await db.query(`SELECT count(*)::int n FROM settings
    WHERE id LIKE 'candidate-validation:%'
      AND COALESCE(value->>'state','') NOT IN
        ('proposed','frozen','completed','failed','rejected')`)).rows[0];
  if (!Number.isSafeInteger(row?.n) || row.n < 0)
    throw Error('CANDIDATE_VALIDATION_QUIET_UNKNOWN');
  return row.n;
}

/** A timeout is not a stop receipt. Do not clear/cancel the original promises or
 * reset durable ownership; late completion must reconcile the original attempt. */
export async function waitForCandidateValidationStop(
  pending: readonly (Promise<unknown> | null)[], timeoutMs = 10000,
) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0)
    throw Error('CANDIDATE_VALIDATION_STOP_TIMEOUT_INVALID');
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      Promise.all(pending.filter((value): value is Promise<unknown> => value !== null)),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(Error('CANDIDATE_VALIDATION_STOP_UNCONFIRMED')), timeoutMs);
      }),
    ]);
  } catch {
    throw Error('CANDIDATE_VALIDATION_STOP_UNCONFIRMED');
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Signal callbacks must handle a refused shutdown without an unhandled rejection
 * terminating the controller. No success/exit is manufactured on failure. */
export function requestConfirmedShutdown(
  close: () => Promise<void>, unconfirmed: () => void,
) {
  return close().catch(() => { unconfirmed(); });
}

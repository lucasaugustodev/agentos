import { pool } from './db.js';
import { buildProviderHealth, type ProviderHealth, type ProviderSample } from './provider-health.js';

// Short enough that a provider which just started failing shows it, long enough
// that one slow reply does not read as an outage.
const WINDOW_MINUTES = 30;

/** Health per account over the recent window, derived from recorded model events.
 * Events name the account by bridge id (with colons), by panel id, or not at all;
 * the last ones are attributed through the run that emitted them. */
export async function providerHealth(accountIds: string[]): Promise<Record<string, ProviderHealth>> {
  if (!accountIds.length) return {};
  const { rows } = await pool.query(
    `SELECT coalesce(a.id, r.account_id) account_id, e.kind, e.payload->>'code' code,
            (e.payload->>'providerMs')::bigint provider_ms,
            (e.payload->>'queuedMs')::bigint queued_ms,
            floor(extract(epoch from e.created_at)*1000)::bigint at
       FROM events e
       LEFT JOIN accounts a ON a.id = e.payload->>'accountId' OR a.bridge_id = e.payload->>'accountId'
       LEFT JOIN runs r ON e.payload->>'accountId' IS NULL
        AND r.id = CASE WHEN e.payload->>'runId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (e.payload->>'runId')::uuid END
      WHERE e.kind IN('MODEL_RESPONSE','MODEL_ERROR')
        AND e.created_at > now() - ($2 || ' minutes')::interval
        AND coalesce(a.id, r.account_id) = ANY($1::text[])
      ORDER BY e.created_at`,
    [accountIds, String(WINDOW_MINUTES)],
  );
  const samples: ProviderSample[] = rows.map((r: any) => ({
    accountId: r.account_id,
    at: Number(r.at),
    ok: r.kind === 'MODEL_RESPONSE',
    code: r.code,
    providerMs: r.provider_ms === null ? null : Number(r.provider_ms),
    queuedMs: r.queued_ms === null ? null : Number(r.queued_ms),
  }));
  return Object.fromEntries(accountIds.map((id) => [id, buildProviderHealth(samples, id)]));
}

export const providerHealthWindowMinutes = WINDOW_MINUTES;

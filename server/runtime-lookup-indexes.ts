import type { Pool } from 'pg';

const indexes = [
  ['tasks_goal_scope_idx', `CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_goal_scope_idx
    ON public.tasks(goal_id,(execution->>'baseWorkspace'))`],
  ['artifacts_run_name_idx', 'CREATE INDEX CONCURRENTLY IF NOT EXISTS artifacts_run_name_idx ON public.artifacts(run_id,name)'],
  ['acceptances_run_created_idx', 'CREATE INDEX CONCURRENTLY IF NOT EXISTS acceptances_run_created_idx ON public.acceptances(run_id,created_at DESC)'],
  ['events_run_activity_idx', `CREATE INDEX CONCURRENTLY IF NOT EXISTS events_run_activity_idx
    ON public.events((payload->>'runId'),created_at DESC)
    WHERE kind IN('OPENCODE_TOOL_EVENT','HARNESS_TOOL_EVENT','ARTIFACT_CREATED','MODEL_RESPONSE')`],
  ['events_confirmed_run_kind_idx', `CREATE INDEX CONCURRENTLY IF NOT EXISTS events_confirmed_run_kind_idx
    ON public.events((payload->>'runId'),kind)
    WHERE kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'confirmed'='true'`],
  ['events_kind_created_idx', 'CREATE INDEX CONCURRENTLY IF NOT EXISTS events_kind_created_idx ON public.events(kind,created_at DESC)'],
] as const;

/** Build outside the global admission transaction. PostgreSQL permits live
 * writes throughout a concurrent build; one session owns installation. */
export async function installRuntimeLookupIndexes(pool: Pool) {
  const client = await pool.connect();
  let owned = false;
  try {
    await client.query('SELECT pg_advisory_lock(4280099)');
    owned = true;
    for (const [name, statement] of indexes) {
      const existing = (await client.query(
        'SELECT indisvalid AND indisready valid FROM pg_index WHERE indexrelid=to_regclass($1)',
        ['public.' + name],
      )).rows[0];
      if (existing) {
        if (!existing.valid) throw Error('RUNTIME_LOOKUP_INDEX_INVALID');
        continue;
      }
      await client.query(statement);
    }
  } finally {
    if (owned) await client.query('SELECT pg_advisory_unlock(4280099)').catch(() => {});
    client.release();
  }
}

import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID, randomBytes } from 'node:crypto';
import { migrate, pool } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { installTaskExecutionMetadata, taskExecutionMetadataReceipt } from '../server/task-execution-metadata.js';
import { readContinuationCandidates } from '../server/continuation-candidates.js';

const now = '2026-10-11T20:00:00.000Z';
const legacy = `SELECT t.*,r.id run_id,r.fence,r.state run_state,r.error_code,r.account_id run_account,r.model run_model FROM tasks t JOIN goals g ON g.id=t.goal_id
      LEFT JOIN settings s ON s.id='autonomy:'||g.project
      JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
      WHERE g.state IN ('active','failed') AND (t.state IN ('failed','submitted','recovery_blocked','reconciling','recovering_artifacts')
        OR (t.state='blocked_failed' AND t.execution->>'readOnlySource'='true' AND r.state='submitted'))
      AND COALESCE(s.value->>'enabled','true')='true'
      AND t.execution->>'workKind' IS DISTINCT FROM 'planning'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR s.value->>'enabled'='true')
      AND (t.execution->'continuation'->>'runId' IS DISTINCT FROM r.id::text
        OR (COALESCE(t.execution->'continuation'->>'nextAt','') <= $1
          AND NOT (COALESCE(t.execution->'continuation'->>'state','') = 'needs_action'
            AND COALESCE(t.execution->'continuation'->>'reasonCode','') = 'REPEATED_NO_PROGRESS')))
      AND t.execution->>'kind' IN ('harness','opencode') ORDER BY t.updated_at LIMIT 30`;
let ordinal = 0;

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installTaskExecutionMetadata();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'autonomy:continuation-candidates-%'");
  await seedRegistry();
  ordinal = 0;
});
afterAll(async () => {
  await pool.query("DELETE FROM settings WHERE id LIKE 'autonomy:continuation-candidates-%'");
  await pool.end();
});

async function goal(state = 'active', autonomy?: unknown) {
  const id = randomUUID(), project = 'continuation-candidates-' + id;
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Candidate','Candidate',$2,$1::text,$3)`, [id, project, state]);
  if (autonomy !== undefined) await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',
    ['autonomy:' + project, JSON.stringify(autonomy)]);
  return id;
}

async function task(goalId: string, execution: any = { kind: 'opencode' }, options: any = {}) {
  const id = randomUUID(), run = randomUUID(), fence = randomUUID(), attempt = options.attempt ?? 1;
  const value = typeof execution === 'function' ? execution(run) : execution;
  const updated = new Date(Date.parse('2026-10-11T01:00:00.000Z') + ordinal++ * 1000).toISOString();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,
      attempt_count,recovery,updated_at)
    VALUES($1::uuid,$2::uuid,$1::text,'Candidate','Preserved instruction','qa.backend',$3,'[]',$4,$5,$6,$7)`,
    [id, goalId, options.state ?? 'failed', value === undefined ? null : JSON.stringify(value), attempt,
      JSON.stringify(options.recovery ?? {}), updated]);
  if (!options.noRun) await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,
      agent_version,state,error_code)
    VALUES($1,$2,$3,$4,'candidate-account','candidate-model','harness',1,$5,'AUTH_REQUIRED')`,
    [run, id, attempt, fence, options.runState ?? 'failed']);
  return { id, run, fence, updated, execution: value };
}

async function equivalent() {
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const expected = (await db.query(legacy, [now])).rows;
    const actual = await readContinuationCandidates(db, now);
    expect(actual).toEqual(expected);
    await db.query('ROLLBACK');
    return actual;
  } finally { await db.query('ROLLBACK'); db.release(); }
}

it('matches raw SQL for null, scalar and malformed execution fields without using panel coercion', async () => {
  const g = await goal();
  for (const value of [undefined, null, [], 2, true, 'scalar', {}, { kind: null }, { kind: 4 },
    { kind: ['opencode'] }, { kind: 'other' }, { kind: 'harness', workKind: 'planning' },
    { kind: 'harness', workKind: null }, { kind: 'opencode', workKind: false },
    { kind: 'opencode', workKind: { nested: true } }]) {
    const t = await task(g, value);
    if (value === undefined) await pool.query('UPDATE tasks SET execution=NULL WHERE id=$1', [t.id]);
  }
  const raw = await task(g, { kind: 'opencode', readOnlySource: 'not-a-boolean' });
  const actual = await equivalent();
  expect(actual.some(t => t.id === raw.id)).toBe(true);
  expect(actual.find(t => t.id === raw.id).execution.readOnlySource).toBe('not-a-boolean');
});

it('preserves blocked_failed text equality, including string true, without boolean casting', async () => {
  const g = await goal(), expected: string[] = [];
  for (const value of [true, 'true', false, 'false', 'yes', 'TRUE', 1, null, {}, []]) {
    const row = await task(g, { kind: 'opencode', readOnlySource: value }, { state: 'blocked_failed', runState: 'submitted' });
    if (value === true || value === 'true') expected.push(row.id);
  }
  await task(g, { kind: 'harness', readOnlySource: true }, { state: 'blocked_failed', runState: 'failed' });
  expect((await equivalent()).map(t => t.id)).toEqual(expected);
});

it('preserves continuation run identity and exact text due comparisons without date casts', async () => {
  const g = await goal();
  const options = [undefined, null, false, 3, 'string', [], {}, { runId: null, nextAt: '9999' },
    { runId: 'another-run', nextAt: '9999' }];
  for (const continuation of options) await task(g, { kind: 'opencode', continuation });
  for (const nextAt of [undefined, null, '', now, '2026-10-11T19:59:59.999Z',
    '2026-10-11T20:00:00.001Z', '9999', 'not-a-date', 0, false, {}, []]) {
    await task(g, (runId: string) => ({ kind: 'opencode', continuation: { runId, nextAt } }));
  }
  const stale = await task(g, { kind: 'harness', continuation: { runId: randomUUID(), nextAt: '9999' } });
  const actual = await equivalent();
  expect(actual.some(row => row.id === stale.id)).toBe(true);
  expect(actual.every(row => row.execution.continuation?.nextAt !== 'not-a-date')).toBe(true);
});

it('checks due before LIMIT30, keeping order behind more than30 older waits and non-runtime tasks', async () => {
  const g = await goal();
  for (let i = 0; i < 35; i++) await task(g, (runId: string) => ({ kind: 'opencode', continuation: { runId, nextAt: '9999' } }));
  for (let i = 0; i < 4; i++) await task(g, { kind: 'opencode', workKind: 'planning' });
  const expected: string[] = [];
  for (let i = 0; i < 35; i++) expected.push((await task(g, { kind: i % 2 ? 'harness' : 'opencode' })).id);
  const actual = await equivalent();
  expect(actual).toHaveLength(30);
  expect(actual.map(t => t.id)).toEqual(expected.slice(0, 30));
});

it('preserves goal, task, autonomy and manual-recovery predicates', async () => {
  for (const state of ['active', 'failed', 'paused', 'completed']) {
    const g = await goal(state);
    await task(g);
    await task(g, { kind: 'harness' }, { recovery: { mode: 'manual' } });
  }
  for (const autonomy of [{ enabled: false }, { enabled: 'false' }, { enabled: true },
    { enabled: 'true' }, { enabled: null }, {}]) {
    const g = await goal('active', autonomy);
    await task(g);
    await task(g, { kind: 'opencode' }, { recovery: { mode: 'manual' } });
  }
  const g = await goal();
  for (const state of ['failed', 'submitted', 'recovery_blocked', 'reconciling', 'recovering_artifacts',
    'ready', 'running', 'cancelled', 'superseded', 'accepted', 'paused']) await task(g, { kind: 'harness' }, { state });
  await task(g, { kind: 'opencode' }, { noRun: true });
  const actual = await equivalent();
  expect(actual.length).toBeGreaterThan(0);
  expect(actual.every(t => !['cancelled', 'superseded', 'accepted', 'paused'].includes(t.state))).toBe(true);
});

it('returns the latest actual run and full task payload while leaving cancellation to the existing controller', async () => {
  const g = await goal(), payload = randomBytes(256 * 1024).toString('base64');
  const t = await task(g, { kind: 'harness', privatePayload: payload,
    supervision: { token: 'test-token' }, continuation: { runId: randomUUID(), nextAt: '9999' } });
  const latest = randomUUID(), fence = randomUUID();
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'latest-account','latest-model','harness',1,'submitted')`, [latest, t.id, fence]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [t.id, JSON.stringify({ runId: latest })]);
  const [row] = await equivalent();
  expect(row).toMatchObject({ id: t.id, run_id: latest, fence, run_state: 'submitted',
    run_account: 'latest-account', run_model: 'latest-model', execution: t.execution });
  expect(row.execution.privatePayload).toBe(payload);
  expect(row.execution.supervision.token).toBe('test-token');
});

it.each(['missing', 'version', 'goal'] as const)('fails closed when %s metadata would hide a candidate', async mode => {
  const g = await goal(), other = await goal(), bad = await task(g, { kind: 'other' });
  await task(g, { kind: 'opencode' });
  if (mode === 'missing') await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [bad.id]);
  if (mode === 'version') await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1', [bad.id]);
  if (mode === 'goal') await pool.query('UPDATE task_execution_metadata SET goal_id=$2 WHERE task_id=$1', [bad.id, other]);
  await expect(readContinuationCandidates(pool, now)).rejects.toMatchObject({ code: 'TASK_EXECUTION_METADATA_INCOMPLETE' });
});

it('rechecks every relational candidate after readiness, including missing metadata beyond the output limit', async () => {
  const g = await goal();
  for (let i = 0; i < 31; i++) await task(g);
  const bad = await task(g, { kind: 'other' });
  let removed = false;
  const db = { query: async (sql: string, values?: any[]) => {
    if (sql.startsWith('WITH relational AS MATERIALIZED')) {
      await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [bad.id]);
      removed = true;
    }
    return pool.query(sql, values);
  } };
  await expect(readContinuationCandidates(db as any, now)).rejects.toMatchObject({ code: 'TASK_EXECUTION_METADATA_MISSING' });
  expect(removed).toBe(true);
});

it('refuses changed readiness instead of preparing schema or changing tasks', async () => {
  const g = await goal(), t = await task(g);
  const receipt = (await pool.query('SELECT value FROM settings WHERE id=$1', [taskExecutionMetadataReceipt])).rows[0].value;
  const before = (await pool.query('SELECT xmin::text version,state,attempt_count,execution FROM tasks WHERE id=$1', [t.id])).rows[0];
  try {
    await pool.query("UPDATE settings SET value=jsonb_set(value,'{sourceHash}','\"wrong\"') WHERE id=$1", [taskExecutionMetadataReceipt]);
    await expect(readContinuationCandidates(pool, now)).rejects.toMatchObject({ code: 'TASK_EXECUTION_METADATA_REQUIRED' });
    expect((await pool.query('SELECT xmin::text version,state,attempt_count,execution FROM tasks WHERE id=$1', [t.id])).rows[0]).toEqual(before);
  } finally { await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [taskExecutionMetadataReceipt, JSON.stringify(receipt)]); }
});

it('reads metadata, raw due and task hydration from the same visible version during a concurrent update', async () => {
  const g = await goal(), t = await task(g), writer = await pool.connect();
  try {
    await writer.query('BEGIN');
    await writer.query(`UPDATE tasks SET execution=$2 WHERE id=$1`,
      [t.id, JSON.stringify({ kind: 'opencode', privatePayload: 'new', continuation: { runId: t.run, nextAt: '9999' } })]);
    const pending = await readContinuationCandidates(pool, now);
    expect(pending.find(row => row.id === t.id).execution).toEqual(t.execution);
    await writer.query('COMMIT');
    expect(await readContinuationCandidates(pool, now)).toEqual([]);
  } finally { await writer.query('ROLLBACK'); writer.release(); }
});

it('does not mutate tasks, runs, settings or events during selection', async () => {
  const g = await goal(), t = await task(g);
  const db = await pool.connect();
  try {
    await db.query('BEGIN READ ONLY');
    const actual = await readContinuationCandidates(db, now);
    expect(actual.map(row => row.id)).toEqual([t.id]);
    await db.query('ROLLBACK');
  } finally { await db.query('ROLLBACK'); db.release(); }
});

it('stops re-selecting a run already escalated to a person, but keeps a new attempt', async () => {
  const g = await goal();
  // Due by time, but already escalated: re-inspecting the same finished run
  // cannot change the answer, and the rescan was emitting a status event every
  // pass, burying the escalation it had already made.
  const parked = await task(g, (run: string) => ({ kind: 'harness',
    continuation: { runId: run, state: 'needs_action', reasonCode: 'REPEATED_NO_PROGRESS',
      nextAt: '2026-10-11T19:00:00.000Z' } }));
  const waiting = await task(g, (run: string) => ({ kind: 'harness',
    continuation: { runId: run, state: 'checking', nextAt: '2026-10-11T19:00:00.000Z' } }));
  // Quota and disconnected accounts recover on their own, so their escalations
  // stay re-checkable; only repeated-no-progress is terminal for this run.
  const quota = await task(g, (run: string) => ({ kind: 'harness',
    continuation: { runId: run, state: 'needs_action', nextAt: '2026-10-11T19:00:00.000Z' } }));
  const ids = (await equivalent()).map((row: any) => row.id);
  expect(ids).not.toContain(parked.id);
  expect(ids).toContain(waiting.id);
  expect(ids).toContain(quota.id);
});

it('re-selects an escalated task once a newer attempt replaces the run', async () => {
  const g = await goal();
  // A person acting queues another attempt; its different runId re-opens the
  // task through the clause that compares runId, with no time involved.
  const t = await task(g, { kind: 'harness',
    continuation: { runId: randomUUID(), state: 'needs_action', reasonCode: 'REPEATED_NO_PROGRESS',
      nextAt: '2099-01-01T00:00:00.000Z' } });
  expect((await equivalent()).map((row: any) => row.id)).toContain(t.id);
});

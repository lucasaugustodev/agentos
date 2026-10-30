import { beforeAll, beforeEach, afterAll, expect, it } from 'vitest';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool, migrate } from '../server/db.js';
import {
  migrationSource, migrationModules, migrationReceiptKey, migrationAdvisoryLock,
  prepareSchema, requireSchemaReady,
} from '../server/schema-readiness.js';

beforeAll(async () => { await migrate(); });
beforeEach(async () => { await pool.query('DELETE FROM settings WHERE id=$1', [migrationReceiptKey]); });
afterAll(async () => {
  await pool.query('DELETE FROM settings WHERE id=$1', [migrationReceiptKey]);
  await pool.query('DROP TABLE IF EXISTS schema_receipt_partial_probe');
  await pool.end();
});
async function stored() {
  return (await pool.query('SELECT value FROM settings WHERE id=$1', [migrationReceiptKey])).rows[0]?.value;
}

it('fails closed without proof and does not install anything at boot', async () => {
  await expect(requireSchemaReady()).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' });
  expect(await stored()).toBeUndefined();
});

it('records the complete real migration and performs boot checking in a read-only transaction', async () => {
  const receipt = await prepareSchema();
  expect(receipt.alreadyApplied).toBe(false);
  expect(receipt.modules).toHaveLength(migrationModules.length);
  expect(receipt.modules.map((module:{path:string})=>module.path)).toContain('server/model-policy-sql.ts');
  const client = await pool.connect();
  try {
    await client.query('BEGIN READ ONLY');
    expect((await requireSchemaReady(client)).fingerprint).toBe(receipt.fingerprint);
    await client.query('ROLLBACK');
  } finally { client.release(); }
  const triggers = (await pool.query(`SELECT tgname FROM pg_trigger WHERE NOT tgisinternal
    AND tgname IN('agentos_instruction_snapshot','agentos_instruction_immutable')`)).rows;
  expect(triggers).toHaveLength(3);
  const indexes = (await pool.query(`SELECT indisvalid,indisready FROM pg_index
    WHERE indexrelid=to_regclass('public.events_run_activity_idx')`)).rows;
  expect(indexes).toEqual([{ indisvalid: true, indisready: true }]);
  let reapplied = false;
  expect((await prepareSchema({ apply: async () => { reapplied = true; } })).alreadyApplied).toBe(true);
  expect(reapplied).toBe(false);
});

it('fingerprints every migration source module and rejects a different source despite an old complete receipt', async () => {
  await prepareSchema();
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-schema-'));
  const actualRoot = fileURLToPath(new URL('../', import.meta.url));
  try {
    for (const name of migrationModules) {
      await mkdir(path.dirname(path.join(root, name)), { recursive: true });
      await writeFile(path.join(root, name), await readFile(path.join(actualRoot, name)));
    }
    const original = await migrationSource();
    expect((await migrationSource(root)).fingerprint).toBe(original.fingerprint);
    for (const name of migrationModules) {
      const bytes = await readFile(path.join(root, name));
      await writeFile(path.join(root, name), Buffer.concat([bytes, Buffer.from('\n// changed migration source\n')]));
      const changed = await migrationSource(root);
      expect(changed.fingerprint).not.toBe(original.fingerprint);
      await expect(requireSchemaReady(pool, changed)).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' });
      await writeFile(path.join(root, name), bytes);
    }
  } finally { await rm(root, { recursive: true, force: true }); }
});

it('checks readiness while another session holds the task table lock that would block boot DDL', async () => {
  await prepareSchema();
  const reader = await pool.connect(), boot = await pool.connect();
  try {
    await reader.query('BEGIN');
    await reader.query('LOCK TABLE tasks IN ACCESS SHARE MODE');
    await boot.query('BEGIN READ ONLY');
    await boot.query("SET LOCAL lock_timeout='100ms'");
    await boot.query("SET LOCAL statement_timeout='1s'");
    await expect(requireSchemaReady(boot)).resolves.toMatchObject({ state: 'complete' });
  } finally {
    await boot.query('ROLLBACK');
    await reader.query('ROLLBACK');
    boot.release(); reader.release();
  }
});

it('serializes concurrent preparers and only applies once', async () => {
  let applied = 0;
  let began!: () => void, release!: () => void;
  const started = new Promise<void>(resolve => { began = resolve; });
  const barrier = new Promise<void>(resolve => { release = resolve; });
  const apply = async () => { applied++; began(); await barrier; await migrate(); };
  const first = prepareSchema({ apply });
  await started;
  const second = prepareSchema({ apply, retryDelaysMs: [20, 100, 250, 500, 1000, 2000] });
  release();
  const results = await Promise.all([first, second]);
  expect(applied).toBe(1);
  expect(results.map(x => x.alreadyApplied).sort()).toEqual([false, true]);
  await expect(requireSchemaReady()).resolves.toMatchObject({ state: 'complete' });
});

it('invalidates the previous receipt before partial DDL and never certifies a failed phase', async () => {
  const original = await prepareSchema();
  const next = { version: 1 as const, fingerprint: 'f'.repeat(64), modules: original.modules };
  await expect(prepareSchema({ source: next, apply: async () => {
    expect((await stored()).state).toBe('applying');
    await expect(requireSchemaReady()).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' });
    await pool.query('CREATE TABLE IF NOT EXISTS schema_receipt_partial_probe(id int)');
    await pool.query("DO $$ BEGIN RAISE EXCEPTION 'failed phase' USING ERRCODE='23514'; END $$");
  } })).rejects.toMatchObject({ code: '23514' });
  expect((await stored()).state).toBe('applying');
  await expect(requireSchemaReady(pool, next)).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' });
  await prepareSchema();
  await expect(requireSchemaReady()).resolves.toMatchObject({ fingerprint: original.fingerprint });
});

it.each(['40P01', '55P03'])('retries only transient PostgreSQL %s outside migration transactions', async code => {
  let attempts = 0;
  await prepareSchema({ retryDelaysMs: [1], apply: async () => {
    attempts++;
    if (attempts === 1)
      await pool.query(`DO $$ BEGIN RAISE EXCEPTION 'transient migration conflict' USING ERRCODE='${code}'; END $$`);
    await migrate();
  } });
  expect(attempts).toBe(2);
  await expect(requireSchemaReady()).resolves.toMatchObject({ state: 'complete' });
});

it('bounds transient retries and releases its session lock after failure', async () => {
  let attempts = 0;
  await expect(prepareSchema({ retryDelaysMs: [1, 1], apply: async () => {
    attempts++;
    await pool.query("DO $$ BEGIN RAISE EXCEPTION 'conflict persists' USING ERRCODE='55P03'; END $$");
  } })).rejects.toMatchObject({ code: '55P03' });
  expect(attempts).toBe(3);
  expect((await stored()).state).toBe('applying');
  const client = await pool.connect();
  try {
    expect((await client.query('SELECT pg_try_advisory_lock($1) owned', [migrationAdvisoryLock])).rows[0].owned).toBe(true);
    await client.query('SELECT pg_advisory_unlock($1)', [migrationAdvisoryLock]);
  } finally { client.release(); }
});

it('does not retry a persistent error or publish a complete receipt', async () => {
  let attempts = 0;
  await expect(prepareSchema({ retryDelaysMs: [1, 1], apply: async () => {
    attempts++;
    await pool.query("DO $$ BEGIN RAISE EXCEPTION 'invalid migration' USING ERRCODE='23514'; END $$");
  } })).rejects.toMatchObject({ code: '23514' });
  expect(attempts).toBe(1);
  expect((await stored()).state).toBe('applying');
});

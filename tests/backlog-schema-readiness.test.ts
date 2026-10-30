import { beforeAll, beforeEach, afterEach, afterAll, it, expect } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { migrationModules, migrationReceiptKey, migrationAdvisoryLock, prepareSchema, requireSchemaReady } from '../server/schema-readiness.js';
import {
  backlogMigrationModules, backlogMigrationReceiptKey, backlogMigrationSource,
  backlogMigrationCompatibility, prepareBacklogSchema, requireBacklogSchemaReady,
} from '../server/backlog-schema-readiness.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const originalLimit = config.scopedPlannerLimit;
beforeAll(async () => { await migrate(); await prepareSchema(); await prepareBacklogSchema(); });
beforeEach(async () => {
  await prepareSchema();
  await pool.query('DELETE FROM settings WHERE id=$1', [backlogMigrationReceiptKey]);
});
afterEach(() => { config.scopedPlannerLimit = originalLimit; });
afterAll(async () => {
  await pool.query('DELETE FROM settings WHERE id=ANY($1::text[])', [[migrationReceiptKey, backlogMigrationReceiptKey]]);
  await pool.query('DROP TABLE IF EXISTS backlog_receipt_partial_probe');
  await pool.end();
});
async function stored(key = backlogMigrationReceiptKey) {
  return (await pool.query('SELECT value FROM settings WHERE id=$1', [key])).rows[0]?.value;
}
async function definitions() {
  return (await pool.query(`SELECT oid::text,xmin::text,proname FROM pg_proc
    WHERE pronamespace='public'::regnamespace AND proname LIKE 'agentos_%' ORDER BY oid`)).rows;
}
async function runBoot() {
  return promisify(execFile)(process.execPath, ['--import', path.join(root, 'node_modules/tsx/dist/loader.mjs'),
    path.join(root, 'scripts/backlog-loop.ts')], { cwd: root, timeout: 10000,
    env: { ...process.env, AGENTOS_BRIDGE_HOST: '', AGENTOS_REMOTE_EXECUTION: '0' } });
}

it('installs all six procedures and nested schemas without changing the seven-module base receipt', async () => {
  const base = await stored(migrationReceiptKey);
  const receipt = await prepareBacklogSchema();
  expect(receipt.alreadyApplied).toBe(false);
  expect(receipt.modules).toHaveLength(backlogMigrationModules.length);
  expect(receipt.installers).toHaveLength(6);
  expect(receipt.parameters).toEqual({ scopedPlannerLimit: originalLimit });
  expect(await stored(migrationReceiptKey)).toEqual(base);
  expect((await pool.query(`SELECT name,to_regclass('public.'||name) present FROM unnest(ARRAY[
    'workforce_backlog_plans','workforce_backlog_items','deferred_handoffs','workspace_preparations',
    'planning_requests','supervision_cases','supervisor_jobs','management_reviews','management_actions',
    'delivery_transitions','delivery_consumptions','goal_followups']) name`)).rows.every(row => row.present)).toBe(true);
  const before = await definitions();
  const repeat = await prepareBacklogSchema({ apply: async () => { throw Error('UNEXPECTED_REINSTALL'); } });
  expect(repeat.alreadyApplied).toBe(true);
  expect(await definitions()).toEqual(before);
});

it.each(['missing', 'partial', 'mismatch', 'tampered-parameters'])('refuses real backlog boot with %s backlog proof before ownership or operational recovery', async mode => {
  if (mode !== 'missing') {
    await prepareBacklogSchema();
    await pool.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1', [backlogMigrationReceiptKey,
      JSON.stringify(mode === 'partial' ? { state: 'applying' } : mode === 'tampered-parameters' ?
        { parameters: { scopedPlannerLimit: 999 } } : { fingerprint: 'f'.repeat(64) })]);
  }
  const before = await definitions();
  const operationalBefore = (await pool.query(`SELECT id,value FROM settings WHERE id IN
    ('planning-engine','supervision-engine','backlog-engine','delivery-engine') ORDER BY id`)).rows;
  await expect(runBoot()).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining('BACKLOG_MIGRATION_REQUIRED') });
  expect(await definitions()).toEqual(before);
  expect((await pool.query(`SELECT id,value FROM settings WHERE id IN
    ('planning-engine','supervision-engine','backlog-engine','delivery-engine') ORDER BY id`)).rows).toEqual(operationalBefore);
  expect((await pool.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
    AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=4280066`)).rowCount).toBe(0);
});

it('checks both proofs read-only while task/case readers would block the former boot DDL', async () => {
  await prepareBacklogSchema();
  const before = await definitions(), reader = await pool.connect(), boot = await pool.connect();
  try {
    await reader.query('BEGIN');
    await reader.query('LOCK TABLE tasks,supervision_cases IN ACCESS SHARE MODE');
    await boot.query('BEGIN READ ONLY');
    await boot.query("SET LOCAL lock_timeout='100ms'");
    await boot.query("SET LOCAL statement_timeout='1s'");
    await expect(requireBacklogSchemaReady(boot)).resolves.toMatchObject({ parameters: { scopedPlannerLimit: originalLimit } });
    expect(await definitions()).toEqual(before);
  } finally {
    await boot.query('ROLLBACK'); await reader.query('ROLLBACK'); boot.release(); reader.release();
  }
});

it('hashes every direct/nested installer, configuration source and new preparer while legacy compatibility needs only existing files', async () => {
  expect(backlogMigrationModules).toEqual(expect.arrayContaining(['server/manager-authority.ts','server/model-policy-sql.ts']));
  expect(migrationModules).toContain('server/model-policy-sql.ts');
  const target = await mkdtemp(path.join(tmpdir(), 'agentos-backlog-schema-'));
  try {
    const files = [...new Set([...migrationModules, ...backlogMigrationModules])];
    for (const name of files) {
      await mkdir(path.dirname(path.join(target, name)), { recursive: true });
      await writeFile(path.join(target, name), await readFile(path.join(root, name)));
    }
    expect(await backlogMigrationCompatibility(target)).toEqual(await backlogMigrationCompatibility());
    await expect(backlogMigrationSource(target)).rejects.toMatchObject({ code: 'ENOENT' });
    // The historical base lacks the new helper. The real CLI must still prove
    // compatibility and prepare both receipts before advertising success.
    const cli = [ '--import', path.join(root, 'node_modules/tsx/dist/loader.mjs'),
      path.join(root, 'scripts/prepare-schema.ts'), '--same-source-as', target ];
    const prepared = await promisify(execFile)(process.execPath, cli, { cwd: root, timeout: 10000 });
    const receipt = JSON.parse(prepared.stdout.trim().split('\n').at(-1)!);
    expect(receipt).toMatchObject({ ok: true, sameSourceAsVerified: true, backlog: { state: 'complete' } });
    const storedBase = await stored(migrationReceiptKey), storedBacklog = await stored();
    const changedPath = path.join(target, 'server/followups.ts'), originalBytes = await readFile(changedPath);
    await writeFile(changedPath, Buffer.concat([originalBytes, Buffer.from('\n// changed base installer\n')]));
    await expect(promisify(execFile)(process.execPath, cli, { cwd: root, timeout: 10000 }))
      .rejects.toMatchObject({ code: 1, stderr: expect.stringContaining('BACKLOG_MIGRATION_COMPATIBILITY_UNPROVEN') });
    expect(await stored(migrationReceiptKey)).toEqual(storedBase);
    expect(await stored()).toEqual(storedBacklog);
    await writeFile(changedPath, originalBytes);
    const implementation = 'server/backlog-schema-readiness.ts';
    await writeFile(path.join(target, implementation), await readFile(path.join(root, implementation)));
    const original = await backlogMigrationSource();
    expect(await backlogMigrationSource(target)).toEqual(original);
    for (const name of [...backlogMigrationModules, implementation]) {
      const bytes = await readFile(path.join(target, name));
      await writeFile(path.join(target, name), Buffer.concat([bytes, Buffer.from('\n// changed installer source\n')]));
      expect((await backlogMigrationSource(target)).fingerprint).not.toBe(original.fingerprint);
      expect((await backlogMigrationCompatibility(target)).compatibilityFingerprint === original.compatibilityFingerprint).toBe(name === implementation);
      await writeFile(path.join(target, name), bytes);
    }
  } finally { await rm(target, { recursive: true, force: true }); }
});

it('binds the effective SQL planner limit and rejects stale config even when file hashes match', async () => {
  await prepareBacklogSchema();
  const before = await backlogMigrationSource();
  config.scopedPlannerLimit = originalLimit === 30 ? 29 : originalLimit + 1;
  const after = await backlogMigrationSource();
  expect(after.modules).toEqual(before.modules);
  expect(after.fingerprint).not.toBe(before.fingerprint);
  await expect(requireBacklogSchemaReady()).rejects.toMatchObject({ code: 'BACKLOG_MIGRATION_REQUIRED' });
  await prepareBacklogSchema();
  expect((await pool.query("SELECT pg_get_functiondef('agentos_scope_planner_limit(uuid)'::regprocedure) definition")).rows[0].definition)
    .toContain(`LEAST(${config.scopedPlannerLimit},`);
  await expect(requireBacklogSchemaReady()).resolves.toMatchObject({ parameters: { scopedPlannerLimit: config.scopedPlannerLimit } });
});

it('invalidates before the first DDL and never certifies a partially failed installation', async () => {
  await prepareBacklogSchema();
  config.scopedPlannerLimit = originalLimit === 30 ? 29 : originalLimit + 1;
  await expect(prepareBacklogSchema({ apply: async () => {
    expect((await stored()).state).toBe('applying');
    await expect(requireBacklogSchemaReady()).rejects.toMatchObject({ code: 'BACKLOG_MIGRATION_REQUIRED' });
    await pool.query('CREATE TABLE IF NOT EXISTS backlog_receipt_partial_probe(id int)');
    throw Object.assign(Error('failed phase'), { code: '23514' });
  } })).rejects.toMatchObject({ code: '23514' });
  expect((await stored()).state).toBe('applying');
  await expect(requireSchemaReady()).resolves.toMatchObject({ state: 'complete' });
  config.scopedPlannerLimit = originalLimit;
  await prepareBacklogSchema();
  await expect(requireBacklogSchemaReady()).resolves.toMatchObject({ version: 1 });
});

it.each(['configuration', 'base-receipt'])('revalidates %s after applying and cannot publish completion for drift', async mode => {
  await expect(prepareBacklogSchema({ apply: async () => {
    if (mode === 'configuration') config.scopedPlannerLimit = originalLimit === 30 ? 29 : originalLimit + 1;
    else await pool.query("UPDATE settings SET value=value||'{\"state\":\"applying\"}'::jsonb WHERE id=$1", [migrationReceiptKey]);
  } })).rejects.toMatchObject({ code: mode === 'configuration' ? 'BACKLOG_MIGRATION_SOURCE_CHANGED' : 'MIGRATION_REQUIRED' });
  expect((await stored()).state).toBe('applying');
});

it('shares the base preparation lock without nesting and leaves both proofs intact when another preparer owns it', async () => {
  const owner = await pool.connect();
  try {
    await owner.query('SELECT pg_advisory_lock($1)', [migrationAdvisoryLock]);
    let called = false;
    await expect(prepareBacklogSchema({ retryDelaysMs: [], apply: async () => { called = true; } }))
      .rejects.toMatchObject({ code: '55P03' });
    expect(called).toBe(false); expect(await stored()).toBeUndefined();
    await expect(requireSchemaReady()).resolves.toMatchObject({ state: 'complete' });
  } finally { await owner.query('SELECT pg_advisory_unlock($1)', [migrationAdvisoryLock]); owner.release(); }
  await expect(prepareBacklogSchema()).resolves.toMatchObject({ alreadyApplied: false });
});

it('serializes concurrent backlog preparation and performs only one installation', async () => {
  let began!: () => void, release!: () => void, applied = 0;
  const started = new Promise<void>(resolve => { began = resolve; });
  const barrier = new Promise<void>(resolve => { release = resolve; });
  const apply = async () => { applied++; began(); await barrier; };
  const first = prepareBacklogSchema({ apply }); await started;
  const second = prepareBacklogSchema({ apply, retryDelaysMs: [20, 100, 250, 500, 1000, 2000] }); release();
  const results = await Promise.all([first, second]);
  expect(applied).toBe(1);
  expect(results.map(result => result.alreadyApplied).sort()).toEqual([false, true]);
});

it.each(['40P01', '55P03', '23514'])('bounds and classifies preparation retries for %s, then releases its owner', async code => {
  let applied = 0;
  await expect(prepareBacklogSchema({ retryDelaysMs: [1, 1], apply: async () => {
    applied++; throw Object.assign(Error('phase failure'), { code });
  } })).rejects.toMatchObject({ code });
  expect(applied).toBe(code === '23514' ? 1 : 3);
  expect((await stored()).state).toBe('applying');
  const owner = await pool.connect();
  try {
    expect((await owner.query('SELECT pg_try_advisory_lock($1) owned', [migrationAdvisoryLock])).rows[0].owned).toBe(true);
    await owner.query('SELECT pg_advisory_unlock($1)', [migrationAdvisoryLock]);
  } finally { owner.release(); }
});

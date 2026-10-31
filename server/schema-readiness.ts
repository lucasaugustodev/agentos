import { createHash } from 'node:crypto';
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { pool, migrate, type DB } from './db.js';

// This is the complete source closure of migrate(), not an application build hash.
export const migrationModules = [
  'server/db.ts',
  'server/workstream-policy.ts',
  'server/workforce-policy.ts',
  'server/model-policy-sql.ts',
  'server/artifact-recovery-policy.ts',
  'server/cycles/store.ts',
  'server/task-instructions.ts',
  'server/task-retry-guidance-schema.ts',
  'server/runtime-lookup-indexes.ts',
  'server/objective-model-route-schema.ts',
] as const;
export const migrationReceiptKey = 'schema:migration-active';
export const migrationAdvisoryLock = 4280101;
const sourceRoot = fileURLToPath(new URL('../', import.meta.url));
const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
export type MigrationSource = {
  version: 1;
  fingerprint: string;
  modules: Array<{ path: string; sha256: string }>;
};
function failure(code: string) {
  return Object.assign(new Error(code), { code });
}

export async function migrationSource(root = sourceRoot): Promise<MigrationSource> {
  const resolved = await realpath(root);
  const modules = [];
  for (const name of migrationModules) {
    const file = await realpath(path.join(resolved, name));
    if (path.relative(resolved, file).startsWith('..') || path.isAbsolute(path.relative(resolved, file)))
      throw failure('MIGRATION_SOURCE_PATH');
    modules.push({ path: name, sha256: sha(await readFile(file)) });
  }
  return { version: 1, fingerprint: sha(JSON.stringify({ version: 1, modules })), modules };
}

async function readReceipt(db: DB) {
  if (!(await db.query("SELECT to_regclass('public.settings') present")).rows[0].present)
    return null;
  return (await db.query('SELECT value FROM public.settings WHERE id=$1', [migrationReceiptKey])).rows[0]?.value;
}
function complete(receipt: any, source: MigrationSource) {
  return receipt?.version === 1 && receipt.state === 'complete' &&
    receipt.fingerprint === source.fingerprint &&
    Array.isArray(receipt.modules) && receipt.modules.length === source.modules.length &&
    source.modules.every((item, index) => receipt.modules[index]?.path === item.path &&
      receipt.modules[index]?.sha256 === item.sha256) &&
    Number.isFinite(Date.parse(receipt.completedAt));
}

/** Boot is read-only. Missing or partial proof never starts DDL or weakens guards. */
export async function requireSchemaReady(db: DB = pool, source?: MigrationSource) {
  const expected = source ?? await migrationSource();
  const receipt = await readReceipt(db);
  if (!complete(receipt, expected)) throw failure('MIGRATION_REQUIRED');
  return receipt;
}

/** One explicit installer owns all phases; the scheduler lock remains scoped to
 * migrate()'s original transaction. Trigger installation and concurrent indexes
 * retain their existing, intentionally separate transaction boundaries. */
export async function prepareSchema(options: {
  source?: MigrationSource;
  apply?: () => Promise<void>;
  retryDelaysMs?: readonly number[];
} = {}) {
  const source = options.source ?? await migrationSource();
  const apply = options.apply ?? migrate;
  const retryDelays = options.retryDelaysMs ?? [250, 1000, 2500, 5000];
  for (let attempt = 0; ; attempt++) {
    const owner = await pool.connect();
    let locked = false;
    try {
      locked = (await owner.query('SELECT pg_try_advisory_lock($1) owned', [migrationAdvisoryLock])).rows[0].owned;
      if (!locked) throw failure('55P03');
      const prior = await readReceipt(owner);
      if (complete(prior, source)) return { ...prior, alreadyApplied: true };
      // Invalidate the previous active receipt BEFORE any possibly partial DDL.
      // A fresh empty database has no settings table until migrate() creates it.
      if ((await owner.query("SELECT to_regclass('public.settings') present")).rows[0].present)
        await owner.query(`INSERT INTO public.settings(id,value) VALUES($1,$2)
          ON CONFLICT(id) DO UPDATE SET value=excluded.value`, [migrationReceiptKey,
          JSON.stringify({ ...source, state: 'applying', startedAt: new Date().toISOString() })]);
      await apply();
      // Source changes while applying cannot receive a valid completion receipt.
      if (!options.source && (await migrationSource()).fingerprint !== source.fingerprint)
        throw failure('MIGRATION_SOURCE_CHANGED');
      const receipt = { ...source, state: 'complete', completedAt: new Date().toISOString() };
      await owner.query(`INSERT INTO public.settings(id,value) VALUES($1,$2)
        ON CONFLICT(id) DO UPDATE SET value=excluded.value`, [migrationReceiptKey, JSON.stringify(receipt)]);
      return { ...receipt, alreadyApplied: false };
    } catch (error: any) {
      if (!['40P01', '55P03'].includes(error?.code) || attempt >= retryDelays.length) throw error;
    } finally {
      if (locked) await owner.query('SELECT pg_advisory_unlock($1)', [migrationAdvisoryLock]).catch(() => {});
      owner.release();
    }
    // No transaction or advisory lock is retained during backoff.
    await delay(retryDelays[attempt]);
  }
}

import { createHash } from 'node:crypto';
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { pool, type DB } from './db.js';
import { config } from './config.js';
import { installBacklogPlanner } from './backlog.js';
import { installPlanningDemand } from './planning-demand.js';
import { installPlanningEvents } from './planning-events.js';
import { installSupervision } from './supervision.js';
import { installManagement } from './management.js';
import { installFollowups } from './followups.js';
import { migrationSource, migrationAdvisoryLock, requireSchemaReady } from './schema-readiness.js';
import { installTaskExecutionMetadata, requireTaskExecutionMetadataReady } from './task-execution-metadata.js';

// Source closure of the six installer procedures (including nested installers),
// plus the parser of the one configuration value interpolated into their SQL.
export const backlogMigrationModules = [
  'server/backlog.ts', 'server/deferred-handoffs.ts',
  'server/delivery-consumption.ts', 'server/delivery-supervision.ts', 'server/supervision-generations.ts',
  'server/planning-demand.ts', 'server/workspace-preparation.ts',
  'server/planning-events.ts', 'server/supervision.ts',
  'server/manager-context-section-store.ts', 'server/manager-context-sections.ts',
  'server/management.ts', 'server/delivery-transitions.ts',
  'server/manager-authority.ts', 'server/model-policy-sql.ts',
    'server/candidate-validation.ts', 'server/candidate-validation-evidence.ts', 'server/published-rebase.ts',
  'server/verification-source-binding.ts', 'scripts/agentos-verification-source-binding.py',
  'server/followups.ts', 'server/config.ts',
  'server/task-execution-metadata.ts',
  'server/migration-table-locks.ts',
] as const;
const installerNames = ['backlog', 'planning-demand', 'planning-events', 'supervision', 'management', 'followups'];
const implementationModule = 'server/backlog-schema-readiness.ts';
export const backlogMigrationReceiptKey = 'schema:backlog-active';
const sourceRoot = fileURLToPath(new URL('../', import.meta.url));
const sha = (bytes: string | Buffer) => createHash('sha256').update(bytes).digest('hex');
const failure = (code: string) => Object.assign(new Error(code), { code });

/** Compatibility compares existing installer sources under the preparer's
 * effective environment. It does not assert the environment of an old process. */
export async function backlogMigrationCompatibility(root = sourceRoot) {
  const resolved = await realpath(root);
  const modules = [];
  for (const name of backlogMigrationModules) {
    const file = await realpath(path.join(resolved, name));
    const relative = path.relative(resolved, file);
    if (relative.startsWith('..') || path.isAbsolute(relative)) throw failure('BACKLOG_MIGRATION_SOURCE_PATH');
    modules.push({ path: name, sha256: sha(await readFile(file)) });
  }
  const parameters = { scopedPlannerLimit: config.scopedPlannerLimit };
  const content = { version: 1 as const, modules, parameters, installers: installerNames };
  return { ...content, compatibilityFingerprint: sha(JSON.stringify(content)) };
}

export async function backlogMigrationSource(root = sourceRoot) {
  const compatible = await backlogMigrationCompatibility(root);
  const base = await migrationSource(root);
  const resolved = await realpath(root), file = await realpath(path.join(resolved, implementationModule));
  const relative = path.relative(resolved, file);
  if (relative.startsWith('..') || path.isAbsolute(relative)) throw failure('BACKLOG_MIGRATION_SOURCE_PATH');
  const content = { ...compatible, baseFingerprint: base.fingerprint,
    implementation: { path: implementationModule, sha256: sha(await readFile(file)) } };
  return { ...content, fingerprint: sha(JSON.stringify(content)) };
}
type BacklogMigrationSource = Awaited<ReturnType<typeof backlogMigrationSource>>;

async function readReceipt(db: DB) {
  return (await db.query('SELECT value FROM public.settings WHERE id=$1', [backlogMigrationReceiptKey])).rows[0]?.value;
}
function complete(receipt: any, source: BacklogMigrationSource) {
  if (receipt?.state !== 'complete' || !Number.isFinite(Date.parse(receipt.completedAt))) return false;
  // Compare every bound field, not merely a caller-supplied fingerprint string.
  return Object.entries(source).every(([key, value]) => isDeepStrictEqual(receipt[key], value));
}

/** Both receipts are read-only guards, before any controller ownership or work. */
export async function requireBacklogSchemaReady(db: DB = pool) {
  const base = await requireSchemaReady(db);
  const source = await backlogMigrationSource();
  if (source.baseFingerprint !== base.fingerprint || !complete(await readReceipt(db), source))
    throw failure('BACKLOG_MIGRATION_REQUIRED');
  await requireTaskExecutionMetadataReady(db);
  return source;
}

async function applyBacklogSchema() {
  // Preserve each installer's transaction boundaries and dependency order.
  await installTaskExecutionMetadata();
  await installBacklogPlanner();
  await installPlanningDemand();
  await installPlanningEvents();
  await installSupervision();
  await installManagement();
  await installFollowups();
}

/** Explicit preparation only. Uses the SAME session lock as the base preparer,
 * never calls prepareSchema while holding it, and never acquires the scheduler
 * advisory lock. Ordinary PostgreSQL relation locks retain the CLI's timeouts. */
export async function prepareBacklogSchema(options: {
  apply?: () => Promise<void>;
  retryDelaysMs?: readonly number[];
} = {}) {
  const source = await backlogMigrationSource();
  const retryDelays = options.retryDelaysMs ?? [250, 1000, 2500, 5000];
  for (let attempt = 0; ; attempt++) {
    const owner = await pool.connect();
    let locked = false;
    try {
      locked = (await owner.query('SELECT pg_try_advisory_lock($1) owned', [migrationAdvisoryLock])).rows[0].owned;
      if (!locked) throw failure('55P03');
      const base = await requireSchemaReady(owner);
      if (base.fingerprint !== source.baseFingerprint ||
        (await backlogMigrationSource()).fingerprint !== source.fingerprint)
        throw failure('BACKLOG_MIGRATION_SOURCE_CHANGED');
      const prior = await readReceipt(owner);
      if (complete(prior, source)) return { ...prior, alreadyApplied: true };
      await owner.query(`INSERT INTO public.settings(id,value) VALUES($1,$2)
        ON CONFLICT(id) DO UPDATE SET value=excluded.value`, [backlogMigrationReceiptKey,
        JSON.stringify({ ...source, state: 'applying', startedAt: new Date().toISOString() })]);
      await (options.apply ?? applyBacklogSchema)();
      await requireSchemaReady(owner);
      if ((await backlogMigrationSource()).fingerprint !== source.fingerprint)
        throw failure('BACKLOG_MIGRATION_SOURCE_CHANGED');
      const receipt = { ...source, state: 'complete', completedAt: new Date().toISOString() };
      await owner.query(`INSERT INTO public.settings(id,value) VALUES($1,$2)
        ON CONFLICT(id) DO UPDATE SET value=excluded.value`, [backlogMigrationReceiptKey, JSON.stringify(receipt)]);
      return { ...receipt, alreadyApplied: false };
    } catch (error: any) {
      if (!['40P01', '55P03'].includes(error?.code) || attempt >= retryDelays.length) throw error;
    } finally {
      if (locked) await owner.query('SELECT pg_advisory_unlock($1)', [migrationAdvisoryLock]).catch(() => {});
      owner.release();
    }
    await delay(retryDelays[attempt]);
  }
}

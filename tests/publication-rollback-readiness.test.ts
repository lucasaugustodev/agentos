import { beforeAll, afterAll, expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { pool } from '../server/db.js';
import { prepareSchema } from '../server/schema-readiness.js';
import { prepareBacklogSchema } from '../server/backlog-schema-readiness.js';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { installRunCriteriaMetadata } from '../server/run-criteria-metadata.js';
import { installTaskPanelMetadata } from '../server/task-panel-metadata.js';

const exec = promisify(execFile);
const keys = ['schema:migration-active', 'schema:backlog-active',
  'schema:task-execution-metadata', 'schema:run-criteria-metadata', 'schema:task-panel-metadata'];
let script: string;
async function receipts() {
  return (await pool.query('SELECT id,value FROM settings WHERE id=ANY($1) ORDER BY id', [keys])).rows;
}
async function inspect() {
  return exec(process.execPath, ['--import', path.resolve('node_modules/tsx/dist/loader.mjs'),
    '--input-type=module', '-e', script], {
    cwd: process.cwd(), timeout: 40_000, maxBuffer: 1024 * 1024,
    env: { PATH: process.env.PATH, DATABASE_URL: process.env.DATABASE_URL,
      PGOPTIONS: '-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000',
      HOME: '/nonexistent', NODE_OPTIONS: '', NODE_PATH: '' },
  });
}
beforeAll(async () => {
  await prepareSchema(); await installRunCriteriaMetadata(); await installTaskExecutionMetadata();
  await prepareBacklogSchema(); await installTaskPanelMetadata();
  script = (await exec('python3', ['-B', 'tests/fixtures/publication_rollback_test.py', '--readiness-js'])).stdout;
});
afterAll(async () => { await pool.end(); });

it('checks all five gates from actual source in one read-only transaction without changing receipts', async () => {
  const before = await receipts(); expect(before).toHaveLength(5);
  const result = await inspect(); expect(result.stderr).toBe('');
  expect(JSON.parse(result.stdout)).toEqual({ version: 1, ok: true, readOnly: true,
    gates: ['base', 'backlog', 'taskExecution', 'runCriteria', 'panel'] });
  expect(await receipts()).toEqual(before);
});

it.each(keys)('refuses missing/partial readiness for %s without running a repair', async (key) => {
  const original = (await pool.query('SELECT value FROM settings WHERE id=$1', [key])).rows[0].value;
  try {
    await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [key, JSON.stringify({ ...original, state: 'applying' })]);
    const before = await receipts();
    await expect(inspect()).rejects.toMatchObject({ code: 1, stderr: 'ROLLBACK_SCHEMA_NOT_READY\n' });
    expect(await receipts()).toEqual(before);
  } finally { await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [key, JSON.stringify(original)]); }
});

it('qualifies pointer preservation and lost reply behavior with the real Python guard', async () => {
  const result = await exec('python3', ['-B', 'tests/fixtures/publication_rollback_test.py']);
  expect(result.stdout).toBe(''); expect(result.stderr).toContain('OK');
});

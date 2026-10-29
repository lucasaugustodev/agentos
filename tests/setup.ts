process.env.AGENTOS_GLM_ONLY = '0';
process.env.AGENTOS_MODEL_POLICY_FILE = '/nonexistent/agentos-isolated-test-model-policy.json';
import 'dotenv/config';
import pg from 'pg';
import path from 'node:path';
import { realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';

// The product requires canonical storage roots. macOS points TMPDIR at /var,
// itself a symlink to /private/var, so every mkdtemp root would look like a
// path escape. Hand the suites a canonical temporary directory instead of
// weakening the guard that rejects one.
process.env.TMPDIR = await realpath(tmpdir());
const url = new URL(process.env.AGENTOS_TEST_DATABASE_URL ?? process.env.DATABASE_URL!);
if (!['127.0.0.1', 'localhost'].includes(url.hostname) || url.port !== '15482')
  throw Error('Tests require the isolated local PostgreSQL container.');
if (!process.env.AGENTOS_TEST_DATABASE_URL) {
  url.pathname = '/agentos';
  const admin = new pg.Pool({ connectionString: url.toString() });
  if (!(await admin.query("SELECT 1 FROM pg_database WHERE datname='agentos_test'")).rowCount)
    await admin.query('CREATE DATABASE agentos_test');
  await admin.end();
}
url.pathname = '/agentos_test';
process.env.DATABASE_URL = url.toString();
process.env.AGENTOS_DATA_DIR = path.resolve('.local-test');
process.env.AGENTOS_LAYA_URL = '';
process.env.AGENTOS_RECALL_URL = '';
process.env.AGENTOS_RECALL_KEY = '';
process.env.AGENTOS_DSH_COMMAND = '';
process.env.AGENTOS_DSH_ARGS = '[]';
process.env.AGENTOS_BRIDGE_HOST = '';
process.env.AGENTOS_REMOTE_EXECUTION = '0';

// Explicit test-database preparation, after the isolated URL and provider
// settings above. A fresh canonical test run must not inherit a focal receipt.
// Subsequent suites take the installers' read-only completed fast paths.
const { beforeAll } = await import('vitest');
beforeAll(async () => {
  const { prepareSchema } = await import('../server/schema-readiness.js');
  const { installRunCriteriaMetadata } = await import('../server/run-criteria-metadata.js');
  await prepareSchema();
  await installRunCriteriaMetadata();
}, 60000);

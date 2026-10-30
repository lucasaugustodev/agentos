import { expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';

it('binds verified preparation and replay to source bytes, generation and destination without changing legacy callers', async () => {
  const { stdout, stderr } = await promisify(execFile)('python3', [
    '-B', path.resolve('tests/fixtures/workspace_prepare_binding_test.py'),
  ], { timeout: 60_000, maxBuffer: 1024 * 1024 });
  expect(stdout).toBe('');
  expect(stderr).toContain('Ran 34 tests');
  expect(stderr).toContain('OK');
}, 65_000);

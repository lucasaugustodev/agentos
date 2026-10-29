import { expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';

// This suite belongs to the isolated Linux validation profile: input fixtures
// and the trusted fake toolchain must be root-owned and read-only there.
it('preserves the validation execution boundary and physical stop requirement', async () => {
  const { stdout, stderr } = await promisify(execFile)(
    'python3',
    ['-B', path.resolve('tests/fixtures/validator_execution_boundary_test.py')],
    { timeout: 30_000, maxBuffer: 1024 * 1024 },
  );
  expect(stdout).toBe('');
  expect(stderr).toContain('OK');
  expect(stderr).toMatch(/Ran \d+ tests/);
});

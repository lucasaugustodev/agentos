import { expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';

it.each([
  'validator_provenance_test.py',
  'candidate_validation_test.py',
  'candidate_validation_evidence_test.py',
])(
  'verifies privileged validation contracts in an isolated tree: %s',
  async (fixture) => {
    const { stdout, stderr } = await promisify(execFile)(
      'python3',
      ['-B', path.resolve('tests/fixtures', fixture)],
      { timeout: 30_000, maxBuffer: 1024 * 1024 },
    );
    expect(stdout).toBe('');
    expect(stderr).toContain('OK');
    expect(stderr).toMatch(/Ran \d+ tests/);
  },
);

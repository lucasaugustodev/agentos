import {expect, it} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';

it('binds publish eligibility to real controller preparation rather than candidate claims', async () => {
  const {stdout, stderr} = await promisify(execFile)('python3',
    ['-B', path.resolve('tests/fixtures/candidate_origin_test.py')],
    {timeout:30_000,maxBuffer:1024*1024});
  expect(stdout).toBe('');
  expect(stderr).toContain('OK');
  expect(stderr).toContain('Ran 28 tests');
});

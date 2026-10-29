import {expect,it} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';

it('does not inherit the current release through a reused ancestor workspace name',async()=>{
  const {stdout,stderr}=await promisify(execFile)('python3',
    ['-B',path.resolve('tests/fixtures/candidate_origin_parent_generation_test.py')],
    {timeout:30000,maxBuffer:1024*1024});
  expect(stdout).toBe('');expect(stderr).toContain('Ran 1 test');expect(stderr).toContain('OK');
});

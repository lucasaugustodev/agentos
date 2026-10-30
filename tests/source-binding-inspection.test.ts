import {expect,it} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';

it('reads only protected admission metadata and refuses ambiguous or changing source generations',async()=>{
  const {stdout,stderr}=await promisify(execFile)('python3',[
    '-B',path.resolve('tests/fixtures/source_binding_inspection_test.py'),
  ],{timeout:60_000,maxBuffer:1024*1024});
  expect(stdout).toBe('');
  expect(stderr).toContain('Ran 23 tests');
  expect(stderr).toContain('OK');
  const stopped=await promisify(execFile)(process.execPath,[
    '--test','--test-reporter=tap',path.resolve('tests/source-binding-stop.test.mjs'),
  ],{timeout:30_000,maxBuffer:1024*1024});
  expect(stopped.stdout).toContain('# tests 8');
  expect(stopped.stdout).toContain('# pass 8');
  expect(stopped.stdout).toContain('# fail 0');
  expect(stopped.stdout).toContain('# skipped 0');
},65_000);

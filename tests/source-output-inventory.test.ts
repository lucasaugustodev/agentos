import {expect,it} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
it('inventories the actual promoted source and protected baseline, excluding evidence-only code',async()=>{
  const {stdout,stderr}=await promisify(execFile)('python3',['-B',path.resolve('tests/fixtures/source_output_inventory_test.py')],{timeout:30_000});
  expect(stdout).toBe('');expect(stderr).toContain('Ran 5 tests');expect(stderr).toContain('OK');
});
it('preserves the source inventory contract through stopped artifact and plan recovery',async()=>{
  const {stdout}=await promisify(execFile)(process.execPath,['--test','--test-reporter=tap',
    path.resolve('tests/delivery-manifest.test.mjs'),path.resolve('tests/artifact-collector.test.mjs')],{timeout:40_000,maxBuffer:2*1024*1024});
  expect(stdout).toContain('# fail 0');expect(stdout).toContain('# skipped 0');
  expect(stdout).toContain('recovery retains the opted-in baseline inventory required by source acceptance');
},45_000);

import {expect,it} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
it('inventories the actual promoted source and protected baseline, excluding evidence-only code',async()=>{
  const {stdout,stderr}=await promisify(execFile)('python3',['-B',path.resolve('tests/fixtures/source_output_inventory_test.py')],{timeout:30_000});
  expect(stdout).toBe('');expect(stderr).toContain('Ran 5 tests');expect(stderr).toContain('OK');
});

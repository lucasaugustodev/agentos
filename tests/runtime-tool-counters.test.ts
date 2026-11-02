import {expect,it} from 'vitest';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

// Canonical CI and npm test collect *.test.ts. Keep the real runner fixture in
// its own Node process so import hooks cannot affect other Vitest suites.
it.each([
  ['runtime-tool-counters.test.mjs',16],
  ['runtime-rate-limit-retry.test.mjs',12],
] as const)('runs actual harness integration %s through the standard Node test runner without experimental flags',(file,count)=>{
  const result=spawnSync(process.execPath,['--test','--test-reporter=tap',
    fileURLToPath(new URL('./'+file,import.meta.url))],{
    cwd:fileURLToPath(new URL('../',import.meta.url)),encoding:'utf8',timeout:15000,maxBuffer:1024*1024,
    env:{PATH:process.env.PATH??'',HOME:'/nonexistent',LANG:'C.UTF-8'},
  });
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  expect(result.status,result.stdout+'\n'+result.stderr).toBe(0);
  for(const line of [`# tests ${count}`,`# pass ${count}`,'# fail 0','# cancelled 0','# skipped 0','# todo 0'])
    expect(result.stdout.split(/\r?\n/)).toContain(line);
});

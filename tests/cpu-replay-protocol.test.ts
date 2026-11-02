import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { expect, it } from 'vitest';

it('runs the CPU executor protocol guards in canonical CI without executing a checker or starting a service', () => {
  const output = execFileSync('/usr/bin/python3', ['-B', path.resolve('tests/test_cpu_replay.py'), '--protocol-json'], {
    encoding: 'utf8', timeout: 15000, maxBuffer: 65536,
    env: { PATH: '/usr/bin:/bin', LANG: 'C.UTF-8' },
  });
  const result = JSON.parse(output);
  expect(result).toMatchObject({ failures: 0, errors: 0, skipped: 0, successful: true });
  expect(result.testsRun).toBe(result.tests.length);
  expect(new Set(result.tests).size).toBe(result.testsRun);
  expect(result.tests).toEqual(expect.arrayContaining([
    'test_both_fixed_profiles', 'test_no_shell_or_extra_input', 'test_pin_and_scope',
    'test_artifact_tamper', 'test_base_cannot_be_rebound',
    'test_lost_receipt_is_uncertain_never_rerun', 'test_new_fence_does_not_adopt_old_intent',
    'test_completed_inspection_does_not_dispatch',
  ]));
});

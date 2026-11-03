import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { expect, it } from 'vitest';

it('executes all ten DeepSeek wire cases including response-ceiling and partial-tool guards', () => {
  const output = execFileSync(process.execPath, ['--test', '--test-reporter=tap',
    path.resolve('tests/deepseek-profile-wire.test.mjs')], {
    encoding: 'utf8', timeout: 20000, maxBuffer: 262144,
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin', LANG: 'C.UTF-8', NO_COLOR: '1',
      AGENTOS_MODEL_POLICY_FILE: '/tmp/agentos-node-protocol-no-policy.json' },
  });
  for (const [name, value] of Object.entries({ tests: 10, pass: 10, fail: 0, cancelled: 0, skipped: 0, todo: 0 })) {
    expect(output.match(new RegExp(`^# ${name} (\\d+)$`, 'gm'))).toEqual([`# ${name} ${value}`]);
  }
  const names = [...output.matchAll(/^ok \d+ - (.+)$/gm)].map(match => match[1]);
  expect(names).toEqual([
    'v2 selects exactly one operator-owned API pair; model names grant no profile',
    'DeepSeek actual serialized wire low and telemetry use its thinking contract',
    'DeepSeek actual serialized wire high and telemetry use its thinking contract',
    'DeepSeek actual serialized wire xhigh and telemetry use its thinking contract',
    'explicit disabled thinking preserves the requested budget and omits effort and Z.ai options',
    'all assistant reasoning survives both directions and subsequent tool turns byte-for-byte',
    'the larger DeepSeek output ceiling leaves legacy GLM and unverified routes unchanged',
    'a length-terminated tool call remains rejected even when its arguments are valid',
    'explicit empty reasoning is preserved and absence is never fabricated',
    'mixed valid calls share six permits and cancellation retains an owned permit until body unwinds',
  ]);
}, 30000);

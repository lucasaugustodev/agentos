import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
// @ts-expect-error Runtime ESM has no generated declarations.
import { modelRetryDelay, isCompactionRequest, harnessCallBudgetMs, providerContentBlock, failedCallDiagnostic, MODEL_RETRY_DELAYS_MS, MODEL_TIMEOUT_RETRIES, HARNESS_CALL_BUDGET_MS, COMPACTION_BUDGET_MS } from '../bridge/model-retry.mjs';
import { SOURCE_BINARY_CHECK, sourcePatchInstruction } from '../bridge/source-patch-contract.mjs';
// @ts-expect-error Runtime ESM has no generated declarations.
import { prepareSandboxIdentity, SANDBOX_UID } from '../bridge/sandbox-identity.mjs';
import { repairInstruction } from '../server/continuation.js';

const timeout = () => new DOMException('Provider request timed out', 'TimeoutError');

describe('model call retry inside an attempt', () => {
  it('repeats a provider transport failure with growing delays, then gives up', () => {
    const failure = new Error('500 Internal Server Error');
    expect(MODEL_RETRY_DELAYS_MS.map((_: number, retry: number) => modelRetryDelay(failure, retry))).toEqual(MODEL_RETRY_DELAYS_MS);
    expect(modelRetryDelay(failure, MODEL_RETRY_DELAYS_MS.length)).toBeNull();
  });

  it('repeats a timed-out call fewer times, since each one already spent the whole budget', () => {
    for (let retry = 0; retry < MODEL_TIMEOUT_RETRIES; retry++) expect(modelRetryDelay(timeout(), retry)).toBeGreaterThan(0);
    expect(modelRetryDelay(timeout(), MODEL_TIMEOUT_RETRIES)).toBeNull();
    expect(modelRetryDelay(timeout(), 0, { compaction: true })).toBeNull();
    expect(modelRetryDelay(new Error('terminated'), 0, { compaction: true })).toBe(MODEL_RETRY_DELAYS_MS[0]);
  });

  it('keeps budget and timeout repeats inside the controller inactivity window', () => {
    const window = 60 * 60 * 1000;
    const delays = MODEL_RETRY_DELAYS_MS.slice(0, MODEL_TIMEOUT_RETRIES).reduce((a: number, b: number) => a + b, 0);
    expect(HARNESS_CALL_BUDGET_MS * (1 + MODEL_TIMEOUT_RETRIES) + delays).toBeLessThan(window);
    expect(COMPACTION_BUDGET_MS).toBeLessThan(window);
    expect(MODEL_RETRY_DELAYS_MS.reduce((a: number, b: number) => a + b, 0)).toBeLessThan(window / 2);
    expect(harnessCallBudgetMs(720_000, false)).toBe(HARNESS_CALL_BUDGET_MS);
    expect(harnessCallBudgetMs(1_500_000, false)).toBe(1_500_000);
    expect(harnessCallBudgetMs(720_000, true)).toBe(COMPACTION_BUDGET_MS);
  });

  it('never repeats decisions: policy, auth, quota, size or cancellation', () => {
    expect(modelRetryDelay(new Error('MODEL_NOT_AVAILABLE'), 0)).toBeNull();
    expect(modelRetryDelay(Object.assign(new Error('gate'), { code: 'GLM_GATE_UNAVAILABLE' }), 0)).toBeNull();
    expect(modelRetryDelay(new Error('429 Too Many Requests'), 0)).toBeNull();
    expect(modelRetryDelay(new Error('401 invalid credential'), 0)).toBeNull();
    expect(modelRetryDelay(new Error('socket hang up'), 0, { aborted: true })).toBeNull();
    expect(modelRetryDelay(timeout(), 0, { aborted: true })).toBeNull();
  });
});

describe('router content refusal', () => {
  const refused = new Error('502 "upstream unavailable"');
  const named = [{ role: 'toolResult', toolCallId: 'a', content: [{ type: 'text', text: 'initdb: no /etc/passwd entry for uid 0' }] }];
  it('fails at once when the session already carries text the router refuses', () => {
    expect(providerContentBlock(refused, named)).toBe(true);
    expect(modelRetryDelay(refused, 0, { messages: named })).toBeNull();
    expect(failedCallDiagnostic(refused, named).providerContentBlocked).toBe(true);
  });
  it('still rides out the same status when nothing in the session explains it', () => {
    expect(modelRetryDelay(refused, 0, { messages: [{ role: 'user', content: 'build the api' }] })).toBe(MODEL_RETRY_DELAYS_MS[0]);
    expect(providerContentBlock(new Error('terminated'), named)).toBe(false);
  });
});

describe('sandbox identity', () => {
  it('writes passwd and group outside the home and switches to an ordinary uid', async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'agentos-identity-'));
    try {
      const args = await prepareSandboxIdentity(dir);
      expect(args.slice(0, 5)).toEqual(['--unshare-user', '--uid', String(SANDBOX_UID), '--gid', String(SANDBOX_UID)]);
      expect(args).toContain('/etc/passwd');
      expect(readFileSync(path.join(dir, 'passwd'), 'utf8')).toContain(`agent:x:${SANDBOX_UID}:${SANDBOX_UID}:`);
      await expect(prepareSandboxIdentity(dir)).rejects.toMatchObject({ code: 'EEXIST' });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('compaction request detection', () => {
  const directive = 'You are now acting as a compaction engine for this AI coding assistant. Condense...';
  it('recognizes the directive as the final user message in both wire shapes', () => {
    expect(isCompactionRequest({ messages: [{ role: 'user', content: 'task' }, { role: 'user', content: [{ type: 'text', text: directive }] }] })).toBe(true);
    expect(isCompactionRequest({ messages: [{ role: 'system', content: 's' }, { role: 'user', content: directive }] })).toBe(true);
  });

  it('does not mark later ordinary calls that carry an earlier checkpoint', () => {
    const checkpoint = { role: 'user', content: [{ type: 'text', text: '<compacted-summary>\n## Primary Request\n</compacted-summary>' }] };
    expect(isCompactionRequest({ messages: [checkpoint, { role: 'assistant', content: 'ok' }, { role: 'user', content: 'tool result' }] })).toBe(false);
    expect(isCompactionRequest({ messages: [{ role: 'user', content: directive }, { role: 'assistant', content: 'summary' }] })).toBe(false);
    expect(isCompactionRequest({})).toBe(false);
  });
});

describe('source patch binary rule', () => {
  // The collector diffs base and candidate as text: added, changed and deleted
  // binaries all reject the delivery, untouched ones and data/ do not.
  it('lists exactly the binary changes the collector rejects, relative to the snapshot base', () => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'agentos-binary-'));
    const png = Buffer.from('89504e470d0a1a0a0000000d49484452', 'hex');
    const put = (rel: string, bytes: Buffer | string) => {
      mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
      writeFileSync(path.join(root, rel), bytes);
    };
    const git = (...args: string[]) => execFileSync('git', ['-C', root, '-c', 'user.name=t', '-c', 'user.email=t@t', ...args]);
    try {
      put('uploads/kept-from-base.png', png);
      put('uploads/deleted-from-base.png', png);
      put('index.php', '<?php echo "olá";\n');
      git('init', '-q');
      git('add', '.');
      git('commit', '-qm', 'Isolated objective baseline');
      rmSync(path.join(root, 'uploads', 'deleted-from-base.png'));
      put('uploads/u1.png', png);
      put('data/uploads/kept.png', png);
      put('evidence/shot.png', png);
      put('logo.svg', '<svg xmlns="http://www.w3.org/2000/svg"/>');
      put('server.log', png);
      const script = path.resolve('bridge/source-binary-check.mjs');
      let out = '';
      try {
        execFileSync(process.execPath, [script, root], { encoding: 'utf8' });
      } catch (e: any) {
        out = e.stdout;
      }
      expect(out.trim().split('\n').map((l: string) => l.split(/\s+/).slice(0, 2).join(' '))).toEqual([
        'MOVE uploads/u1.png',
        'RESTORE uploads/deleted-from-base.png',
      ]);
      git('checkout', '-q', 'HEAD', '--', 'uploads/deleted-from-base.png');
      rmSync(path.join(root, 'uploads', 'u1.png'));
      expect(execFileSync(process.execPath, [script, root], { encoding: 'utf8' })).toContain('OK');
      expect(sourcePatchInstruction).toContain(SOURCE_BINARY_CHECK);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it('tells the next attempt how to repair a binary rejection instead of rewriting the work', () => {
    const text = repairInstruction('PATCH_BINARY_SOURCE');
    expect(text).toContain(SOURCE_BINARY_CHECK);
    expect(text).toContain("RESTORE");
    expect(text).toContain('Não refaça a implementação');
  });
});

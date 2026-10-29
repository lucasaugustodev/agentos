import { describe, expect, it } from 'vitest';
import { buildProviderHealth, type ProviderSample } from '../server/provider-health.js';

const ACCOUNT = 'agentos:api:verboo';
let clock = 0;
const ok = (providerMs: number, queuedMs = 0): ProviderSample =>
  ({ accountId: ACCOUNT, at: ++clock, ok: true, providerMs, queuedMs });
const bad = (code: string, providerMs: number | null = null): ProviderSample =>
  ({ accountId: ACCOUNT, at: ++clock, ok: false, code, providerMs });

describe('provider health', () => {
  it('claims nothing without samples', () => {
    const h = buildProviderHealth([], ACCOUNT);
    expect(h.state).toBe('idle');
    expect(h.p95ProviderMs).toBeNull();
    expect(h.lastSuccessAt).toBeNull();
  });

  it('ignores other accounts', () => {
    const h = buildProviderHealth(
      [{ accountId: 'other', at: 1, ok: false, code: 'MODEL_TIMEOUT' }],
      ACCOUNT,
    );
    expect(h.state).toBe('idle');
    expect(h.requests).toBe(0);
  });

  it('reports a provider answering steadily as healthy', () => {
    const h = buildProviderHealth([ok(1200), ok(900), ok(1500)], ACCOUNT);
    expect(h.state).toBe('healthy');
    expect(h.failures).toBe(0);
    expect(h.p50ProviderMs).toBe(1200);
    expect(h.maxProviderMs).toBe(1500);
  });

  it('a single timeout is enough to stop calling it healthy', () => {
    const h = buildProviderHealth([ok(1000), bad('MODEL_TIMEOUT'), ok(1100)], ACCOUNT);
    expect(h.state).toBe('slow');
    expect(h.timeouts).toBe(1);
    // The recovery means the streak is broken, so it is not failing.
    expect(h.consecutiveFailures).toBe(0);
  });

  it('counts only the most recent run of failures as the streak', () => {
    const h = buildProviderHealth(
      [bad('MODEL_REQUEST_FAILED'), ok(800), bad('MODEL_REQUEST_FAILED'), bad('MODEL_REQUEST_FAILED')],
      ACCOUNT,
    );
    expect(h.consecutiveFailures).toBe(2);
    expect(h.failures).toBe(3);
  });

  it('three failures in a row is failing, five is down', () => {
    const three = [ok(900), bad('X'), bad('X'), bad('X')];
    expect(buildProviderHealth(three, ACCOUNT).state).toBe('failing');
    const five = [ok(900), bad('X'), bad('X'), bad('X'), bad('X'), bad('X')];
    expect(buildProviderHealth(five, ACCOUNT).state).toBe('down');
  });

  it('is down when nothing succeeded at all', () => {
    const h = buildProviderHealth([bad('MODEL_REQUEST_FAILED')], ACCOUNT);
    expect(h.state).toBe('down');
    expect(h.lastFailureCode).toBe('MODEL_REQUEST_FAILED');
    expect(h.lastSuccessAt).toBeNull();
  });

  it('measures latency on successes only, so failures cannot look fast', () => {
    const h = buildProviderHealth([ok(30_000), bad('MODEL_REQUEST_FAILED', 40)], ACCOUNT);
    expect(h.p50ProviderMs).toBe(30_000);
    expect(h.maxProviderMs).toBe(30_000);
  });

  it('keeps queue wait apart from provider latency', () => {
    const h = buildProviderHealth([ok(2_000, 45_000)], ACCOUNT);
    expect(h.p95ProviderMs).toBe(2_000);
    expect(h.maxQueuedMs).toBe(45_000);
  });
});

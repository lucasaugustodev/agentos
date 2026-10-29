import { afterEach, describe, expect, it } from 'vitest';
import { generationTiming } from '../server/generation-timing.js';

const REAL_ACCOUNT = 'agentos:api:cb017dea-b631-49ae-bd6b-36655265c9d6';

afterEach(() => {
  delete process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS;
});

describe('generation timing', () => {
  it('gives the measured budget to a GLM model under any account id', () => {
    // The lane is the model's, not a placeholder UUID's: an installation
    // running GLM under its own account needs the same budget.
    expect(generationTiming(REAL_ACCOUNT, 'glm-5.3-flash').requestTimeoutMs).toBe(720_000);
  });

  it('keeps the shorter default for a non-GLM model', () => {
    expect(generationTiming(REAL_ACCOUNT, 'some-other-model').requestTimeoutMs).toBe(240_000);
  });

  it('honours a configured budget within bounds', () => {
    process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS = '900000';
    expect(generationTiming(REAL_ACCOUNT, 'glm-5.3-flash').requestTimeoutMs).toBe(900_000);
  });

  it('ignores a budget outside bounds rather than silently truncating a plan', () => {
    for (const value of ['10', '7200000', 'nonsense']) {
      process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS = value;
      expect(generationTiming(REAL_ACCOUNT, 'glm-5.3-flash').requestTimeoutMs).toBe(720_000);
    }
  });

  it('leaves the transport room beyond the provider deadline for queueing', () => {
    const timing = generationTiming(REAL_ACCOUNT, 'glm-5.3-flash');
    // A transport deadline at or below the provider's would abort a call that
    // was still legitimately waiting for its permit.
    expect(timing.timeout).toBeGreaterThan(timing.requestTimeoutMs);
  });
});

// Whether a provider is answering, and how fast, read from the model events the
// gateway already records. Derived, never stored: a rollup that is written down
// goes stale silently, and a stale "connected" is exactly what hid a provider
// that had stopped answering.

/** One recorded model outcome. `providerMs` is null when no permit was granted. */
export type ProviderSample = {
  accountId: string;
  at: number;
  ok: boolean;
  code?: string | null;
  providerMs?: number | null;
  queuedMs?: number | null;
};

export type ProviderHealth = {
  accountId: string;
  /** idle: nothing in the window, so neither health nor failure is claimed. */
  state: 'idle' | 'healthy' | 'slow' | 'failing' | 'down';
  requests: number;
  failures: number;
  timeouts: number;
  consecutiveFailures: number;
  lastSuccessAt: number | null;
  lastFailureAt: number | null;
  lastFailureCode: string | null;
  /** Provider latency only; queue wait is reported apart so the two never blur. */
  p50ProviderMs: number | null;
  p95ProviderMs: number | null;
  maxProviderMs: number | null;
  maxQueuedMs: number | null;
};

// A single timeout already means some request never came back inside its whole
// budget, which no latency average would show.
const FAILING_STREAK = 3;
const DOWN_STREAK = 5;

const percentile = (sorted: number[], p: number) =>
  sorted.length ? sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)] : null;

export function buildProviderHealth(samples: ProviderSample[], accountId: string): ProviderHealth {
  // Oldest first, so the trailing streak is the most recent run of failures.
  const ordered = samples.filter((s) => s.accountId === accountId).sort((a, b) => a.at - b.at);
  const failures = ordered.filter((s) => !s.ok);
  const timeouts = failures.filter((s) => s.code === 'MODEL_TIMEOUT');
  const succeeded = ordered.filter((s) => s.ok);
  let consecutiveFailures = 0;
  for (let i = ordered.length - 1; i >= 0 && !ordered[i].ok; i--) consecutiveFailures++;
  const latencies = succeeded
    .map((s) => s.providerMs)
    .filter((v): v is number => typeof v === 'number')
    .sort((a, b) => a - b);
  const queued = ordered
    .map((s) => s.queuedMs)
    .filter((v): v is number => typeof v === 'number');
  const last = <T>(rows: T[]) => (rows.length ? rows[rows.length - 1] : undefined);
  const health: ProviderHealth = {
    accountId,
    state: 'idle',
    requests: ordered.length,
    failures: failures.length,
    timeouts: timeouts.length,
    consecutiveFailures,
    lastSuccessAt: last(succeeded)?.at ?? null,
    lastFailureAt: last(failures)?.at ?? null,
    lastFailureCode: last(failures)?.code ?? null,
    p50ProviderMs: percentile(latencies, 50),
    p95ProviderMs: percentile(latencies, 95),
    maxProviderMs: latencies.length ? latencies[latencies.length - 1] : null,
    maxQueuedMs: queued.length ? Math.max(...queued) : null,
  };
  if (!ordered.length) return health;
  // Order matters: a provider answering nothing at all outranks a slow one.
  if (consecutiveFailures >= DOWN_STREAK || (!succeeded.length && failures.length))
    health.state = 'down';
  else if (consecutiveFailures >= FAILING_STREAK) health.state = 'failing';
  else if (timeouts.length) health.state = 'slow';
  else health.state = failures.length ? 'slow' : 'healthy';
  return health;
}

// Seconds below a minute, then minutes: an operator reads "2 min", not "126s".
export function duration(ms: number | null | undefined) {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return '—';
  const value = Math.max(0, ms);
  if (value < 1000) return Math.round(value) + 'ms';
  if (value < 60_000) return (value / 1000).toFixed(value < 10_000 ? 1 : 0) + 's';
  return Math.round(value / 60_000) + ' min';
}

export function sinceLabel(at: number | null, now: number) {
  return at === null ? 'nunca' : 'há ' + duration(now - at);
}

/** Default provider budget per request; mirrors the bridge's 720s. */
export const MODEL_REQUEST_BUDGET_MS = 720_000;

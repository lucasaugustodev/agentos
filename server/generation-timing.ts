/** Deadlines bound one call, not the objective's token budget. The provider
 * deadline begins after request admission; the transport also allows queueing.
 * GLM's measured streamed reasoning exceeded the former four-minute deadline. */
export function generationTiming(accountId: string, model: string) {
  // Identify the lane by its model, as the scheduler, the SQL admission trigger
  // and the request gate already do. An installation running GLM under its own
  // account id needs the same measured budget, not the four-minute default.
  const glm = /^glm[-.]/i.test(model ?? '') ||
    accountId === (process.env.AGENTOS_GLM_ACCOUNT_ID || 'agentos:api:00000000-0000-4000-8000-000000000001');
  // Measured throughput varies by router: 720s fits the reference deployment.
  // A slower endpoint needs a larger budget, not a silently truncated plan.
  const budget = Number(process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS);
  const requestTimeoutMs = glm
    ? (Number.isSafeInteger(budget) && budget >= 60000 && budget <= 3600000 ? budget : 720000)
    : 240000;
  return { requestTimeoutMs, timeout: glm ? requestTimeoutMs + 180000 : 240000, reasoning: glm ? 'low' : 'medium' };
}

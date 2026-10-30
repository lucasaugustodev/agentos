/** Deadlines bound one call, not the objective's token budget. The provider
 * deadline begins after request admission; the transport also allows queueing.
 * GLM's measured streamed reasoning exceeded the former four-minute deadline. */
import {readModelPolicy,modelRouteAllowed} from '../bridge/model-policy.mjs';
export function generationTiming(accountId: string, model: string) {
  const glm = accountId === 'agentos:api:00000000-0000-4000-8000-000000000001' && model === 'glm-5.3-flash';
  const policy=readModelPolicy();
  const deepseek=policy?.version===2&&modelRouteAllowed(accountId,model,policy);
  return { requestTimeoutMs: glm ? 720000 : 240000, timeout: glm ? 900000 : 240000, reasoning: glm || deepseek ? 'low' : 'medium' };
}

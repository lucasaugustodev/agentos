/** Deadlines bound one call, not the objective's token budget. The provider
 * deadline begins after request admission; the transport also allows queueing.
 * GLM's measured streamed reasoning exceeded the former four-minute deadline. */
import {readModelPolicy,modelRouteAllowed} from '../bridge/model-policy.mjs';
export function generationTiming(accountId: string, model: string) {
  const glm = accountId === 'agentos:api:00000000-0000-4000-8000-000000000001' && model === 'glm-5.3-flash';
  const policy=readModelPolicy();
  const deepseek=policy?.version===2&&modelRouteAllowed(accountId,model,policy);
  return { requestTimeoutMs: glm ? 720000 : 240000, timeout: glm ? 900000 : 240000, reasoning: glm ? 'low' : deepseek ? 'high' : 'medium' };
}

/** Planner-only output allowance: the selected route exhausted 8192 tokens in
 * reasoning before producing JSON. Other roles and legacy profiles stay fixed. */
export function planningGenerationSettings(accountId:string,model:string,incremental:boolean) {
  const timing=generationTiming(accountId,model),policy=readModelPolicy();
  return {...timing,maxOutputTokens:policy?.version===2&&modelRouteAllowed(accountId,model,policy)
    ? 32768 : incremental ? 8192 : 12000};
}

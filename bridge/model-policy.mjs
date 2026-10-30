import { readFileSync } from 'node:fs';

/** Operator-owned configuration, never supplied by a prompt or RPC parameter. */
export function readModelPolicy() {
  const file = process.env.AGENTOS_MODEL_POLICY_FILE || '/etc/agentos/model-policy.json';
  let raw;
  try { raw = readFileSync(file, 'utf8'); }
  catch (error) {
    if (error.code === 'ENOENT' && process.env.AGENTOS_GLM_ONLY !== '1') return null;
    throw Object.assign(Error('MODEL_POLICY_UNAVAILABLE'), { code: 'MODEL_POLICY_UNAVAILABLE' });
  }
  return validateModelPolicy(JSON.parse(raw));
}
export function validateModelPolicy(p) {
  const legacy = p?.version === 1 && p.mode === 'glm-only' &&
      p.accountId === 'agentos-api-00000000-0000-4000-8000-000000000001' &&
      p.bridgeId === 'agentos:api:00000000-0000-4000-8000-000000000001' &&
      p.model === 'glm-5.3-flash';
  const id = typeof p?.accountId === 'string' && p.accountId.match(/^agentos-api-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/)?.[1];
  const exclusive = p?.version === 2 && p.mode === 'exclusive-model' && id &&
      p.bridgeId === `agentos:api:${id}` && p.model === 'deepseek-v4-pro-0813' &&
      Object.keys(p).every(key => ['version','mode','accountId','bridgeId','model','requestLimit','sessionLimit'].includes(key));
  if ((!legacy && !exclusive) || p.requestLimit !== 6 || p.sessionLimit !== 12)
    throw Object.assign(Error('MODEL_POLICY_INVALID'), { code: 'MODEL_POLICY_INVALID' });
  return p;
}

/** Same six-permit lane for legacy GLM and the operator's selected API account.
 * A model name alone never grants a profile or bypasses a permit. */
export function usesSharedRequestGate(accountId, policy = readModelPolicy()) {
  return accountId === 'agentos:api:00000000-0000-4000-8000-000000000001' ||
    !!policy && [policy.accountId, policy.bridgeId].includes(accountId);
}
export function modelRouteAllowed(accountId, model, policy = readModelPolicy()) {
  return !policy || ([policy.accountId, policy.bridgeId].includes(accountId) && model === policy.model);
}
export function assertModelRoute(accountId, model, policy = readModelPolicy()) {
  if (!modelRouteAllowed(accountId, model, policy))
    throw Object.assign(Error('MODEL_POLICY_GLM_ONLY'), { code: 'MODEL_POLICY_GLM_ONLY' });
}

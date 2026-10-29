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
  if (p?.version !== 1) throw Object.assign(Error('MODEL_POLICY_INVALID'), { code: 'MODEL_POLICY_INVALID' });
  if (p.mode === 'glm-only') {
    if (p.accountId !== 'agentos-api-00000000-0000-4000-8000-000000000001' ||
        p.bridgeId !== 'agentos:api:00000000-0000-4000-8000-000000000001' ||
        p.model !== 'glm-5.3-flash' || p.requestLimit !== 6 || p.sessionLimit !== 12)
      throw Object.assign(Error('MODEL_POLICY_INVALID'), { code: 'MODEL_POLICY_INVALID' });
  }
  return p;
}
export function modelRouteAllowed(accountId, model, policy = readModelPolicy()) {
  if (!policy || policy.mode !== 'glm-only') return true;
  return [policy.accountId, policy.bridgeId].includes(accountId) && model === policy.model;
}
export function assertModelRoute(accountId, model, policy = readModelPolicy()) {
  if (!modelRouteAllowed(accountId, model, policy))
    throw Object.assign(Error('MODEL_POLICY_GLM_ONLY'), { code: 'MODEL_POLICY_GLM_ONLY' });
}

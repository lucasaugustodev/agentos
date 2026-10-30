import { readModelPolicy, modelRouteAllowed } from './model-policy.mjs';
import { glmModelProfile } from './glm-model-profile.mjs';

/** Explicit transport contract; changing a model string cannot opt into it. */
export function operatorModelProfile(accountId, modelId, policy = readModelPolicy()) {
  if (policy?.version === 2 && modelRouteAllowed(accountId, modelId, policy)) return {
    reasoning: true,
    maxTokens: 32768,
    compat: {
      thinkingFormat: 'deepseek', supportsReasoningEffort: true,
      maxTokensField: 'max_tokens', supportsStore: false, supportsDeveloperRole: false,
    },
    thinkingLevelMap: { off: null, minimal: 'low', low: 'low', medium: 'high', high: 'high', xhigh: 'max' },
  };
  return glmModelProfile(accountId, modelId);
}

export function workerModelSettings(accountId, modelId) {
  return operatorModelProfile(accountId, modelId).reasoning
    ? { reasoning: 'low', maxOutputTokens: 32768 }
    : { reasoning: 'high', maxOutputTokens: 8192 };
}

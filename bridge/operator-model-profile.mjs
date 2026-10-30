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
    // This operator route advertises high/max, unlike the native DeepSeek API.
    thinkingLevelMap: { off: null, minimal: 'high', low: 'high', medium: 'high', high: 'high', xhigh: 'max' },
  };
  return glmModelProfile(accountId, modelId);
}

export function workerModelSettings(accountId, modelId) {
  const profile=operatorModelProfile(accountId, modelId);
  return profile.reasoning
    ? { reasoning: profile.compat.thinkingFormat === 'deepseek' ? 'high' : 'low', maxOutputTokens: 32768 }
    : { reasoning: 'high', maxOutputTokens: 8192 };
}

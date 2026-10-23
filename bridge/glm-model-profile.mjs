import { inGlmRequestLane } from './gated-generation.mjs';
// The connected GLM endpoint implements Z.ai's thinking contract. Keep this
// profile scoped to the verified account/model; never infer it from a prompt.
// The lane has one definition, the one the request gate enforces: an
// installation running the verified model under its own account id needs the
// same thinking contract, not the generic defaults.
export function glmModelProfile(accountId, modelId) {
  if (!inGlmRequestLane({ accountId, model: modelId }) || modelId !== 'glm-5.3-flash') return {};
  return {
    reasoning: true,
    maxTokens: 32768,
    compat: {
      thinkingFormat: 'zai', supportsReasoningEffort: true,
      maxTokensField: 'max_tokens', supportsStore: false, supportsDeveloperRole: false,
    },
    thinkingLevelMap: { off: null, minimal: 'low', low: 'low', medium: 'high', high: 'high', xhigh: 'max' },
  };
}

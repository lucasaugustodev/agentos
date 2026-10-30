import test from 'node:test';
import assert from 'node:assert/strict';
import { glmModelProfile } from '../bridge/glm-model-profile.mjs';
// Exercise the inspected adapter actually selected by bridge/server.mjs.
// No optional SDK, real credential or network is needed.
import { openAICompletionsFallback } from '../bridge/openai-completions-fallback.mjs';
const account = 'agentos:api:00000000-0000-4000-8000-000000000001';
for (const [reasoning, expected] of [['low', 'low'], ['medium', 'high'], ['xhigh', 'max']]) {
  test(`GLM wire format preserves ${reasoning} effort and bounded output`, async () => {
    let wire;
    const model = { id: 'glm-5.3-flash', name: 'GLM', provider: 'openai', api: 'openai-completions',
      baseUrl: 'https://fixture.invalid/v1', input: ['text'], contextWindow: 128000,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, ...glmModelProfile(account, 'glm-5.3-flash') };
    const fetch = async (_url, options) => {
      wire = JSON.parse(options.body);
      const chunks = [
        { id: 'fixture', choices: [{ index: 0, delta: { role: 'assistant', content: 'ok' }, finish_reason: null }] },
        { id: 'fixture', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } },
      ];
      return new Response(chunks.map(c => 'data: ' + JSON.stringify(c) + '\n\n').join('') + 'data: [DONE]\n\n',
        { headers: { 'content-type': 'text/event-stream' } });
    };
    const result = await openAICompletionsFallback({fetch}).streamSimple(model,
      { systemPrompt: 'fixture', messages: [{ role: 'user', content: 'fixture', timestamp: Date.now() }] },
      { apiKey: 'fixture', fetch, reasoning, maxTokens: 12000 }).result();
    assert.equal(result.stopReason, 'stop');
    assert.equal(wire.reasoning_effort, expected);
    assert.equal(wire.max_tokens, 12000);
    assert.equal(wire.max_completion_tokens, undefined);
    assert.equal(wire.messages[0].role, 'system');
    assert.equal(wire.thinking.type, 'enabled');
    assert.equal(wire.thinking.clear_thinking, false);
  });
}
test('does not change another account or another model', () => {
  assert.deepEqual(glmModelProfile('unverified', 'glm-5.3-flash'), {});
  assert.deepEqual(glmModelProfile(account, 'other'), {});
});

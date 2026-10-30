// OpenAI wire adapter for the pinned DSH runtime. Credentials never enter the sandbox.
import { workerModelSettings } from './operator-model-profile.mjs';
export function toGeneration(body, route) {
  const messages = [],
    system = [];
  for (const m of body.messages ?? []) {
    const text =
      typeof m.content === 'string'
        ? m.content
        : (m.content ?? [])
            .filter((b) => b.type === 'text')
            .map((b) => b.text)
            .join('\n');
    if (m.role === 'system' || m.role === 'developer') {
      system.push(text);
      continue;
    }
    if (m.role === 'tool') {
      messages.push({
        role: 'toolResult',
        toolCallId: m.tool_call_id,
        toolName: m.name ?? 'bash',
        content: [{ type: 'text', text }],
        isError: false,
        timestamp: Date.now(),
      });
    } else if (m.role === 'assistant') {
      const content = text ? [{ type: 'text', text }] : [];
      if (typeof m.reasoning_content === 'string' && m.reasoning_content)
        content.unshift({type:'thinking',thinking:m.reasoning_content});
      for (const t of m.tool_calls ?? [])
        content.push({
          type: 'toolCall',
          id: t.id,
          name: t.function.name,
          arguments: JSON.parse(t.function.arguments),
        });
      messages.push({
        role: 'assistant',
        content,
        api: 'openai-completions',
        provider: 'agentos-gateway',
        model: route.model,
        stopReason: content.some((b) => b.type === 'toolCall') ? 'toolUse' : 'stop',
        timestamp: Date.now(),
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
      });
    } else if (m.role === 'user')
      messages.push({ role: 'user', content: text, timestamp: Date.now() });
    else throw Error('WIRE_ROLE_UNSUPPORTED');
  }
  return {
    ...route,
    system: system.join('\n\n'),
    messages,
    tools: (body.tools ?? []).map((t) => ({
      name: t.function.name,
      description: t.function.description,
      parameters: t.function.parameters,
    })),
    maxOutputTokens: workerModelSettings(route.accountId, route.model).maxOutputTokens,
  };
}
export function completion(message, model) {
  const content = message.content
    .filter((b) => b.type === 'text')
    .map((b) => b.text)
    .join('\n');
  const tool_calls = message.content
    .filter((b) => b.type === 'toolCall')
    .map((b, index) => ({
      index,
      id: b.id,
      type: 'function',
      function: { name: b.name, arguments: JSON.stringify(b.arguments) },
    }));
  const reasoning = message.content.filter(b=>b.type==='thinking').map(b=>b.thinking).join('');
  const delta = { role: 'assistant', content, ...(reasoning ? {reasoning_content:reasoning} : {}), ...(tool_calls.length ? { tool_calls } : {}) };
  const finish_reason = tool_calls.length
    ? 'tool_calls'
    : message.stopReason === 'length'
      ? 'length'
      : 'stop';
  const common = {
    id: 'agentos-' + Date.now(),
    object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000),
    model,
  };
  return [
    { ...common, choices: [{ index: 0, delta, finish_reason: null }] },
    {
      ...common,
      choices: [{ index: 0, delta: {}, finish_reason }],
      usage: {
        prompt_tokens: (message.usage?.input ?? 0) + (message.usage?.cacheRead ?? 0),
        completion_tokens: message.usage?.output ?? 0,
        total_tokens: message.usage?.totalTokens ?? 0,
      },
    },
  ];
}

// OpenAI wire adapter for the pinned DSH runtime. Credentials never enter the sandbox.
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
    maxOutputTokens: /^glm[-.]/i.test(route.model ?? '') ? 32768 : 8192,
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
// Messages wire adapter for the pinned DSH runtime, whose LLM adapters speak
// only the Messages protocol while the account route serves OpenAI completions.
// Both wires converge on the same neutral generation; the route still fixes the
// account and model, and credentials never enter the sandbox.
const flatten = (content) =>
  typeof content === 'string'
    ? content
    : (content ?? [])
        .filter((b) => b.type === 'text')
        .map((b) => b.text)
        .join('\n');
export function fromMessages(body, route) {
  const messages = [],
    system = [],
    toolNames = new Map();
  if (body.system) system.push(flatten(body.system));
  for (const m of body.messages ?? []) {
    const blocks =
      typeof m.content === 'string' ? [{ type: 'text', text: m.content }] : (m.content ?? []);
    if (m.role === 'system') {
      system.push(flatten(blocks));
      continue;
    }
    if (m.role === 'user') {
      // A Messages turn carries tool results beside ordinary text; each result
      // is its own neutral message, named by the call it answers.
      for (const b of blocks.filter((b) => b.type === 'tool_result'))
        messages.push({
          role: 'toolResult',
          toolCallId: b.tool_use_id,
          toolName: toolNames.get(b.tool_use_id) ?? 'bash',
          content: [{ type: 'text', text: flatten(b.content) }],
          isError: b.is_error === true,
          timestamp: Date.now(),
        });
      const text = flatten(blocks.filter((b) => b.type !== 'tool_result'));
      if (text) messages.push({ role: 'user', content: text, timestamp: Date.now() });
      continue;
    }
    if (m.role !== 'assistant') throw Error('WIRE_ROLE_UNSUPPORTED');
    const text = flatten(blocks),
      content = text ? [{ type: 'text', text }] : [];
    for (const b of blocks.filter((b) => b.type === 'tool_use')) {
      toolNames.set(b.id, b.name);
      content.push({ type: 'toolCall', id: b.id, name: b.name, arguments: b.input ?? {} });
    }
    if (!content.length) continue;
    messages.push({
      role: 'assistant',
      content,
      api: 'anthropic-messages',
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
  }
  return {
    ...route,
    system: system.filter(Boolean).join('\n\n'),
    messages,
    tools: (body.tools ?? []).map((t) => ({
      name: t.name,
      description: t.description,
      parameters: t.input_schema,
    })),
    // Same budget as the completions wire: GLM spends output tokens on
    // reasoning, so the smaller ceiling truncates the answer, not the thinking.
    maxOutputTokens: /^glm[-.]/i.test(route.model ?? '') ? 32768 : 8192,
  };
}
export function messagesEvents(message, model) {
  const blocks = [];
  for (const b of message.content) {
    if (b.type === 'text' && b.text)
      blocks.push({
        start: { type: 'text', text: '' },
        delta: { type: 'text_delta', text: b.text },
      });
    else if (b.type === 'toolCall')
      blocks.push({
        start: { type: 'tool_use', id: b.id, name: b.name, input: {} },
        // The adapter parses the accumulated deltas, not this opening object.
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(b.arguments ?? {}) },
      });
  }
  const usage = message.usage ?? {};
  const events = [
    {
      type: 'message_start',
      message: {
        id: 'agentos-' + Date.now(),
        type: 'message',
        role: 'assistant',
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: {
          input_tokens: usage.input ?? 0,
          output_tokens: 0,
          cache_read_input_tokens: usage.cacheRead ?? 0,
          cache_creation_input_tokens: usage.cacheWrite ?? 0,
        },
      },
    },
  ];
  for (const [index, block] of blocks.entries()) {
    events.push({ type: 'content_block_start', index, content_block: block.start });
    events.push({ type: 'content_block_delta', index, delta: block.delta });
    events.push({ type: 'content_block_stop', index });
  }
  events.push({
    type: 'message_delta',
    delta: {
      stop_reason: blocks.some((b) => b.start.type === 'tool_use')
        ? 'tool_use'
        : message.stopReason === 'length'
          ? 'max_tokens'
          : 'end_turn',
      stop_sequence: null,
    },
    usage: { output_tokens: usage.output ?? 0 },
  });
  events.push({ type: 'message_stop' });
  return events;
}
// An error after the stream opened can no longer use an HTTP status, so the
// adapter classifies it by this type. Keep the gateway code as the message.
export function messagesError(code) {
  const type =
    code === 'QUOTA_EXCEEDED'
      ? 'rate_limit_error'
      : code === 'AUTH_REQUIRED'
        ? 'authentication_error'
        : code === 'CONTEXT_TOO_LARGE'
          ? 'invalid_request_error'
          : 'api_error';
  return { type: 'error', error: { type, message: code } };
}

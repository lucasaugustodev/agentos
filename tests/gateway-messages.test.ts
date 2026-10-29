import { expect, it } from 'vitest';
// @ts-expect-error Remote ESM has no generated declarations.
import { fromMessages, messagesEvents, messagesError } from '../bridge/runtime-gateway.mjs';

it('carries a Messages turn into the neutral generation while fixing the account route', () => {
  const value = fromMessages(
    {
      model: 'untrusted-model',
      system: 'Scoped worker',
      max_tokens: 4096,
      messages: [
        { role: 'user', content: 'Run test' },
        {
          role: 'assistant',
          content: [
            { type: 'text', text: 'Running it' },
            { type: 'tool_use', id: 't1', name: 'bash', input: { command: 'node test.mjs' } },
          ],
        },
        {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 't1', content: [{ type: 'text', text: 'exit 0' }] },
            { type: 'text', text: 'And now the report' },
          ],
        },
        { role: 'system', content: 'Report in Portuguese' },
      ],
      tools: [{ name: 'bash', description: 'shell', input_schema: { type: 'object' } }],
    },
    { accountId: 'chosen', model: 'chosen-model' },
  );
  expect(value.model).toBe('chosen-model');
  expect(value.system).toBe('Scoped worker\n\nReport in Portuguese');
  expect(value.tools[0]).toMatchObject({ name: 'bash', parameters: { type: 'object' } });
  expect(value.messages[0]).toMatchObject({ role: 'user', content: 'Run test' });
  expect(value.messages[1].content[1]).toMatchObject({
    type: 'toolCall',
    id: 't1',
    arguments: { command: 'node test.mjs' },
  });
  // The result names the call it answers, and the remaining text stays a turn.
  expect(value.messages[2]).toMatchObject({
    role: 'toolResult',
    toolCallId: 't1',
    toolName: 'bash',
    isError: false,
  });
  expect(value.messages[3]).toMatchObject({ role: 'user', content: 'And now the report' });
});

it('reports a failed tool result and refuses an unknown wire role', () => {
  const value = fromMessages(
    {
      messages: [
        { role: 'assistant', content: [{ type: 'tool_use', id: 't9', name: 'read', input: {} }] },
        {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 't9', is_error: true, content: 'missing file' },
          ],
        },
      ],
    },
    { accountId: 'a', model: 'm' },
  );
  expect(value.messages[1]).toMatchObject({ toolName: 'read', isError: true });
  expect(value.messages[1].content[0].text).toBe('missing file');
  expect(() => fromMessages({ messages: [{ role: 'tool', content: 'x' }] }, {})).toThrow(
    'WIRE_ROLE_UNSUPPORTED',
  );
});

it('streams text and tool calls as settled Messages blocks with one terminal stop', () => {
  const events = messagesEvents(
    {
      content: [
        { type: 'text', text: 'done' },
        { type: 'toolCall', id: 't2', name: 'bash', arguments: { command: 'pwd' } },
      ],
      stopReason: 'toolUse',
      usage: { input: 10, cacheRead: 5, output: 3, totalTokens: 18 },
    },
    'chosen-model',
  );
  expect(events.map((e: any) => e.type)).toEqual([
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
    'message_delta',
    'message_stop',
  ]);
  expect(events[0].message.usage).toMatchObject({ input_tokens: 10, cache_read_input_tokens: 5 });
  expect(events[2].delta).toMatchObject({ type: 'text_delta', text: 'done' });
  expect(events[4]).toMatchObject({ index: 1, content_block: { type: 'tool_use', id: 't2' } });
  // The adapter parses the accumulated deltas, so they must hold the whole input.
  expect(JSON.parse(events[5].delta.partial_json)).toEqual({ command: 'pwd' });
  expect(events[7]).toMatchObject({
    delta: { stop_reason: 'tool_use' },
    usage: { output_tokens: 3 },
  });
});

it('maps a truncated answer and keeps provider failures classifiable after the stream opens', () => {
  const truncated = messagesEvents(
    { content: [{ type: 'text', text: 'half' }], stopReason: 'length', usage: {} },
    'm',
  );
  expect(truncated.at(-2).delta.stop_reason).toBe('max_tokens');
  const empty = messagesEvents({ content: [], stopReason: 'stop', usage: {} }, 'm');
  expect(empty.map((e: any) => e.type)).toEqual(['message_start', 'message_delta', 'message_stop']);
  expect(empty.at(-2).delta.stop_reason).toBe('end_turn');
  expect(messagesError('QUOTA_EXCEEDED').error.type).toBe('rate_limit_error');
  expect(messagesError('AUTH_REQUIRED').error.type).toBe('authentication_error');
  expect(messagesError('MODEL_TIMEOUT')).toMatchObject({
    type: 'error',
    error: { type: 'api_error', message: 'MODEL_TIMEOUT' },
  });
});

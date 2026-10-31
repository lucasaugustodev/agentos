// Inspected OpenAI-compatible transport selected for the verified GLM route.
// Message shapes mirror bridge/runtime-gateway.mjs, which converts the same
// wire format in the opposite direction; the two must stay symmetric.
// Only typed availability evidence leaves this module: an upstream body never
// becomes an error message, an event or an artifact.
import { classifyProviderError,retryAfterSeconds } from './opencode-provider-error.mjs';
import { semanticModelWatchdog, readAbortableModelBody, modelAbortFailure, MODEL_IDLE_TIMEOUT_MS } from './semantic-model-watchdog.mjs';
import { createModelTransportTelemetry } from './model-transport-telemetry.mjs';
import { diagnosticFinishReason, observeProtocolTerminal } from './protocol-terminal-diagnostic.mjs';
import {transportFailureReason,markModelTransportFailure} from './model-transport-failure.mjs';

export const RETRY_ATTEMPTS = 3;
export const RETRY_BUDGET_MS = 30000;
const MAX_DELAY_MS = 15000;
export const PROGRESS_INTERVAL_MS = 3000;
const failure = (code) => Object.assign(Error(code), {code});
const MAX_RESPONSE_BYTES = 8 * 1024 * 1024;

const textOf = (content) =>
  typeof content === 'string'
    ? content
    : (content ?? []).filter((b) => b?.type === 'text').map((b) => b.text).join('\n');

/** Internal generation messages to OpenAI chat-completions messages. */
export function toCompletionsMessages(systemPrompt, messages) {
  const out = [];
  if (systemPrompt) out.push({ role: 'system', content: systemPrompt });
  for (const m of messages ?? []) {
    if (m.role === 'toolResult') {
      out.push({ role: 'tool', tool_call_id: m.toolCallId, content: textOf(m.content) });
      continue;
    }
    if (m.role === 'assistant') {
      const calls = (Array.isArray(m.content) ? m.content : []).filter((b) => b?.type === 'toolCall');
      const text = textOf(m.content);
      out.push({
        role: 'assistant',
        content: text || null,
        ...((m.content ?? []).some?.(b => b.type === 'thinking') ? {
          reasoning_content: m.content.filter(b => b.type === 'thinking').map(b => b.thinking).join(''),
        } : {}),
        ...(calls.length
          ? {
              tool_calls: calls.map((c) => ({
                id: c.id,
                type: 'function',
                function: { name: c.name, arguments: JSON.stringify(c.arguments ?? {}) },
              })),
            }
          : {}),
      });
      continue;
    }
    out.push({ role: m.role, content: textOf(m.content) });
  }
  return out;
}

/** OpenAI chat-completions response to an internal assistant message. */
export function fromCompletionsResponse(data, diagnostic) {
  const choice = data?.choices?.[0];
  return buildMessage({
    text: choice?.message?.content ?? '',
    thinking: choice?.message?.reasoning_content ?? '',
    thinkingPresent: typeof choice?.message?.reasoning_content === 'string',
    calls: (choice?.message?.tool_calls ?? []).map((c, index) => ({
      index, id: c.id, name: c.function?.name, arguments: c.function?.arguments,
    })),
    finish: choice?.finish_reason,
    usage: data?.usage,
  }, diagnostic);
}

function buildMessage({ text, thinking = '', thinkingPresent = false, calls, finish, usage }, diagnostic) {
  const observe = value => { try { Promise.resolve(diagnostic?.(value)).catch(() => {}); } catch {} };
  const reject = (reason, code = 'MODEL_INCOMPLETE_RESPONSE') => {
    observe(reason); throw failure(code);
  };
  // No partial response may reach a tool executor. A transport EOF is not a
  // provider terminal marker; length also takes precedence over tool calls.
  if (finish === 'length') reject('output_limit', 'MODEL_OUTPUT_LIMIT');
  if (!['stop','tool_calls'].includes(finish)) reject(diagnosticFinishReason(finish) === 'missing' ? 'missing_finish' : 'invalid_finish');
  if ((finish === 'tool_calls') !== (calls.length > 0)) reject('finish_tool_mismatch');
  if (!calls.length && !text) reject('empty_message', 'MISSING_MODEL_RESPONSE');
  const content = [];
  if (thinking || thinkingPresent) content.push({type:'thinking',thinking});
  if (text) content.push({ type: 'text', text: String(text) });
  const ids = new Set();
  for (const call of calls) {
    if (typeof call.id !== 'string' || !call.id) reject('tool_id_missing');
    if (ids.has(call.id)) reject('tool_id_duplicate');
    if (typeof call.name !== 'string' || !call.name) reject('tool_name_missing');
    ids.add(call.id);
    let args;
    try { args = JSON.parse(call.arguments); } catch { reject('invalid_tool_json'); }
    if (!args || typeof args !== 'object' || Array.isArray(args)) reject('invalid_tool_shape');
    content.push({ type: 'toolCall', id: call.id, name: call.name, arguments: args });
  }
  const message = {
    role: 'assistant',
    content,
    stopReason: content.some((b) => b.type === 'toolCall')
      ? 'toolUse'
      : finish === 'length' ? 'length' : 'stop',
    usage: usage
      ? { input: Math.max(0,(usage.prompt_tokens ?? 0)-(usage.prompt_tokens_details?.cached_tokens ?? 0)), output: usage.completion_tokens ?? 0,
          cacheRead: usage.prompt_tokens_details?.cached_tokens ?? 0,
          totalTokens: usage.total_tokens ?? 0 }
      : null,
  };
  observe('accepted');
  return message;
}

/** Consume one chat-completions SSE body into the same internal message.
 * This provider streams at a few tokens per second, so a long generation that
 * is not streamed is one silent request held open for many minutes. */
export async function readCompletionsStream(body, signal, onProgress, now = () => Date.now(), onSemanticProgress, onBodyPartial, telemetry) {
  const decoder = new TextDecoder();
  const startedAt = now();
  let chunks = 0, reportedAt = -Infinity, reportedSemantic = 0, semanticCharacters = 0;
  const calls = new Map();
  let buffer = '', text = '', thinking = '', thinkingPresent = false, finish, usage, bytes = 0, done = false, bodyObserved = false;
  let eofObserved = false, validation = 'interrupted';
  const counters = () => ({characters:text.length, reasoningCharacters:thinking.length,
    toolCharacters:[...calls.values()].reduce((n,c)=>n+c.arguments.length,0),
    semanticCharacters, chunks, elapsedMs:now()-startedAt});
  try { for await (const part of readAbortableModelBody(body, signal)) {
    telemetry?.chunk(part.byteLength);
    // Once any SSE body arrived, even an incomplete first JSON frame cannot be
    // replayed. This receipt is deliberately separate from semantic liveness:
    // comments/usage/partial framing never renew the idle watchdog.
    if (part.byteLength && !bodyObserved) { bodyObserved = true; onBodyPartial?.(); }
    const semanticBeforePart = semanticCharacters;
    chunks++;
    bytes += part.byteLength;
    if (bytes > MAX_RESPONSE_BYTES) { validation = 'response_too_large'; throw failure('MODEL_RESPONSE_TOO_LARGE'); }
    buffer += decoder.decode(part, { stream: true });
    let cut;
    while ((cut = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, cut).trim();
      buffer = buffer.slice(cut + 1);
      if (!line.startsWith('data:')) continue;
      const payload = line.slice(5).trim();
      if (payload === '[DONE]') { done = true; continue; }
      if (done) { validation = 'data_after_done'; throw failure('MODEL_INCOMPLETE_RESPONSE'); }
      let event;
      try { event = JSON.parse(payload); } catch { validation = 'malformed_frame'; throw failure('MODEL_INCOMPLETE_RESPONSE'); }
      if (event.error) { validation = 'provider_error'; throw failure('MODEL_REQUEST_FAILED'); }
      if (event.usage) { usage = event.usage; telemetry?.usage(usage); }
      const choice = event.choices?.[0];
      if (!choice) continue;
      if (choice.finish_reason) finish = choice.finish_reason;
      const delta = choice.delta ?? {};
      let added = 0;
      if (typeof delta.reasoning_content === 'string') { thinkingPresent = true; thinking += delta.reasoning_content; added += delta.reasoning_content.length; }
      if (typeof delta.content === 'string') { text += delta.content; added += delta.content.length; }
      for (const call of delta.tool_calls ?? []) {
        const index = call.index ?? calls.size;
        const current = calls.get(index) ?? { index, id: undefined, name: undefined, arguments: '' };
        if (typeof call.id === 'string' && call.id && call.id !== current.id) {
          current.id = call.id; added += call.id.length;
        }
        if (typeof call.function?.name === 'string' && call.function.name && call.function.name !== current.name) {
          current.name = call.function.name; added += call.function.name.length;
        }
        if (typeof call.function?.arguments === 'string') { current.arguments += call.function.arguments; added += call.function.arguments.length; }
        calls.set(index, current);
      }
      if (added > 0) {
        semanticCharacters += added;
        // Unthrottled and content-free. The watchdog cannot depend on telemetry cadence.
        onSemanticProgress?.(semanticCharacters);
        telemetry?.semantic({characters:text.length,reasoningCharacters:thinking.length,
          toolCharacters:[...calls.values()].reduce((n,c)=>n+c.arguments.length,0),semanticCharacters});
      }
    }
    // Neither empty chunks, comments, usage nor finish markers renew progress.
    if (onProgress && semanticCharacters > semanticBeforePart && semanticCharacters > reportedSemantic && now() - reportedAt >= PROGRESS_INTERVAL_MS) {
      reportedAt = now(); reportedSemantic = semanticCharacters;
      onProgress(counters());
    }
  }
  eofObserved = true;
  telemetry?.bodyEnd();
  if (signal?.aborted) throw modelAbortFailure(signal);
  if (buffer.trim()) { validation = 'unterminated_tail'; throw failure('MODEL_INCOMPLETE_RESPONSE'); }
  const message = buildMessage({ text, thinking, thinkingPresent, calls: [...calls.values()].sort((a, b) => a.index - b.index), finish, usage }, value => { validation = value; });
  onProgress?.({...counters(),done:true});
  return message;
  } finally {
    observeProtocolTerminal(telemetry, {version:1,format:'sse',eofObserved,doneObserved:done,
      finishReason:diagnosticFinishReason(finish),trailingBytes:Buffer.byteLength(buffer,'utf8'),
      toolCount:calls.size,validation,aborted:signal?.aborted===true});
  }
}

async function readCompletionsJson(body, signal, onPartial, telemetry) {
  const parts=[]; let bytes=0, eofObserved=false, validation='interrupted', finish, toolCount=0;
  try { for await (const part of readAbortableModelBody(body,signal)) {
    telemetry?.chunk(part.byteLength);
    if (part.byteLength) onPartial(); // An incomplete JSON body is never replayed.
    bytes += part.byteLength;
    if (bytes > MAX_RESPONSE_BYTES) { validation='response_too_large'; throw failure('MODEL_RESPONSE_TOO_LARGE'); }
    parts.push(part);
  }
  eofObserved=true;
  telemetry?.bodyEnd();
  let data;
  try { data=JSON.parse(Buffer.concat(parts).toString('utf8')); }
  catch(error) { validation='malformed_json'; throw error; }
  finish=data?.choices?.[0]?.finish_reason;
  toolCount=Array.isArray(data?.choices?.[0]?.message?.tool_calls)?data.choices[0].message.tool_calls.length:0;
  telemetry?.usage(data.usage);
  // JSON replies have no observable per-delta semantics; do not invent TTFT.
  return fromCompletionsResponse(data, value => { validation=value; });
  } finally {
    observeProtocolTerminal(telemetry, {version:1,format:'json',eofObserved,doneObserved:false,
      finishReason:diagnosticFinishReason(finish),trailingBytes:0,toolCount,validation,aborted:signal?.aborted===true});
  }
}

function typedFailure(failure) {
  return Object.assign(Error(failure.code), {
    code: failure.code,
    status: failure.status,
    retryAfterSeconds: failure.retryAfterSeconds ?? null,
  });
}
const cancelled = () => Object.assign(Error('CANCELLED'), { code: 'CANCELLED' });

function wait(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(cancelled());
    const onAbort = () => { clearTimeout(timer); reject(cancelled()); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', onAbort); resolve(); }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/** Honour Retry-After when present, otherwise exponential backoff with jitter.
 * A wait that would outlive the budget is refused: the permit this request
 * holds belongs to the gate, and a long provider pause is the scheduler's to
 * absorb, not this call's. */
export function backoffDelay(attempt, retryAfterSeconds, random = Math.random) {
  const header = Number.isSafeInteger(retryAfterSeconds) && retryAfterSeconds >= 0
    ? retryAfterSeconds * 1000 : null;
  // A stated wait is obeyed exactly, never shortened; one longer than the
  // budget is refused above instead of being retried too early.
  if (header !== null) return header;
  return Math.min(500 * 2 ** attempt + random() * 250, MAX_DELAY_MS);
}

export function openAICompletionsFallback({ fetch: fetchImpl = fetch, random = Math.random,
  idleTimeoutMs = MODEL_IDLE_TIMEOUT_MS, now = () => Date.now(), timers,
  telemetryNow = () => performance.now() } = {}) {
  return {
    streamSimple(model, { systemPrompt, messages, tools }, options = {}) {
      const resultPromise = (async () => {
        // The base URL is operator input and this request carries the credential.
        const endpoint = new URL(model.baseUrl);
        if (endpoint.protocol !== 'https:' &&
            !['127.0.0.1', 'localhost', '::1'].includes(endpoint.hostname))
          throw Object.assign(Error('INSECURE_PROVIDER_URL'), { code: 'INSECURE_PROVIDER_URL' });
        const url = model.baseUrl.replace(/\/+$/, '') +
          (model.baseUrl.endsWith('/chat/completions') ? '' : '/chat/completions');
        const body = {
          model: model.id,
          messages: toCompletionsMessages(systemPrompt, messages),
          stream: true,
          stream_options: {include_usage:true},
          max_tokens: options.maxTokens ?? model.maxTokens ?? 4096,
          ...(model.compat?.thinkingFormat === 'zai' ? {
            thinking:{type:'enabled',clear_thinking:false},
            reasoning_effort:model.thinkingLevelMap?.[options.reasoning ?? 'low'] ?? 'low',
          } : {}),
          ...(model.compat?.thinkingFormat === 'deepseek' ? {
            thinking:{type:options.reasoning === 'off' ? 'disabled' : 'enabled'},
            ...(options.reasoning === 'off' ? {} : {
              reasoning_effort:model.thinkingLevelMap?.[options.reasoning ?? 'high'] ?? 'high',
            }),
          } : {}),
          ...(tools?.length ? { tools: tools.map((t) => ({ type: 'function', function: t })) } : {}),
        };
        const request = {
          method: 'POST',
          redirect: 'error',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${options.apiKey}` },
          body: JSON.stringify(body),
          signal: options.signal,
        };
        const observation=createModelTransportTelemetry(request.body,options.onTelemetry,telemetryNow);
        const deadline = now() + RETRY_BUDGET_MS;
        for (let attempt = 0; ; attempt++) {
          if (options.signal?.aborted) throw cancelled();
          const last = attempt >= RETRY_ATTEMPTS - 1;
          const watchdog = semanticModelWatchdog(options.signal,idleTimeoutMs,timers);
          let response, partial = false, retry, successful=false, observedError;
          const telemetry=observation.attempt(attempt+1);
          try {
            response = await fetchImpl(url, {...request,signal:watchdog.signal});
            telemetry.headers(response.status,/event-stream/i.test(response.headers.get('content-type') ?? '')?'sse':'json');
            if (watchdog.signal.aborted) throw modelAbortFailure(watchdog.signal);
            if (response.ok) {
              const progress = () => { partial = true; watchdog.progress(); };
              const result = /event-stream/i.test(response.headers.get('content-type') ?? '')
                ? await readCompletionsStream(response.body,watchdog.signal,options.onProgress,now,progress,()=>{partial=true;},telemetry)
                : await readCompletionsJson(response.body,watchdog.signal,()=>{partial=true;},telemetry);
              successful=true;return result;
            }
            const classified=await classifyProviderError(response,{now:now()});
            // Keep the provider's wait as typed availability data. Never turn
            // an exhausted local retry budget into permission to retry early.
            if(response.status>=500)classified.retryAfterSeconds=retryAfterSeconds(response.headers.get('retry-after'),now());
            throw typedFailure(classified);
          } catch (error) {
            observedError=error?.code;
            // Caller cancellation/gate loss has precedence over a racing idle timer.
            // The owning gate preserves its original abort reason on unwind.
            if (options.signal?.aborted) throw cancelled();
            if (watchdog.signal.aborted) throw modelAbortFailure(watchdog.signal);
            if (response?.ok) {
              const code = ['MODEL_OUTPUT_LIMIT','MODEL_RESPONSE_TOO_LARGE','MISSING_MODEL_RESPONSE',
                'MODEL_INCOMPLETE_RESPONSE'].includes(error.code) ? error.code : 'MODEL_INCOMPLETE_RESPONSE';
              const delay = backoffDelay(attempt,null,random);
              if (partial || last || code === 'MODEL_OUTPUT_LIMIT' || code === 'MODEL_RESPONSE_TOO_LARGE' || now()+delay>deadline)
                throw markModelTransportFailure(failure(code),partial?null:transportFailureReason(error));
              retry={attempt:attempt+1,code,delayMs:delay};
            } else if (response) {
              const retryable = error.code === 'RATE_LIMIT' || error.status >= 500;
              const transientStatus=[500,502,503,504].includes(error.status)&&
                (!response.headers.has('retry-after')||error.retryAfterSeconds===0);
              if (!retryable || last) throw markModelTransportFailure(error,transientStatus?'provider_unavailable':null);
              const delay=backoffDelay(attempt,error.retryAfterSeconds,random);
              if (now()+delay>deadline) throw markModelTransportFailure(error,transientStatus?'provider_unavailable':null);
              retry={attempt:attempt+1,code:error.code,status:error.status,delayMs:delay};
            } else {
              const delay=backoffDelay(attempt,null,random);
              if(last || now()+delay>deadline) throw markModelTransportFailure(failure('MODEL_REQUEST_FAILED'),transportFailureReason(error));
              retry={attempt:attempt+1,code:'MODEL_REQUEST_FAILED',delayMs:delay};
            }
          } finally {
            watchdog.dispose();
            // Readers await cancellation themselves; also await unconsumed/error
            // bodies before retry/return so the gate cannot release a live transport.
            if (response?.body && !response.body.locked && typeof response.body.cancel === 'function')
              await response.body.cancel().catch(()=>{});
            const timedOut=watchdog.signal.aborted&&watchdog.signal.reason?.code==='MODEL_IDLE_TIMEOUT';
            telemetry.finish(successful?'success':options.signal?.aborted?'cancelled':timedOut?'timeout':retry?'retry':'error',
              successful?null:options.signal?.aborted?'CANCELLED':timedOut?'MODEL_IDLE_TIMEOUT':retry?.code??observedError);
          }
          options.onRetry?.(retry);
          await wait(retry.delayMs,options.signal);
        }
      })().catch(error => {
        options.onFailure?.({code: /^[A-Z_]{3,60}$/.test(error.code ?? '') ? error.code : 'MODEL_REQUEST_FAILED',
          ...(Number.isInteger(error.status) ? {status:error.status} : {}),
          ...(Number.isSafeInteger(error.retryAfterSeconds) ? {retryAfterSeconds:error.retryAfterSeconds} : {})});
        throw error;
      });
      return { result: () => resultPromise };
    },
  };
}

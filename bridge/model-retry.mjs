// One provider call inside a long attempt. The gateway buffers the response
// until it is complete, so a call that failed has delivered nothing to the
// agent loop and has no effect to reconcile: repeating that call is safe, while
// failing the attempt discards every tool result produced before it.
//
// Measured on the GLM router (2026-10-27): it streams steadily, the longest gap
// between events was 4.4s, but output runs at ~3.3 tokens/s. A call that writes
// a large file or a long summary is slow, not hung; 720s cut off anything above
// ~2,400 tokens and an attempt of 80-140 calls almost always met one.
//
// The controller stops an attempt after 60 minutes without a model response or
// tool event (server/runtime.ts idleTimeout). A failed call renews nothing, so
// budget x (1 + timeout repeats) + delays must stay inside that window.
export const HARNESS_CALL_BUDGET_MS = 1_080_000;
export const COMPACTION_BUDGET_MS = 1_800_000;
// The router's upstream has been seen answering 502 "upstream unavailable" for
// more than four minutes at a time; fast refusals cost only the wait, so ride
// out ~19 minutes before handing the attempt back to the scheduler.
export const MODEL_RETRY_DELAYS_MS = [15_000, 60_000, 180_000, 300_000, 600_000];
export const MODEL_TIMEOUT_RETRIES = 1;

// Internal refusals are raised as Error('SOME_CODE') or carry error.code; they
// are decisions (policy, auth, quota, size), not transport accidents.
const INTERNAL = /^[A-Z][A-Z0-9_]+$/;
const ACCOUNT = /quota|429|rate.limit|auth|credential|401|403/i;

// The router refuses any request whose text names these system files, answering
// 502 "upstream unavailable" within a second (reproduced 2026-10-27 with a
// one-line prompt). Once such text sits in a session's history every later call
// is refused the same way, so repeating only burns the attempt's time. The text
// is never altered to get past the filter; a fresh attempt starts a new session.
const PROVIDER_BLOCKED_TEXT = /\/etc\/(passwd|shadow|group)\b/;
function messageText(message) {
  const content = message?.content;
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.map((block) => (typeof block?.text === 'string' ? block.text : typeof block?.arguments === 'string' ? block.arguments : JSON.stringify(block?.arguments ?? ''))).join('');
}
/** Whether a refusal is the router's content block rather than an outage. */
export function providerContentBlock(error, messages = []) {
  return /\b502\b/.test(String(error?.message ?? '')) && /upstream unavailable/i.test(String(error?.message ?? '')) &&
    messages.some((m) => PROVIDER_BLOCKED_TEXT.test(messageText(m)));
}

/** Milliseconds to wait before repeating the call, or null when it must fail. */
export function modelRetryDelay(error, retry, { aborted = false, compaction = false, messages = [] } = {}) {
  if (aborted || retry >= MODEL_RETRY_DELAYS_MS.length) return null;
  if (providerContentBlock(error, messages)) return null;
  if (error?.name === 'TimeoutError')
    return !compaction && retry < MODEL_TIMEOUT_RETRIES ? MODEL_RETRY_DELAYS_MS[retry] : null;
  const message = String(error?.message ?? '');
  if (error?.code || INTERNAL.test(message) || ACCOUNT.test(message)) return null;
  return MODEL_RETRY_DELAYS_MS[retry];
}

/** Provider budget for one call; a configured larger budget still wins. */
export function harnessCallBudgetMs(configured, compaction) {
  return compaction ? COMPACTION_BUDGET_MS : Math.max(configured, HARNESS_CALL_BUDGET_MS);
}

/** What a refused call looked like, without its content: the provider's own
 * words, the last roles, and tool results whose call is no longer present
 * (compaction can summarize a call away while its result stays in the tail). */
export function failedCallDiagnostic(error, messages = []) {
  const calls = new Set();
  for (const m of messages)
    if (m?.role === 'assistant' && Array.isArray(m.content))
      for (const b of m.content) if (b?.type === 'toolCall') calls.add(b.id);
  return {
    detail: String(error?.message ?? '').slice(0, 300),
    messages: messages.length,
    lastRoles: messages.slice(-6).map((m) => m?.role).join(','),
    orphanToolResults: messages.filter((m) => m?.role === 'toolResult' && !calls.has(m.toolCallId)).length,
    providerContentBlocked: providerContentBlock(error, messages),
  };
}

// The harness appends its directive as the FINAL user message. Earlier
// checkpoints stay in the history as <compacted-summary> blocks, so the tag
// alone would mark every later ordinary call as a compaction.
const COMPACTION_DIRECTIVE = 'You are now acting as a compaction engine';
/** Whether a parsed Messages or chat/completions request is the summarization call. */
export function isCompactionRequest(request) {
  const last = request?.messages?.at?.(-1);
  if (last?.role !== 'user') return false;
  const text =
    typeof last.content === 'string'
      ? last.content
      : Array.isArray(last.content)
        ? last.content.map((block) => (typeof block?.text === 'string' ? block.text : '')).join('')
        : '';
  return text.trimStart().startsWith(COMPACTION_DIRECTIVE);
}

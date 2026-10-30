// Content-free observations only. These fields never authorize completion,
// tool execution, retries or recovery; the existing parser owns those decisions.
const finishReasons = new Set(['missing', 'stop', 'tool_calls', 'length', 'other']);
const validations = new Set([
  'accepted', 'unterminated_tail', 'missing_finish', 'invalid_finish',
  'finish_tool_mismatch', 'tool_id_missing', 'tool_id_duplicate',
  'tool_name_missing', 'invalid_tool_json', 'invalid_tool_shape', 'empty_message',
  'malformed_frame', 'malformed_json', 'data_after_done', 'provider_error',
  'output_limit', 'response_too_large', 'interrupted',
]);

export function diagnosticFinishReason(value) {
  return value === undefined || value === null || value === '' ? 'missing'
    : ['stop', 'tool_calls', 'length'].includes(value) ? value : 'other';
}

/** Re-copy a closed schema; never retain an observer caller's extra fields. */
export function sanitizeProtocolTerminal(value) {
  if (!value) return null;
  const {version,format,finishReason,validation,eofObserved,doneObserved,aborted,trailingBytes,toolCount}=value;
  if (version !== 1 || !['sse', 'json'].includes(format) ||
      !finishReasons.has(finishReason) || !validations.has(validation) ||
      ![eofObserved,doneObserved,aborted].every(v => typeof v === 'boolean') ||
      ![trailingBytes,toolCount].every(v => Number.isSafeInteger(v) && v >= 0)) return null;
  return {version:1,format,eofObserved,doneObserved,finishReason,trailingBytes,toolCount,validation,aborted};
}

export function observeProtocolTerminal(telemetry, value) {
  try {
    const safe = sanitizeProtocolTerminal(value);
    if (safe) Promise.resolve(telemetry?.protocolTerminal?.(safe)).catch(() => {});
  } catch { /* An observer cannot replace the parser's result or error. */ }
}

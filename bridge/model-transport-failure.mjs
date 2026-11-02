// Private provenance: arbitrary provider text/error JSON cannot opt into retry.
const failures = new WeakMap();
const rateLimits = new WeakSet();
// Called only after the HTTP adapter classified an explicit rejected response.
// Provider text, worker errors and serialized lookalikes cannot create this mark.
export function markModelRateLimitFailure(error) {
  if (error?.code === 'RATE_LIMIT' && error.status === 429) rateLimits.add(error);
  return error;
}
export function isModelRateLimitFailure(error) { return rateLimits.has(error); }
export function transportFailureReason(error) {
  const code = error?.cause?.code ?? error?.code;
  if (['ECONNRESET', 'EPIPE', 'UND_ERR_SOCKET'].includes(code)) return 'connection_interrupted';
  if (['ETIMEDOUT', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT'].includes(code))
    return 'connection_timeout';
  return null;
}
export function markModelTransportFailure(error, reason) {
  if (['connection_interrupted', 'connection_timeout', 'provider_unavailable'].includes(reason)) failures.set(error, reason);
  return error;
}
export function modelTransportFailure(error) { return failures.get(error) ?? null; }

// Observation identity is separate from the provider request body. The HTTP
// adapter's attempt counter may restart inside a repeated logical generation.
export function generationObserver(emit,{requestId,generationAttempt}) {
  const attempt=Number.isSafeInteger(generationAttempt)&&generationAttempt>=1?generationAttempt:1;
  return (kind,payload)=>emit(kind,{...payload,requestId,generationAttempt:attempt});
}

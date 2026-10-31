// Private provenance: arbitrary provider text/error JSON cannot opt into retry.
const failures = new WeakMap();
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
  const attempt=Number.isInteger(generationAttempt)&&generationAttempt>=1&&generationAttempt<=3?generationAttempt:1;
  return (kind,payload)=>emit(kind,{...payload,requestId,generationAttempt:attempt});
}

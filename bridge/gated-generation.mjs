import { assertModelRoute } from './model-policy.mjs';
import { randomUUID } from 'node:crypto';
import { acquireGlmRequestSlot } from './glm-request-slot.mjs';
import { requestModel, DeferredModelRequest } from './model-request.mjs';

export const GLM_ACCOUNT = 'agentos:api:00000000-0000-4000-8000-000000000001';
// Private in-process evidence that a caller already owns the permit. JSON/RPC
// callers cannot supply this identity, so they cannot bypass admission.
export const GLM_REQUEST_ADMITTED = Symbol('agentos.glm.request.admitted');

// A permit belongs to a complete provider call, including its streamed body.
// While queued only caller cancellation applies; the provider timeout starts
// after admission. Never bypass a missing or disconnected gate.
export async function withGlmRequestSlot(
  { runId, requestId, signal, emit = () => {}, socketPath, requestTimeoutMs },
  operation,
  dependencies = {},
) {
  const acquire = dependencies.acquire ?? acquireGlmRequestSlot;
  const monotonicNow=dependencies.telemetryNow??(()=>performance.now());
  let started=null,lastElapsed=0;
  try {const value=monotonicNow();if(Number.isFinite(value))started=value;}catch{}
  const queued = new AbortController();
  const upstream = new AbortController();
  const cancelQueue = () => queued.abort(signal.reason);
  signal?.addEventListener('abort', cancelQueue, { once: true });
  if (signal?.aborted) cancelQueue();
  let slot, lost, timer, lastQueueState;
  const safeEmit = (kind, state = {}) => {
    const payload = { runId, accountId: GLM_ACCOUNT };
    if(typeof requestId==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId))payload.requestId=requestId;
    payload.gateElapsedMs=null;
    try {
      const value=monotonicNow();
      if(started!==null&&Number.isFinite(value)){
        lastElapsed=Math.max(lastElapsed,value-started);payload.gateElapsedMs=Math.round(lastElapsed*1000)/1000;
      }
    } catch{}
    for (const key of [
      'active',
      'queued',
      'limit',
      'legacyReserved',
      'effectiveActive',
      'maxObserved',
      'position',
    ]) {
      if (Number.isFinite(state[key])) payload[key] = state[key];
    }
    try {
      Promise.resolve(emit(kind, payload)).catch(()=>{});
    } catch {
      /* Observation must not alter permits. */
    }
  };
  try {
    slot = await acquire({
      runId,
      socketPath,
      signal: queued.signal,
      onLost: (error) => {
        lost = error;
        upstream.abort(error);
      },
      onState: (state) => {
        if (state.state === 'acquired') {
          // The caller now cancels the upstream transport, not its permit. Keep
          // the permit until the transport/body has finished unwinding.
          signal?.removeEventListener('abort', cancelQueue);
          safeEmit('MODEL_REQUEST_ADMITTED', state);
        } else if (state.state === 'queued') {
          const queueState = `${state.position}:${state.legacyReserved}`;
          if (queueState !== lastQueueState) safeEmit('MODEL_QUEUE_WAIT', state);
          lastQueueState = queueState;
        } else if (state.state === 'lost') safeEmit('MODEL_REQUEST_GATE_LOST', state);
      },
    });
    const requestSignal = AbortSignal.any([upstream.signal, ...(signal ? [signal] : [])]);
    requestSignal.throwIfAborted();
    if (requestTimeoutMs) {
      timer = setTimeout(
        () => upstream.abort(new DOMException('Provider request timed out', 'TimeoutError')),
        requestTimeoutMs,
      );
      timer.unref?.();
    }
    const result = await operation(requestSignal);
    requestSignal.throwIfAborted();
    return result;
  } catch (error) {
    if (lost && !signal?.aborted) throw lost;
    if (upstream.signal.aborted) throw upstream.signal.reason;
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', cancelQueue);
    upstream.abort();
    if (slot) {
      slot.release();
      safeEmit('MODEL_REQUEST_RELEASED');
    }
  }
}

export function withAccountRequestGate(params, signal, operation, emit, dependencies, admission) {
  assertModelRoute(params.accountId, params.model);
  const original = operation;
  operation = (signal) => { assertModelRoute(params.accountId, params.model); return original(signal); };
  if (params.accountId !== GLM_ACCOUNT || admission === GLM_REQUEST_ADMITTED)
    return operation(signal);
  return withGlmRequestSlot(
    {
      runId: params.runId ?? randomUUID(),
      requestId: params.requestId,
      signal,
      emit,
      requestTimeoutMs: params.requestTimeoutMs,
    },
    operation,
    dependencies,
  );
}

export function requestGatedModel(gate, url, options, consume, dependencies = {}) {
  return withGlmRequestSlot(
    gate,
    async (signal) => {
      let started = false, response;
      try {
        response = await requestModel(
          url,
          { ...options, signal },
          gate.emit ?? (() => {}),
          { ...dependencies, beforeRequest: () => {
            // Recheck synchronously after admission and before EACH fetch,
            // including a retry after a transient 5xx/transport failure.
            const deferred = gate.beforeRequest?.();
            if (deferred !== undefined) throw new DeferredModelRequest(deferred);
            if (!started) { started = true; gate.onRequest?.(); }
          } },
        );
      } catch (error) {
        if (error instanceof DeferredModelRequest) return error.result;
        throw error;
      }
      try {
        return await consume(response);
      } finally {
        // Cancellation is also required when validation throws before consumption.
        // A for-await loop cancels its reader itself when body processing throws.
        if (!response.body?.locked) await response.body?.cancel().catch(() => {});
      }
    },
    dependencies,
  );
}

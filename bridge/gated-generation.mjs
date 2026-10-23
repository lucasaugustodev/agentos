import { assertModelRoute } from './model-policy.mjs';
import { randomUUID } from 'node:crypto';
import { acquireGlmRequestSlot } from './glm-request-slot.mjs';
import { requestModel, DeferredModelRequest } from './model-request.mjs';

export const GLM_ACCOUNT =
  process.env.AGENTOS_GLM_ACCOUNT_ID || 'agentos:api:00000000-0000-4000-8000-000000000001';

/** The GLM lane is the configured account or any GLM model, matching how
 * workforce-policy already identifies it in SQL. An installation that runs GLM
 * under its own account id must still spend the same six permits. */
export function inGlmRequestLane({ accountId, model }) {
  return accountId === GLM_ACCOUNT || /^glm[-.]/i.test(model ?? '');
}
/** Measured throughput varies by router, so the budget is configured, not
 * assumed. server/generation-timing.ts applies these same bounds and default
 * to the planner lane; a slower endpoint needs a larger budget, not a call cut
 * short with nothing to show for it. */
export function glmRequestBudgetMs() {
  const budget = Number(process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS);
  return Number.isSafeInteger(budget) && budget >= 60000 && budget <= 3600000 ? budget : 720000;
}
// Private in-process evidence that a caller already owns the permit. JSON/RPC
// callers cannot supply this identity, so they cannot bypass admission.
export const GLM_REQUEST_ADMITTED = Symbol('agentos.glm.request.admitted');

// A permit belongs to a complete provider call, including its streamed body.
// While queued only caller cancellation applies; the provider timeout starts
// after admission. Never bypass a missing or disconnected gate.
export async function withGlmRequestSlot(
  { runId, accountId = GLM_ACCOUNT, signal, emit = () => {}, socketPath, requestTimeoutMs },
  operation,
  dependencies = {},
) {
  const acquire = dependencies.acquire ?? acquireGlmRequestSlot;
  const queued = new AbortController();
  const upstream = new AbortController();
  const cancelQueue = () => queued.abort(signal.reason);
  signal?.addEventListener('abort', cancelQueue, { once: true });
  if (signal?.aborted) cancelQueue();
  let slot, lost, timer, lastQueueState;
  const safeEmit = (kind, state = {}) => {
    const payload = { runId, accountId };
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
      emit(kind, payload);
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
  if (!inGlmRequestLane(params) || admission === GLM_REQUEST_ADMITTED)
    return operation(signal);
  return withGlmRequestSlot(
    {
      runId: params.runId ?? randomUUID(),
      accountId: params.accountId ?? GLM_ACCOUNT,
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

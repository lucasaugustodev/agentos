// Queue wait and provider time are reported apart on purpose. A permit promises
// that the provider deadline starts after admission; collapsing both into one
// number is what hid a deadline that ran over the queue wait. Keep this module
// free of side effects so the invariant can be tested on its own.

// `gated` requests wait for a host-wide permit; ungated ones never queue, so
// their admission is their own start.
export function modelRequestTiming({ gated = true, now = Date.now } = {}) {
  let requestedAt, admittedAt;
  return {
    // Call when the request is handed to the gate, not when the socket opens:
    // bytes read before this point are ours, not the provider's.
    start() {
      requestedAt = now();
      if (!gated) admittedAt = requestedAt;
      return requestedAt;
    },
    // The gate announces admission; mirror it instead of guessing.
    observe(kind) {
      if (kind === 'MODEL_REQUEST_ADMITTED' && requestedAt && !admittedAt) admittedAt = now();
    },
    // Empty before `start`: a failure that precedes the request has no provider
    // timing to report, and reporting zeros there would read as an instant reply.
    sample() {
      if (!requestedAt) return {};
      const at = now();
      return {
        durationMs: at - requestedAt,
        queuedMs: (admittedAt ?? at) - requestedAt,
        // Null, not zero: never admitted means the provider was never asked.
        providerMs: admittedAt ? at - admittedAt : null,
      };
    },
  };
}

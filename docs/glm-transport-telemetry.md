# GLM transport observations

The inspected completions adapter emits two bounded observations per actual
fetch attempt. They do not authorize delivery acceptance, change model settings,
retry a call, renew the semantic watchdog, or release a request permit.

`MODEL_WIRE_REQUESTED` records a whitelist derived from the serialized request:
model, stream, output ceiling, reasoning effort, thinking type, clear-thinking
flag, request byte count, message count and tool count. It records what this
adapter sends, not proof that a proxy honors the parameters or uses that model.
DSH session defaults may differ from these effective wire settings.

`MODEL_TRANSPORT_FINISHED` records the attempt number, terminal outcome, typed
error code, numeric HTTP status, body format, counters and timing offsets. All
offsets use a monotonic clock with fetch start as zero:

- headers received;
- first nonempty body chunk;
- first and last observed semantic delta;
- end of body, if reached normally;
- completion after transport cleanup.

Keepalives, usage frames and empty deltas do not count as semantic progress.
Body arrival is distinct from semantic progress. JSON replies have body timings
but no observed per-delta semantic timestamps. Failure or cancellation may leave
body-end and other unobserved timestamps null. A retry outcome describes one
finished transport attempt, not completion of the overall model request.

Usage preserves the provider's reported input, output, total, cached-input and
reasoning counters. Missing or invalid counters are null; the entire usage
field is null when none was reported. These counters may omit reasoning tokens.
Tokens divided by body time measure reported delivery throughput including
pauses, not GPU decoding speed. Header delay does not separate network latency,
upstream queueing or prefill.

The bridge attaches the same requestId to wire/transport and gate events. Direct
generate calls allocate it before admission; worker requests retain their
previously announced identity. Gate observations also include gateElapsedMs,
measured from entry into the gate helper. Historical callers without a valid
request UUID retain their previous run-level events; no identity is invented
after admission. Attempt numbers distinguish transport retries within a request.

No prompts, messages, tool names, arguments, results, raw provider errors, URLs,
headers or credentials are emitted. Each transport payload is capped at4096
bytes before the bridge attaches its fixed-size request UUID. Events carry
schema version1 and adapter version `openai-completions-fallback-v1`.
Throwing/rejecting observers or failed telemetry clocks cannot change provider
results, retry decisions or gate ownership. Telemetry is best-effort: an absent
receipt is not proof that no provider effect occurred.

Offline regression tests use mocked transports and clocks. They verify the
effective GLM thinking/effort/output transformation, gate identity, keepalive
separation, success/error/retry/cancellation, cleanup ordering, unavailable
usage, field-size bounds and secret exclusion. No live model or database is
required for these tests.

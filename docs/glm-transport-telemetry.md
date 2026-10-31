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

An optional `protocolTerminal` object preserves schema version 1 and explains
the parser's terminal decision using only whitelisted enums, booleans and
integer counts. It distinguishes an unterminated frame tail, missing or invalid
finish reason, finish/tool mismatch, missing or duplicate tool identity,
missing tool name, invalid argument JSON or shape, and body interruption. It
also records whether normal body EOF and `[DONE]` were actually observed.
Cancellation does not imply EOF. An observed terminal marker is insufficient
to accept incomplete arguments, and a valid terminal response without `[DONE]`
remains accepted as before. The downstream SDK's `STREAM_CLOSED` error alone
does not establish which condition the upstream parser rejected.

`validation: accepted` means only that the model response passed its existing
protocol checks; it grants no tool, retry, stop, task or delivery authority.
The object contains no tool identifiers, names, arguments, body fragments or
provider strings. Worker and cycle events preserve it with the request and
attempt identity; the progress tracker does not treat it as liveness. Offline
fixtures test both strict parsing and the parser-to-bridge-to-cycle-to-PostgreSQL
event path in an isolated test database.

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

Buffered worker generations retain one logical `requestId`. A privately
classified connection failure or HTTP 500/502/503/504 may repeat the generation
at most twice before the ordinary terminal failure path. `generationAttempt`
distinguishes these three generations; transport `attempt` remains the HTTP
attempt within each one. `MODEL_GENERATION_RETRY` preserves each failed
generation before the next admission. Each generation releases and reacquires
the shared gate; the global limit remains six concurrent requests. This permits
at most nine fast HTTP fetches (three per generation), or three fetches when a
late 502 already exhausted the adapter's 30-second retry budget. Provider bodies
and headers do not carry these observation fields. GLM retains its semantic
idle watchdog; a progressing first generation gains no new wall-clock cutoff.
The existing no-replay barrier after any provider body bytes remains in force,
including incomplete frames and keepalives. No retry can publish a partial response, replay earlier tools, ignore caller
cancellation, or proceed with unresolved tool identities. Exhaustion still
uses the existing task recovery path; task-level continuation is not introduced.
An outstanding positive `Retry-After` on a server error forbids the outer
generation retry. The typed wait is preserved, including HTTP-date values;
it is never shortened to the gateway's local backoff.

No prompts, messages, tool names, arguments, results, raw provider errors, URLs,
headers or credentials are emitted. Each transport payload is capped at4096
bytes before the bridge attaches its fixed-size request UUID. Events carry
schema version1 and adapter version `openai-completions-fallback-v1`.
Throwing/rejecting observers or failed telemetry clocks cannot change provider
results, retry decisions or gate ownership. Telemetry is best-effort: an absent
receipt is not proof that no provider effect occurred.

Analytical runtime calls also forward bridge events, including each generation
inside a manager review. The synchronous bridge callback queues async observer
writes in order and flushes them before returning that generation's result or
error. A rejected observer does not suppress later observations or retry the
model. Manager read/completion notifications remain a separate awaited channel;
transport events are not re-emitted through that channel. These observations do
not change delivery parsing, acceptance, request limits or remote stop evidence.

Offline regression tests use mocked transports and clocks. They verify the
effective GLM thinking/effort/output transformation, gate identity, keepalive
separation, success/error/retry/cancellation, cleanup ordering, unavailable
usage, field-size bounds and secret exclusion. No live model or database is
required for these tests.

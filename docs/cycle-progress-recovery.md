# Cycle progress and recovery

The cycle runtime must distinguish an execution that is advancing from a worker
that is merely alive. A fixed one-hour execution timer previously cancelled both
cases and reported `PROVIDER_TIMEOUT`, even when the provider was responding and
the worker was producing tools and files.

## Current contract

- Cycles use the same one-hour inactivity deadline as ordinary task executions.
  Verified model responses, tool events and semantic stream growth renew it.
  Heartbeats, queue messages and request-start messages do not.
- The actual cycle attempt fence accompanies `worker.execute`. Runtime checkpoints
  and the existing narrow progress watchdog can therefore bind their evidence to
  the current attempt. A cycle does not impersonate an objective/task identity.
- Expiration requests cancellation and reports `RUNTIME_IDLE_TIMEOUT` after the
  remote cancellation acknowledgement. A missing acknowledgement remains
  `STOP_UNCONFIRMED`; a timeout does not prove physical termination.
- A returned delivery still requires stopped evidence, immutable artifacts and
  the original acceptance criteria. A diagnostic checkpoint is not a delivery.
- Already dispatched attempts retain their original timer and envelope. Worker
  adoption must drain active attempts and verify the new process and release.

`tests/cycle-runtime-progress.test.ts` exercises the real cycle runtime and bridge
with a controlled clock and child-process transport. It covers progress past one
hour, heartbeat/queue-only expiry, cancellation confirmation, late progress after
cancellation, missing stop evidence, and typed progress-stall failures.

## Remaining limits

Tool activity is not the same as useful delivery progress. The existing watchdog
detects specific repeated failed-evidence polling, not every semantic loop.
An initial queue wait exceeding one hour can still exhaust the inactivity limit.
These cases need separate queue-admission and progress policies, rather than
treating heartbeats as useful work or removing all limits.

## Improvement loop

For each operational failure, retain the attempt identity, observed symptom,
causal evidence and acceptance criteria. Turn the smallest reproducible framework
defect into a regression test, validate a candidate in an isolated environment,
publish through the release guard, and check adoption before measuring outcomes.

Measure accepted deliveries and time to acceptance, operator recovery actions,
false termination of advancing attempts, time spent without useful progress, and
repeated failures of the same cause. Active session counts alone are not success.

The next recovery increment should expose failed-attempt checkpoints as unverified
inputs only after physical stop proof and hash/fence validation. A supervisor may
use them to propose a scoped recovery. It must preserve the original failure,
criteria and independent QA, and must not promote incomplete files through the
completed-delivery collector. Unconfirmed termination must retain reconciliation
and prevent a duplicate writer.

## Release adoption

Publication and worker readiness must hash the same immutable file set. Generated
Python caches are excluded from both digests and from source/release copies;
source, documentation, scripts and built assets remain signed. The parity tests
execute both digest implementations and verify that real file changes still stop
publication before the swap. Python composition fixtures run with `-B` so test
execution does not add bytecode to the source tree.

## Structured occurrence input

Each cycle attempt receives its complete external input as `cycle-input.json`
through the existing hashed input transport. Its context manifest records the
attempt/fence, occurrence, size, digest and sandbox path. The prompt references
that file instead of truncating large JSON or requiring history reconstruction.
Only complete inputs up to 4 KiB are also included inline. The existing 8 MiB
per-file and 24 MiB combined transport limits still apply; excess is an explicit
failure. Payloads are untrusted data: delivery of the file does not execute a
command, apply a patch or grant any additional authority.

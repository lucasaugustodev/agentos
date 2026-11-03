# NativeQA v3: controller evidence in the continuity watcher

New executions may opt in with `nativeQAContractVersion: 3`. Absent, v1 and v2
definitions retain their original bytes and reconciliation behavior. The v3
producer publishes the same artifacts and implements the positional CPU checker
ABI. Its single QA step receives controller-owned CPU evidence and writes only
a semantic report. It does not copy hashes, result arrays or shell transcripts.

The watcher consumes the authenticated, read-only `cpu-replay` API. It checks
the original protected registration and executor pins against the pinned policy,
the frozen producer and consumer identities, artifact inventories and bytes,
request digest, immutable receipt, handoff and persisted semantic gate. A fresh
physical inspection must confirm the same CPU unit stopped; its observation
timestamp is excluded from planning fingerprints only after validation.

This is a consumer of the core's authenticated evidence, not an independent
launcher verification. In particular, resolution of `baseSource.source` and the
cycle-input origin is performed by the core and bound into the original request
digest. Changing those fields without the original digest fails closed. The
native CHECK adapter separately verifies the launcher and the complete source,
patch, canonical materials and policy before any official action.

Completed CPU failure is distinct from a model stop. An authenticated
`cpu-preparation-terminal` receipt with `modelDispatched: false` reaches
supervision with the specific CPU failure and original references. It never
becomes a fabricated `HARNESS_STOPPED`, QA artifact or acceptance. Incomplete or
unavailable evidence retains ownership. Completed approved candidates remain
reserved while awaiting official measurement.

A valid semantic rejection requests a new hypothesis. A malformed original QA
report can be supplied as historical evidence for a fresh bounded repair: the
diagnostic uses `report: null`, the explicit invalid-report state, original
artifact metadata and controller bindings. Its complete unchanged bytes travel
as `prior-qa-original.json`; they are not reparsed permissively or rewritten into
valid QA. Existing immutable candidate/patch, attempt limits, independent QA and
official transport gates still apply.

Qualification includes legacy definition golden hashes, GET-only lost-ACK
reconciliation, source/fence/digest tampering, fresh/stale physical proof, CPU
failure without a model call, invalid JSON and schema, raw repair attachments,
planning readsets, ownership and registered official receipts. Separate frozen
replays use actual protected registrations, real non-root CPU execution and the
core engine/API. Their semantic provider is explicitly synthetic; these prove
protocol interoperability, not independent semantic quality or GPU performance.

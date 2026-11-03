# NativeQA v3 controller evidence (development, disabled by default)

This explicit contract separates a controlled CPU execution from independent
semantic review. It does not change NativeQA v1/v2, install an executor, enable a
cycle, grant CHECK permission or demonstrate a GPU/performance improvement.

The two-step definition keeps one producer attempt and one QA attempt. QA uses:

```json
{
  "maxAttempts": 1,
  "parameters": {
    "nativeQAContractVersion": 3,
    "cpuReplayObservationVersion": 1,
    "cpuReplayProfile": "python-cpu-candidate-base-v1"
  },
  "dependencyEvidence": {
    "version": 2,
    "kind": "controller-cpu-replay",
    "sourceStep": "produce",
    "reportArtifact": "qa-verification.json"
  }
}
```

The only allowed action is `native.cpu-replay.observe`. A separately protected
executor registration is still mandatory. The dependency snapshot remains
version 1; its contract digest is SHA256(JSON.stringify of
`[2,"controller-cpu-replay","produce","qa-verification.json"]`). The producer
inventory, source attempt/fence and consumer attempt/fence are frozen as before.
Preparation/transport recovery stays on the same QA attempt. A terminal failure
keeps its diagnostic and original artifacts; it does not retry the producer or
silently reuse a receipt for a new consumer.

## Original semantic report

QA reads `cpu-replay-observation.json`, checker, source and raw CPU result. The
model's `qa-verification.json` is preserved byte for byte; the controller never
generates this report or inserts missing hashes, cases or commands.

```json
{
  "version": 3,
  "decision": "approve_for_check",
  "scopePreserved": true,
  "canonicalPrecisionPreserved": true,
  "evaluatorIndependent": true,
  "upstreamAttributionPreserved": true,
  "checkerReview": {
    "casesMeaningful": true,
    "negativeControlsDiscriminating": true,
    "noTautologicalPass": true,
    "rationale": "Independent explanation based on the supplied checker and results."
  },
  "blockingFindings": [],
  "findings": [],
  "limitations": ["CPU emulation does not prove GPU correctness or performance."],
  "gpuValidated": false,
  "performanceGainProven": false,
  "officialActionsPerformed": false
}
```

`decision` may instead be `reject`; booleans can be false. Approval requires all
review booleans true and no blocking findings. Unknown fields, duplicate keys,
nonfinite numbers, invalid field types and false official claims are rejected.
Each string is bounded to 8,000 characters and each list to 100 items. At least
one explicit limitation is required.

Before the QA model is dispatched, the technical gate requires a complete protected receipt, exit 0,
null signal, no timeout/OOM/cancellation/output overflow, present raw result and
fresh physical stop. The raw CPU JSON must be finite/unambiguous, `passed:true`,
bind exact candidate/checker SHA256 values, and contain nonempty uniquely named
`cases` with `passed:true` and `negativeControls` with `detected:true`. Extra
details in these rows are preserved. This verifies the reported execution;
independent QA remains responsible for detecting meaningless/tautological tests.
Patch applicability and official policy remain separate NativeAudit gates.
Technical failures persist `context.nativeQA3PreparationFailure` and an attempt
receipt `{kind:"cpu-preparation-terminal",modelDispatched:false,cpu:{requestId,
requestSHA256,receiptSHA256,stopProof}}`. This is a CPU termination proof, never a
claim that a nonexistent QA model stopped. The original CPU output remains
available to supervision; no semantic report is fabricated and no QA starts.

## Read-only evidence endpoint

`GET /api/cycle-runs/:occurrenceId/attempts/:attemptId/cpu-replay` uses the existing
API authentication. It does not modify `occurrenceDetail` or its legacy hashes.
It reconstructs the request from authenticated artifact bytes, checks the
`cycle_action_receipts` row and invokes **verify**, never execute/inspect/cancel.
The new Python verify mode reads an existing complete protected receipt and
physical unit state without writing locks, admissions, intents or tombstones.

The response envelope is bounded to 2 MiB. Common fields are:

- `version:1`, `kind:"native-qa3-controller-evidence"`, `state`;
- `occurrenceId`, `attemptId`, `runFence`, `definitionVersion`;
- `request`: exact executor request metadata with each artifact's base64 omitted;
- `requestSHA256`, `requestCanonicalization:"cpu-replay-ascii-integers-v1"`;
- `registration:{path,digest,executorPath,executorSHA256,profile}`;
- `snapshot`: controller-pinned dependency inventory;
- `action:{key,state,inputHash}`, `modelDispatched`;
- `semanticReport`: original artifact `{id,name,digest,size}` or null;
- `validation`: persisted record described below, or null.
- `cycleInput`: original raw UTF-8 input handoff reference, or null;
- `preparationFailure`: persisted technical rejection, or null.

`state:"evidence"` additionally has the exact complete `receipt`,
`receiptSHA256`, `receiptCanonicalization`, `physicalConfirmed:true`,
`diagnostic` (null or the preparation failure), and `handoff` (null before model dispatch). A handoff is:

```text
{version:1, contractVersion:3, requestId, requestSHA256, receiptSHA256,
 receiptCanonicalization, registration,
 receipt:{id,name,digest,size,path}}
```

Its receipt reference binds the canonical bytes actually staged read-only for
the QA attempt. `physicalInspection` is
`{version:1,observedAt,receiptSHA256,stopProof}`. The observation is fresh (at most
15 seconds, bounded clock skew); it is **not** part of a stable action/read-set
fingerprint. Consumers must verify freshness before excluding its timestamp.

`state:"diagnostic"` exposes incomplete/waiting/cancelled action state without
claiming execution: receipt, receiptSHA256, handoff and physicalInspection are
null; physicalConfirmed is false; diagnostic is `{code,actionState,terminal}`.
The terminal object is the original DB diagnostic, not a new physical-stop
proof. `modelDispatched:false` does not imply that no CPU tool was dispatched.
Protected file or original registration verification failure returns a typed
error, never an eligible substitute or model-supplied receipt.

## Registration history and final gate

`context.cpuReplayPreparation.registration` pins the original registration
path/digest/executor/profile. Verification accepts that exact current
registration or an exact entry in the root-protected file named by
`AGENTOS_CPU_REPLAY_VERIFICATION_REGISTRATIONS`, schema
`{version:1,registrations:[<binding>]}` (at most 32 entries). A different current
registration does not rebind historical evidence. The adapter must independently
allow the original pins; an allowlist does not retroactively upgrade shadow
observations to NativeQA3.

`baseSource.inputSHA256` is the SHA256 of the original raw UTF-8
`cycle-input.json` bytes produced by ECMAScript JSON.stringify. After handoff it
must equal `context.manifest.cycleInput.digest` and the authenticated raw input
bytes, not a Python reserialization or the base candidate hash. QA base aliases
require a completed/stopped QA with an explicit frozen producer snapshot and
matched gate, and byte identity with the completed/stopped producer candidate
in the same occurrence/project/fence. The alias's actual reference and actor
remain in baseSource; missing legacy snapshots fail closed.

`context.manifest.nativeQA3` is the handoff above.
`context.nativeQA3Validation` contains:

```text
{version:1,contractVersion:3,status:"matched"|"rejected",code:null|string,
 snapshotContractSHA256,source,requestSHA256,receiptSHA256,
 receiptCanonicalization,handoff,semanticReport,semanticDecision,
 candidateExecutedByController:true,gpuValidated:false,performanceGainProven:false}
```

The raw QA report remains separate. Semantic rejection persists this binding
with status rejected and retains the failed QA/stop receipt for diagnosis and
source provenance. Final completion rechecks attempt/fence, producer snapshot,
human state, original action receipt, handoff and original QA artifact under the
database lock. Neither API evidence nor matched metadata alone permits CHECK:
the consumer must also validate the completed/stopped QA, semantic report,
candidate/base/patch, all applicable policy and official preflight.

The initial implementation remains a development contract until server tests,
cross-language full NativeAudit fixtures, registered full CI and an explicitly
reviewed limited rollout are complete. Existing admission retention and CPU
capacity limits still apply.

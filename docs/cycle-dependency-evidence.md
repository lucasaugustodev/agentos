# Cycle dependency evidence v1

An agent step can opt into a deterministic check that its report names the
SHA-256 digests of artifacts from one completed, direct agent dependency:

```json
{
  "key": "independent-qa",
  "dependsOn": ["produce"],
  "maxAttempts": 3,
  "parameters": {"nativeQAContractVersion": 2},
  "dependencyEvidence": {
    "version": 1,
    "sourceStep": "produce",
    "bindings": [
      {"reportArtifact": "qa-verification.json", "jsonPointer": "/cpuEvidence/checkerSHA256", "dependencyArtifact": "cpu-checks.py"},
      {"reportArtifact": "qa-verification.json", "jsonPointer": "/cpuEvidence/resultSHA256", "dependencyArtifact": "cpu-result.json"}
    ]
  }
}
```

This is a cycle step contract, not a new ordinary task criterion. Existing
artifact and semantic criteria remain mandatory. The source must be a direct
agent dependency. A step using this contract cannot declare `repairStep`.
NativeQA v2 additionally requires one producer attempt and at most three QA
attempts. Definitions without this field retain the existing behavior.

Before dispatch, the runtime freezes the completed source step, attempt, fence
and full artifact inventory. The stopped producer receipt is required. The
snapshot is stored in `context.manifest.dependencyEvidence`:

- `version`, `contractSHA256`, `occurrenceId`;
- `consumer` and `source`: `stepId`, `key`, `attemptId`, `attemptNumber`, `runFence`;
- `artifacts`: ordered `{id, name, digest, size}` entries.

The contract hash is SHA-256 of UTF-8 JSON for
`[version, sourceStep, bindings.map(b => [b.reportArtifact, b.jsonPointer, b.dependencyArtifact])]`.
It does not depend on database JSON object key ordering. The exact pinned source
artifacts are staged in the worker inputs. Historical occurrence input remains
available as data; it cannot satisfy this gate.

After the worker stops and publishes artifacts, ordinary delivery criteria run
first. The dependency gate then revalidates every source artifact's bytes and
metadata, loads the consumer report (at most 1 MiB), and resolves each JSON
pointer without coercion. Duplicate JSON keys, nonfinite numbers, malformed
SHA-256 strings, absent fields and changed source evidence fail closed. Source
and consumer identity are checked again in the completion transaction.

The attempt stores `context.dependencyEvidenceValidation` with `version`,
`status` (`matched` or `mismatch`), `source`, and `bindings`. Each binding includes
the actual report artifact reference, JSON pointer, dependency artifact reference,
`expected` digest and `received` digest. No report or digest is rewritten.

Only a well-formed SHA mismatch produced by this gate, with a confirmed stopped
worker, schedules another attempt (`DEPENDENCY_ARTIFACT_BINDING`). The failed
attempt, stopped receipt and artifacts remain intact. The next attempt gets the
structured feedback and the same producer identity/inventory. Only QA runs
again. Changed producer evidence, cancellation, uncertain termination, schema
errors and numerical/semantic failures do not receive this automatic retry.
Confirmed stop after an uncertain attempt also requires attention rather than
becoming a binding retry. Retry exhaustion retains the failed evidence.

Consumers such as the OpenKernel adapter must explicitly recognize NativeQA v2:
one completed producer, contiguous QA attempts within the limit, prior QA attempts
failed only with the binding code and stopped receipts, identical producer
snapshots, and independent validation of the final report. A matching dependency
hash alone is not correctness or authorization for an official submission.

Qualification uses the published source baseline with only this patch overlaid,
an isolated network namespace and a fresh PostgreSQL instance. The regression
suite covers wrong historical hashes followed by a correct QA, preserved producer
and failed artifacts, exhaustion, semantic failures, malformed reports, changed
bytes, source/consumer fences, cancelled and uncertain execution, and legacy
definitions. No production queue, credentials or model calls are used.

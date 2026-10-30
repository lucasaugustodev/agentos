# Canonical source delivery validation

Versioned `execution.sourceContract` is frozen at admission. It adds mandatory
checks to ordinary task acceptance; a passing worker report cannot override a
missing file, a wrong required SHA-256 or a regression of preserved source.
Existing runs without the contract retain their original validation semantics.

For opted-in managed executions the runtime inspector publishes `baseFiles` in
canonical `candidate.json`, alongside the existing `files`, `baseDigest` and
`candidateDigest`. It reads `baseFiles` from the protected workspace baseline
after confirmed executor termination. Evidence and temporary directories remain
excluded from both source inventories. Source prepared under `evidence/` must be
materialized to its authorized canonical source path before delivery; collecting
the report does not promote it automatically.

The controller verifies the immutable artifact reference and recomputes both
inventory digests before checking required files/directories and preservation.
A preserved directory retains every prior file and permits additional files.
Preservation of an absent baseline path fails. Importing an absent known source
uses a required file with its exact hash, not a preservation claim.

Validation identity includes the frozen and current contracts and source
workspace identity only for opted-in runs. A changed/withdrawn contract refuses
acceptance; validation recaptures identity under the existing commit lock after
artifact I/O. Historical receipts and snapshots are not rewritten. Runtime
recovery carries the same inventory option, so a recovered result must satisfy
the same acceptance gate.

These checks prove source packaging and preservation, not behavioral correctness
or benchmark speed. Original tests, registered CI and independent QA remain
required. A failed source check preserves the canonical artifacts for repair.
The text fallback accepts a complete JSON answer or one explicit JSON fence.
It refuses multiple envelopes and any object outside that fence, including an
unfinished later answer; it never selects the last balanced example as evidence.
Workers can use the existing run-bound delivery manifest to recover real output
without depending on their final prose. Neither packaging path grants acceptance.

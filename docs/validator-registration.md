# Registered validator provenance

A publication, the executor that validated it, and the currently registered
executor have separate identities. Updating the release symlink does not register
a new privileged executor. Workers cannot choose executable paths or authorize a
registration. The migration source is the exact public script from commit
`d52ef6b603e637bf301ac1ab0360a72dfd6912a6`, SHA256
`02a92158a4084a1aeb3b4c55998b6740d90428f80acb06a225730a1423ba9c26`.
The separately reviewed coverage executor from commit
`708a0670c99bcd57f5d7353e8aabf02b276fca7e` is also recognized by exact SHA256
`31e7aa59d7b11f3552cfd8499481c727e4a8b316fc5b952e45839ba3df2a9c72`.

`scripts/validator_provenance.py inspect` reads the active immutable publication.
It accepts two explicit formats, without manufacturing historical sidecars:

- Operator guard receipts: matching `release-input.json`, validate/publish
  intents, their actual output receipts, and the native publication record under
  `/var/lib/agentos/releases`. Attempt, occurrence, base, times, digest and the
  historical executor bytes must agree. These files remain unchanged.
- Native broker receipts: the original request, completed result and its digest,
  reviewed execution registration, and the guard's subsequently published record.
  This recognizes later releases automatically when they retain reviewed executor
  bytes. A boolean or log without the bound publication record is insufficient.

An operator reviews the inspection result, then calls `register` as root with a
JSON object containing `registrationId`, `expectedRegisteredSha256`,
`expectedReleaseDigest`, and `expectedPublicationProofDigest`. Values must be
copied from the reviewed inspection, not calculated by a worker. The registrar
accepts only executor versions compiled into `REVIEWED_EXECUTORS`; a future
executor change needs another reviewed source change and explicit transition.
The legacy hash and its known cache-only variant are migration preimages, not
permission to run an arbitrary existing file.

Registration holds the publication, validation and registration locks, refuses
uncertain CI ownership, verifies the preimage, records an immutable intent, and
atomically installs the reviewed bytes. It then seals a versioned receipt and
updates a root-owned pointer. The service and workers cannot write this registry.
An interruption after replacement leaves admission closed; replay the same
registration ID and unchanged intent to reconcile. Do not delete the intent,
change its hashes, or launch a second CI command.

The broker verifies this registration before executing CI. Historical CI receipts
retain their original executor hash. Python caches do not affect the release
digest; source, documentation and built assets do. Missing, changed or unprotected
proof remains an error, never an inferred successful publication.

Validation logs may contain only the registered executor's final 100000
characters. Such a suffix is identified through its native guard receipt; it is
not represented as a full build log. Generic test totals do not attest execution
of required database cases. That coverage needs a separately bound structured
report; this migration does not add coverage to historical results or accept a
task merely because CI completed.

For a worker-authorized validation, required database cases come from the
authenticated admission-policy snapshot. The broker passes that exact list and
candidate digest to the registered executor. New coverage is accepted only with
the integral `vitest-results.json` and `validation-coverage.json`, their hashes in
the native result, the exact build root for this validation attempt, and the same
obligations digest. The exporter independently derives each required case status
from `(file, fullName)`. Missing, failed and skipped required cases stay negative;
unrelated optional skips do not invalidate otherwise complete coverage. No model
summary, filename guess or overall passed count substitutes for this proof.

Run `tests/validator-provenance.test.ts` and the existing candidate validation,
evidence export, published rebase and release digest tests on an isolated server.
The fixtures use temporary trees, never a production database or registry.

# Registered validator provenance

A publication, the executor that validated it, and the currently registered
executor have separate identities. Updating the release symlink does not register
a new privileged executor. Workers cannot choose executable paths or authorize a
registration. A historical public script is preserved from commit
`d52ef6b603e637bf301ac1ab0360a72dfd6912a6`, SHA256
`02a92158a4084a1aeb3b4c55998b6740d90428f80acb06a225730a1423ba9c26`.
It is historical evidence, not an admissible executor for new work. The
intermediate `31e7…` executor was never activated and is not admissible either.
`REVIEWED_BUNDLES` contains only the final reviewed guard plus its exact
`candidate_origin.py`, with their source commit and byte hashes.
The initial bundle is commit `084e0459a7d045c45c22b8159c60dd5742df2acb`,
guard SHA256 `39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44`
and origin-module SHA256
`742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f`.

The capacity bundle from commit `d65e4801eef3412b7ff234fbf81c618acd512373`
has guard SHA256 `cf4351977a994457dd0b74da8449c05597479ba4410e3015e499d7359d094383`.
Its only guard change raises the native operator coverage limit from 128 to 256
exact `(file, fullName)` obligations. The origin module is unchanged. Larger
lists, duplicates, invalid names and paths remain rejected; no list is truncated.
This does not change the separate worker-admission policy limit of 128.
Register the new bundle through the procedure below, preserving previous
registration and failed-attempt receipts. Releases must include this registrar
identity before they consume a publication produced by the new executor.

`scripts/validator_provenance.py inspect` reads the active immutable publication.
It accepts two explicit formats, without manufacturing historical sidecars:

- Operator guard receipts: matching `release-input.json`, validate/publish
  intents, their actual output receipts, and the native publication record under
  `/var/lib/agentos/releases`. Attempt, occurrence, base, times, digest and the
  historical executor bytes must agree. These files remain unchanged.
  Modern intents also declare the actual executor path, SHA, source commit and
  immutable registration reference. Modern receipts cannot infer an executor
  from an older release merely because that was the validation base.
- Native broker receipts: the original request, completed result and its digest,
  reviewed execution registration, and the guard's subsequently published record.
  This recognizes later releases automatically when they retain reviewed executor
  bytes. A boolean or log without the bound publication record is insufficient.

An operator reviews the inspection result, then calls `register` as root with a
JSON object containing `registrationId`, `expectedRegisteredSha256`,
`expectedReleaseDigest`, and `expectedPublicationProofDigest`. Values must be
copied from the reviewed inspection, not calculated by a worker. The registrar
accepts only executor bundles compiled into `REVIEWED_BUNDLES`; a future
executor change needs another reviewed source change and explicit transition.
The legacy hash and its known cache-only variant are migration preimages, not
permission to run an arbitrary existing file.

For the first hardened bundle, add `executorSha256` and `sourceCommit` to that
request. The registrar derives the only permitted staging directory:
`/opt/agentos-control/reviewed-validator-bundles/<executorSha256>`. An operator
places the exact reviewed public files there under protected root ownership.
There is no model-supplied source path or command. The record honestly declares
`sourceKind: reviewed_public_source`; it anchors to the actual current
publication without claiming the new executor has already been published.
This explicit registration precedes first use of the new guard and avoids a
publication/registration cycle. Both validation and publication then use the
normal registered bundle and candidate-origin checks, with no bootstrap escape.

Registration holds the publication, validation and registration locks, refuses
uncertain CI ownership, verifies the preimage, records an immutable intent, and
installs the reviewed module before the entrypoint using atomic replacements.
It then seals a versioned receipt including the exact bundle manifest and
updates a root-owned pointer. The service and workers cannot write this registry.
An interruption after replacement leaves admission closed; replay the same
registration ID and unchanged intent to reconcile. Do not delete the intent,
change its hashes, or launch a second CI command. A lost reply after installation
replays the same receipt; it cannot roll back a newer registration pointer.
The direct cycle executor checks the installed bundle too. A service-writable
module, changed helper or incomplete transition is a closed admission gate.

The root-owned registration archive preserves the exact publication evidence
and reviewed bundle bytes with their original references and SHA256 values.
Garbage collection of an operator's build folder cannot silently invalidate
historical registrations. The archive does not rewrite source receipts.
Existing operator receipts can also be copied byte-for-byte, retaining mtimes,
under `/var/lib/agentos-operator-releases/<operation>/`; the parser recognizes
their original format there. It does not trust a service-owned ancestor merely
because an individual leaf used to be root-owned.

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
the native result, the exact root-owned build root
`/var/lib/agentos-validation-builds/<attemptId>`, and the same
obligations digest. The exporter independently derives each required case status
from `(file, fullName)`. Missing, failed and skipped required cases stay negative;
unrelated optional skips do not invalidate otherwise complete coverage. No model
summary, filename guess or overall passed count substitutes for this proof.

The broker observes the exact validation systemd unit independently after the
guard exits. A killed guard, truncated reply or failed stop check does not prove
that its CI process stopped. Such ownership remains uncertain and cannot
authorize another invocation of the same action.

Run `tests/validator-provenance.test.ts` and the existing candidate validation,
evidence export, published rebase and release digest tests on an isolated server.
The fixtures use temporary trees, never a production database or registry.

# Candidate origin and publication

Passing CI proves checks on measured candidate bytes. It does not authorize
replacing the current release with source prepared before newer fixes existed.

The registered executor captures `candidateOrigin` with the candidate input
digest it measured. The proof comes from existing controller preparation:

- The source baseline under `/var/lib/agentos-bridge/workspace-baselines/`,
  outside worker sandboxes, must match the prepared snapshot's `baseDigest`.
- The root-owned `.agentos-snapshot.json` is mounted read-only by managed
  runtimes. Its source identity links prepared consumers to producer snapshots.
  New preparations also pin the exact parent marker hash. A reused parent
  workspace name cannot transfer a newer origin to an older consumer. Legacy
  chains without this generation binding require rebase; only an exact direct
  match to current source bytes remains sufficient without a parent pin.
- The ancestry must reach actual baseline bytes equal to the active published
  source. The only normalization is the exact `.gitignore` addition performed
  by `workspace-manager.prepare`. Source names or model-provided hashes do not
  prove this equality.
- The active release bytes, native publication receipt and source manifest are
  bound. No historical receipt gains a fabricated origin after publication.

The bounded ancestry check preserves accepted changes copied through producer
workspaces. A changed source file in the candidate is allowed; this proof is
ancestry, not a claim that the changes preserve every behavior. Reviewed scope,
CI and independent QA remain necessary.

The frozen `agentos/ocv…` copy used by CI resolves its original workspace only
through the existing root-owned broker request, frozen receipt and candidate
artifact. All identities and candidate hashes must agree. A worker cannot
choose another origin through action parameters or an `origin.json` artifact.

## Guard integration

`candidate_origin.py` is a privileged bundle member. Before importing it, the
registered guard must verify its exact reviewed SHA256, regular file identity,
root ownership and non-writable ancestors. It must load that fixed controller
path, never search the candidate or its Python import path. Registration must
install and attest the guard and this module as one reviewed bundle.

After measuring the source input bytes and before CI:

```python
origin = trusted_origin_module.capture_origin(workspace, source_candidate_digest)
```

On a successful validation, include both `sourceCandidateDigest` and
`candidateOrigin` in the native receipt. Negative provenance remains explicit
with `publishEligible:false`; diagnostic CI may complete, but publication may
not. Before any publication effect, while holding the existing publication
lock and after checking candidate bytes and the active base:

```python
trusted_origin_module.require_publish_origin(record)
```

This rechecks the same protected evidence and publication identity. A source,
snapshot, baseline, receipt or active publication change invalidates the proof.
The existing guard remains responsible for measured candidate bytes, stopping
the validation unit and protecting the build/copy boundary.

## Adoption and recovery

For a reviewed operator change, first use canonical `manager.prepare` to create
a fresh isolated workspace from the exact registered source of the active
release. Apply the reviewed diff there, then run CI and publication guards.
Do not retrofit origin metadata onto an existing modified directory.

`develop_agentos` already creates managed preparations. If a release changes
while its worker runs, retain that worker's evidence and patches. Prepare a
new workspace from the current release, compose accepted patches with verified
preimages and scope, resolve conflicts, then run CI and QA on the new candidate.
The existing published-rebase controller can prepare this baseline; preparation
is not implementation or acceptance. This module never applies patches or
silently resets a worker.

The bootstrap publication may use the last explicitly reviewed legacy operator
procedure. Its actual receipts remain legacy. After the new publication, the
reviewed bundle registration installs the guard and origin module. All future
publication through the new guard requires the new origin proof; absence is a
closed gate, not permission to reinterpret historical evidence.

Run `tests/candidate-origin.test.ts` on the isolated test server. It invokes real
workspace preparation and tests stale input after publication, accepted-source
chains, explicit rebase, frozen candidate bindings, byte/permission/link changes,
and missing historical origin. It does not deploy or access production.

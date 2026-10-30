# Accepted source chain selection

This module is the pure verifier used by the controller's accepted-source
preparation flow. It does not itself change tasks, prepare a workspace, apply a
patch, issue model calls, or complete requirements. `acceptance:false` and
`finalVerificationRequired:true` remain explicit in every eligible proof.
See [the planner integration](planning-source-preparation.md) for persistence,
protected inspection, recovery and consumer binding.

`proveAcceptedSourceChain` takes a bounded controller snapshot and canonical
artifact/marker bytes with fresh physical-stop observations. It checks current
task/run/attempt/fence/context/acceptance identities, frozen execution fields,
complete contributor coverage, artifact ownership/hash/size, recomputed source
digests, source generation and frozen parent artifact inputs. Parentage follows
the source workspace plus digest and protected generation, not DAG reachability,
role names, task titles or creation times.

Changed sources must form one chain. An unchanged descendant may witness the
same final candidate only with an exactly empty canonical patch. An unrelated
unchanged branch, multiple changed branches, missing generation or omitted
catalog entry causes an explicit fallback. Readonly writes and changes outside
the frozen write scope are rejected. All contributor artifact references remain
in the proof; unchanged QA is not promoted into final requirement acceptance.

An empty or null `writePaths` means no explicit path list, matching the planner's
existing "files strictly necessary for this assigned module" contract. It does
not grant new write authority. `readOnlySource:true` independently rejects every
source change, including when that list is empty. A nonempty list is checked
against the actual before/after inventory, including deletions.
Directory scopes such as `src/` retain their original spelling in the frozen
contract and only drop the final slash when comparing source paths. QA scopes
such as `evidence/` and `data/` are valid scope metadata but remain excluded from
the canonical source inventory. Traversal, absolute paths and aliases are rejected.

This selects an existing canonical tree, not a patch composer. Patch bytes must
match their runtime-owned artifact record and agree with changed/unchanged
inventory status. The module does not apply patches or independently prove a
patch's textual semantics. Those bytes cannot be supplied as worker-authored
authority: the capture adapter must enforce reserved canonical artifact origin.

The authority boundary belongs to the adapter and controller. They must:

- Capture the entire accepted cohort for one goal/revision/base scope, with a
  total count and explicit truncation flag. Select the latest acceptance using
  `created_at DESC,id DESC`; freeze run/context/instruction and artifact identity.
  Project only needed fields; no full prompts or multi-megabyte input bodies.
  Proofs must be loaded only from trusted controller state, never model output or
  an API-supplied proof. `proofDigest` is an integrity fingerprint, not a signature
  or authentication mechanism; anyone able to replace a proof can recompute it.
- Obtain marker bytes and protected ownership from a trusted runtime inspector.
  `snapshotDigest` means SHA256 of the actual marker bytes; a child's
  `sourceSnapshotDigest` binds the parent's exact generation. Never synthesize
  this binding for legacy workspaces. The plain object types are not attestations.
- Inspect exact run/fence/context/workspace quiescence outside DB locks. MainPID0,
  no populated cgroup, no live workspace actors, no lease and no uncertain owner
  are all necessary. A `submitted` row alone is insufficient. The fresh-stop
  window is60seconds and must not be extended just to accommodate a slow copy.
- Persist an immutable planning intent associating the proofDigest with a fresh
  preparationIdentity UUID. `sourceReusePreparation` returns the existing source
  workspace and the three opt-in manager fields: preparationIdentity,
  expectedSourceDigest and expectedSourceSnapshotDigest. proofDigest is linked
  in the controller, not passed as an extra manager parameter.
- Prepare a new destination with workspace-manager source/target locks and
  before/after digest+generation checks. Validate its protected receipt and exact
  cloned digest; a timeout after dispatch retains uncertain-effect reconciliation.
  Never silently try another source or overwrite an old destination.
- Revalidate the complete authority projection with `sourceReuseAuthorityMatches`
  under the normal planner admission lock, alongside its own fence, goal and
  feedback checks. This fingerprint reads no artifact/source bodies and does not
  replace renewed physical inspection or the manager's receipt. Base file bytes
  are represented by the captured sourceDigest, not rehashed under the DB lock.
  `sourceReuseAuthoritySchema` accepts the smaller projection without even the
  base file inventory; `sourceReuseAuthorityProjection` creates that shape for
  comparisons. Capture the matching shape directly from fresh authority reads.
- Keep all recovery artifacts, mark incorporated references, and prevent duplicate
  patch application. Keep integration, independent QA, `verifiedBatch`,
  `incrementalAdvance` and final acceptance unchanged.

Tests use synthetic source inventories and immutable identities, without a DB,
provider or shell. They cover unchanged witnesses, linear multi-producer chains,
branches and omitted contributors, artifact tampering, legacy/generation reuse,
readonly/scope violations, current acceptance/attempt, physical stop freshness,
and metadata changes between proof and later admission. Separate PostgreSQL
tests cover planner races and durable intents; Python fixtures cover actual
protected copies and receipts. These components still require a real deployed
worker trial before claiming operational savings. A passing synthetic chain
alone does not demonstrate a productivity gain.

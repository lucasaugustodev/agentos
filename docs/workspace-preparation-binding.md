# Verified source preparation

`bridge/workspace-manager.py prepare` has an opt-in contract for copying a known
candidate into a new workspace. It proves the bytes used for that copy. It does
not approve the candidate, satisfy final objective criteria, establish a run's
fence, or replace a fresh physical stop proof. Selection of an accepted producer
and its QA chain belongs to a separate control-plane operation.

Existing callers that omit all three fields retain the legacy behavior. No
planner or dispatcher is enabled by this change.

## Request

Send all three fields together with the existing workspace arguments:

```json
{
  "workspace": "sample/onext",
  "sourceWorkspace": "sample/oaccepted",
  "preparationIdentity": "bd8daedb-d7a8-42da-bad7-a2dcd1be0270",
  "expectedSourceDigest": "<lowercase SHA-256 of the canonical source inventory>",
  "expectedSourceSnapshotDigest": "<lowercase SHA-256 of the source marker bytes>"
}
```

The UUID must use the canonical lowercase hyphenated spelling. Digests must be
64 lowercase hexadecimal characters. The marker must already exist and be
protected; the manager never invents a generation for a legacy source.

The source inventory is the existing `sources()` inventory, including its
existing exclusions. In particular, this digest does not certify `assets/`,
`data/`, `evidence/`, dependency links, credentials, or ignored logs. It includes
ordinary source files and `dist/` when present. The inventory definition does
not change for bound preparation.

## Copy and replay

Preparation takes the existing shared worker lock, a shared source workspace
lock, an exclusive destination lock, and an exclusive preparation UUID lock.
The files use the same lock namespace as legacy preparation and worker
execution. The opt-in path also verifies lock ownership, type, links and modes.

The bridge data directory, receipt and baseline directories, and every ancestor
through `/` must be owned by the controller's effective UID and must not be
writable by group or others. Symlink ancestors are rejected. The production
controller runs as root. Existing installations with an unprotected controller
directory fail closed; preparation does not change their ownership or modes.

The manager checks source bytes and marker generation under the locks before
copying, after copying, after Git initialization, and before publishing the
copy. It compares the copied inventory, baseline inventory and expected source
digest. Required `.gitignore` entries must already be present: bound preparation
rejects normalization that would silently change the requested bytes. Legacy
preparation still adds these entries.

Before either destination rename, the manager writes and fsyncs a protected
receipt at `DATA/workspace-preparations/<preparationIdentity>.json`. This reserves
the identity durably. The marker references the binding and receipt digests.
Both marker and receipt are exclusive-create regular files with explicit `0444`
modes, independent of umask. Baseline directories are private and baseline files
are also `0444`. The newly copied source remains editable for later work.

A receipt by itself is **not** evidence of completion. Success and replay require
the matching destination marker, protected baseline and exact destination
inventory. The source must still match the request when replay is attempted.
Reusing the UUID for another destination or source is rejected. Reusing the
destination with another UUID is rejected. An existing modified destination is
preserved, not reset.

## Receipt schema (version 1)

The response adds `preparationReceipt` to the snapshot state. Its exact keys are:

| Field | Meaning |
| --- | --- |
| `version` | Integer `1` |
| `preparationIdentity` | Request UUID |
| `sourceWorkspace`, `targetWorkspace` | Exact request workspace IDs |
| `expectedSourceDigest` | Requested canonical source digest |
| `expectedSourceSnapshotDigest` | Requested raw marker digest |
| `bindingDigest` | Digest of the six binding fields above |
| `observedSourceDigest` | Observed source inventory digest |
| `sourceSnapshotDigest` | Observed source marker digest |
| `cloneBaseDigest` | Copied inventory digest |
| `baselineDigest` | Protected baseline inventory digest |
| `createdAt` | UTC preparation timestamp |
| `receiptDigest` | Digest of all receipt fields except this field |

Digests of JSON use SHA-256 over UTF-8 JSON with sorted keys, no insignificant
whitespace (`separators=(',', ':')`), and `ensure_ascii=False`. The binding fields
are `version`, `preparationIdentity`, `sourceWorkspace`, `targetWorkspace`,
`expectedSourceDigest` and `expectedSourceSnapshotDigest`. There is no
`proofDigest` request field. A higher-level proof can bind this operation through
its UUID and complete receipt without extending this low-level contract.

The snapshot marker keeps its existing workspace, source, base digest,
generation and creation fields. Its additional `preparation` object contains
`version`, `preparationIdentity`, `bindingDigest` and `receiptDigest`.

## Failure and recovery

- `SOURCE_BUSY`: a workspace or preparation lock is held. No copy was admitted.
- `PREPARATION_BINDING_INVALID`: missing or malformed request fields.
- `PREPARATION_SOURCE_DIGEST_CHANGED` or
  `PREPARATION_SOURCE_GENERATION_CHANGED`: source no longer matches the request.
- `PREPARATION_SOURCE_NORMALIZATION`: preparing it would change counted bytes.
- `PREPARATION_CLONE_CHANGED`: copied bytes do not match the request.
- `PREPARATION_BINDING_MISMATCH`, `PREPARATION_TARGET_CHANGED`, or
  `PREPARATION_BASELINE_CHANGED`: replay is not the same completed operation.
- `PREPARATION_DIRECTORY_UNPROTECTED`, `PREPARATION_LOCK_UNPROTECTED`, or
  `PREPARATION_RECEIPT_UNPROTECTED`: ownership, path, type, link or mode checks
  failed. The bridge does not relax those checks.
- `PREPARATION_PARTIAL_STATE` or a missing/invalid receipt on replay: reconcile
  existing state before deciding another action.
- `PREPARATION_EFFECT_UNCONFIRMED`: the durable reservation or destination
  publication started but did not complete normally. Published state is
  preserved; unpublished temporary trees from that invocation are removed.

An interrupted client may have lost the response after an effect. Do not infer
failure or issue an unrelated replacement from a timeout. Reconcile the same
UUID, source, destination and expected digests; an exact completed replay is safe,
while a partial or modified state requires explicit reconciliation. No automatic
cleanup, overwrite, retargeting or historical generation synthesis is provided.

## Validation

The isolated Python fixture exercises real Git preparation, file modes, source
and marker changes during the copy, UUID and destination conflicts, protected
ancestors and locks, umasks, exact replay and injected publication failures.
`tests/workspace-prepare-binding.test.ts` includes that fixture in Vitest. It has
no production configuration, database access or provider calls.

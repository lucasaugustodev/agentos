# Controlled Open Kernel submission adapter

An optional example of an operator-owned external submission queue. It is separate
from the AgentOS runtime and is **not installed or registered automatically**.
It contains no participant account, candidate kernels, private receipts or GPU
results. Passing these offline tests does not demonstrate a deployed integration.

Requires Python 3.10+ on Linux/Unix: standard library only, including `fcntl`,
directory descriptors and `O_NOFOLLOW`. No CUDA, PyTorch or GPU is needed for
the tests. Run them on an isolated development server:

```sh
cd examples/openkernel-controller
python3 -B -m unittest discover -s tests -v
```

Tests inject provider responses or use loopback HTTP servers and synthetic
tokens. They do not contact the official service or submit work. The transport
uses the public [Open Kernel Challenge API](https://openkernelchallenge.dev/llms.txt);
verify its current contract and participation rules before enabling real writes.

## What the adapter enforces

- HTTPS verification, an official-origin/path allowlist, no proxies or redirects,
  and no automatic transport retries.
- One descriptor-based read of the candidate with no symlink traversal. The
  checked source SHA256 identifies the exact bytes sent.
- Operator-owned, hash-pinned candidate and independent-QA metadata, outside
  worker-writable workspaces. Workers cannot self-authorize a submission.
- A shared account lock, persistent daily reservations and rate admission,
  with current usage and limits fetched before submission.
- A journal flushed before POST, then persistent `job_id` before ownership is
  released. An ambiguous write blocks replay across restarts and quota resets.
- Observation of the same problem, mode, job and provider content identity.
  GPU verification requires a successful terminal result, passing checks,
  integer zero return code and no errors, flags or rejection markers.
- A verified check before scored submission of the same source. Scored also
  requires the operator-reviewed performance hypothesis in the pinned binding.

The POST contains only `code` and provider `mode`. Local source hashes and QA
lineage remain in private state. Provider `content_hash` is not assumed to be
the local SHA256. Raw results are stored privately with known credentials and
headers scrubbed; returned summaries contain bounded status and receipt hashes.
Treat the result directory as sensitive operational data even after scrubbing.

The native adapter also rejects candidates whose syntax differs from their base
only in comments, formatting or declared docstrings (`NO_EXECUTABLE_CHANGE`).
That result should return to planning as a negative experiment, without consuming
an official check. This comparison does not prove general semantic equivalence
or speedup; executable expressions, constants and launch decorators remain part
of the comparison, and changed code still needs independent review and testing.

## Configure an operator-owned action

Use fixed action profiles pointing at an approved copy of the script:

```sh
python3 -B /approved/controller/submission_queue.py --action inspect
python3 -B /approved/controller/submission_queue.py --action submit_check
python3 -B /approved/controller/submission_queue.py --action observe
python3 -B /approved/controller/submission_queue.py --action submit_scored
```

`--dry-run` performs GET observations for submissions but makes no POST or quota
reservation. Allow action time for lock waits and bounded HTTP requests.
No daemon, action profile, launcher or cycle is provided or activated here.

The fixed launcher supplies `OPENKERNEL_CONTROLLER_TOKEN` through its environment,
plus `OPENKERNEL_BINDING_FILE` and the exact `OPENKERNEL_BINDING_SHA256`. Keep all
three outside worker prompts, arguments, files and logs. The binding must be a
regular file outside the candidate workspace, not group/world writable, and
immutable to workers. Its structure is:

```json
{
  "project": "openkernel",
  "account": "your-verified-account-id",
  "workspace_root": "/approved/candidates/current",
  "state_dir": "/private/controller/state",
  "account_lock_path": "/private/controller/account.lock",
  "identity": {"goal": "goal-id", "task": "task-id", "run": "run-id", "fence": "fence-id"},
  "qa_registry": {
    "<exact submission.py SHA256>": {
      "qa_digest": "<canonical candidateDigest>",
      "artifact_digest": "<candidate.json artifact SHA256>",
      "qa_artifact_digest": "<independent QA artifact SHA256>",
      "accepted": true,
      "independent": true,
      "ts": 1793275200,
      "performance_hypothesis": "The independently reviewed optimization hypothesis"
    }
  }
}
```

Paths, identifiers, hashes and timestamp above are placeholders. Replace them
with independently verified operator records; do not copy model-authored
`passed:true` into trusted metadata. Candidate files are expected under
`workspace_root/kernels/<known-problem>/submission.py`.

The script reads the AgentOS cycle JSON envelope from stdin. `input.payload`
contains matching identity, `source_sha256`, `artifact_digest` and
`candidate_digest`; `parameters` selects the known problem and, for observation,
the mode, source hash and recorded job ID. The fixed `--action` determines the
operation; `actionKey` remains the occurrence/step identity. A repeated action
pins its complete input except `attemptId`.

Structured responses exit zero so AgentOS can read `ok`, `code` and `effect`.
Exit zero is not task acceptance. Ambiguous submissions return
`ok:false/effect:write`; proven pre-effect refusals use `effect:none`.

## Integrating an existing operator lane

Every CLI and adapter path for the same account must use the **same configured
account lock path**. Do not acquire the lock around an adapter that acquires it
internally. Merely consulting official usage does not prevent concurrent writers.

Existing CLI jobs are not automatically imported as verified checks. Before
enabling writes, reconcile the private journal, source bytes/SHA, canonical
candidate/QA, job ID and provider content identity. Observe that same job; an
empty adapter queue must not trigger a second submission. A reviewed import
mechanism is still required and is deliberately not model-callable here.

For an unacknowledged POST, the official jobs list alone does not prove which
local source SHA was sent. Preserve uncertainty until the operator supplies a
verified receipt; a changed filename, date or quota never authorizes a resend.

To qualify a real integration, provision its private binding and state, reconcile
the existing submission lane, register fixed actions, and demonstrate a genuine
check followed by observation and a scored run for the exact approved source.
Then exercise restart and deduplication against its journal. This example has
no serving submission, rules-acceptance or withdrawal endpoint.
# Native diagnostic CHECK adapter

Projected evidence repairs may carry one additional diagnostic material,
`prior-qa-original.json`. The adapter binds it to the registered historical
independent QA artifact, attempt, fence, input, source and candidate. Its raw
bytes and projection hashes must match. It is not canonical source or current
acceptance evidence; canonical materials and the new independent QA checks
remain mandatory. Other extra materials remain rejected, including this file
on an ordinary experiment. Source-origin audits apply the same distinction.

The source screen sends encoded payload decoders, precompiled binary loaders and
dynamic execution aliases to manual review. It does not execute the submitted
code or prove arbitrary source legality. Readable Triton/CUDA JIT compilation and
installed official helpers remain allowed; data compression outside this narrow
diagnostic lane may need a separate review.

`tools/native_check_adapter.py` adds a separate, opt-in authority lane for a
watchdog's completed producer → independent QA experiment. It does not alter the
existing manual approval path. An operator pins the adapter, qualified transport
and runtime helpers, watcher/config, canonical official files, seed provenance,
boards, workspace, model/account, expiry and account lock in an external policy.
Workers receive no submission credential or controller capability. Only `check`
is supported; scoring remains a separate decision. Quota and rate limits come
from the official account, without an extra daily token/check budget.

CPU evidence binds the actual checker and result hashes. The producer's cases
and negative controls must match QA in order, count, unique names and true
boolean `passed`/`detected` flags. QA may omit only a textual `detail` diagnostic;
if present, it must match the producer's text. Other fields remain exact, so
omitting or changing an additional requirement is rejected. This comparison is
an evidence contract, not proof that a command ran: the controller separately
checks each producer/QA transcript for the actual pinned `sha256sum ... &&
python3 ...` call and its successful tool result. Report claims cannot replace
those execution records, source provenance or physical-stop confirmation.

The controller requires actual terminal/stopped executions, matching fences and
input bytes, read-only workspaces, actual predecessor files delivered to QA,
source provenance, canonical material hashes, and a byte-reproducible single-file
patch (`--- base-candidate.py` / `+++ candidate.py`). It freezes the candidate,
evidence and policy identity before a single qualified transport call under the
existing account lock. Durable intent is mirrored into the existing ledger;
timeouts and uncertain receipts never trigger another POST. The legacy ledger is
a compatibility veto, not authority for a new candidate.

Both producer and QA must use one direct bash command:

```text
sha256sum /absolute/cpu-checks.py /absolute/candidate.py && python3 /absolute/cpu-checks.py [arguments]
```

Do not append `echo`, shell redirections, another command, `cd`, or a newline.
The tool supplies its exit status. `cpu-result.json` records `passed`,
`candidateSHA256`, `checkerSHA256`, named `cases` with `passed:true`, and named
`negativeControls` with `detected:true`. QA binds the checker/result hashes and
the same case arrays in `cpuEvidence`; it explicitly attests
`canonicalPrecisionPreserved`, `evaluatorIndependent`,
`upstreamAttributionPreserved`, `cpuReplayPassed`, `scopePreserved` and
`approvedForOfficialReview`, with `blockingFindings:[]`, findings and limitations.
Both reports preserve `gpuValidated:false` and `officialActionsPerformed:false`.
QA judgement and transcript evidence are not a cryptographic proof of execution
or GPU correctness. The controller never runs worker Python as root. Suspicious
imports/reflection, missing evidence and semantic concerns require native repair
or separate manual review; they do not weaken an existing guard.

`scan-inspect` writes a diagnostic snapshot without a POST, even with a disabled
policy. `scan-check` processes only authorized experiments. Confirmed registered
receipts skip expensive native re-audit; a 45-second scan budget prevents starting
another operation while the batch grows. The active operation is allowed to
finish, so an external service timeout must exceed the bounded API/CLI calls
(a 15-minute oneshot timeout is conservative). Do not kill the process at the scan
budget. The native controller remains the pause switch, checked again before the
intent is created. An expired policy produces an explicit `policy_expired`
diagnostic and does not pause CPU workers.

Selection covers the whole recorded experiment history. Unregistered occurrences
are inspected first, with at most 100 selected occurrences per scan. A policy-bound
cursor in the atomic status rotates after the last actually inspected occurrence,
so old unresolved owners and new arrivals both receive inspection over successive
scans. Registered jobs fill any remaining positions using their verified registry
receipts, without native re-audit or another POST. Neither history nor receipts are
truncated. A crash before the status write repeats inspection under the existing
no-resend guards; the cursor conveys ordering only, never permission or acceptance.
The status describes that bounded scan, not a refreshed copy of omitted historical
diagnostics. The existing registry and durable effect records remain authoritative.

Only a typed `CHECK_QUOTA` refusal from the official preflight, after checking the
account's durable records and before creating a submission intent, emits
`postAttempted=false`. A same-named error from another stage remains unconfirmed.
Any existing local/shared effect overrides that hint; unknown effects stay
`uncertain_owned` and are never resent. Malformed quota data is a controller
failure, not authenticated quota exhaustion. This does not waive quota checks or
grant a CHECK; it lets consumers distinguish resource waiting from uncertainty.

The root-owned registry records the confirmed job plus immutable native/QA,
policy, audit, dispatch and transport receipt hashes. The watchdog reads it and
owns terminal GET observation. A separate status snapshot distinguishes
`await_native`, `eligible`, `check_running`, `rejected_repairable`, `quota_wait`,
`uncertain_owned`, `controller_blocked` and `observation_wait`, bound to occurrence
and candidate/QA hashes. Controller permissions/storage/configuration failures and
read timeouts preserve the candidate and never request a kernel repair. The watchdog
must consume repairable rejections and wake its supervisor before activation;
installing a timer that merely logs those errors would recreate the pending-queue
stall. Do not activate only half of this integration.

An optional `nativeSourceOrigins` policy permits correcting code from a rejected
native experiment without treating that rejection as acceptance:

```json
{
  "nativeSourceOrigins": {
    "enabled": true,
    "maxDepth": 8,
    "registryDirectory": "/var/lib/example-controller/source-origins"
  }
}
```

The directory must be exactly `stateDirectory/source-origins`. Existing seed and
successful-audit provenance remains unchanged. Otherwise the controller follows
the selected artifact's authorized native occurrence, at most eight acyclic hops
to a seed or successful audit. Each hop verifies every dispatched actor has stopped,
their model/account/workspace, dispatch receipts, fenced input manifests, immutable
canonical materials and a patch that reproduces the candidate byte for byte. A
QA-published copy is permitted only as an explicit alias to the exact producer
artifact, with identical bytes and the actual predecessor handoff established.

Origin receipts use `authority: authorized-native-source-origin` and always retain
`qualityAccepted:false`, the parent's actual QA verdict, and both artifact IDs.
They never replace a successful acceptance audit. A corrected child still needs
all its own independent QA, replay, precision, source-screen, executable-change
and official policy gates. Candidate code is not executed by this controller.
Each lookup revalidates the chain and physical stop boundary before reusing an
immutable receipt. Missing source identity or inconsistent provenance produces a
controller diagnostic; temporarily unavailable source files produce observation
wait. Neither condition requests a kernel repair or releases an existing external
job. Enabling this policy is a separate deployment decision; it is off by default.

If the producer failed and QA never started, source origin can still be proved
from that producer's complete dispatch/input/source/patch envelope. The only
allowed unstarted QA state is the native engine's `queued` step with zero
attempts, blocked by its failed producer. A missing or uncertain actor is not a
confirmed stop. Such receipts explicitly record `qa.state: not_run`, null QA
verdicts, the failed producer and any bound negative CPU reports. No QA alias is
allowed without a QA attempt. Optional reports may be unpublished; required
source, patch, dispatch and fenced input evidence cannot be omitted. Version 2
receipts use a separate registry subdirectory and preserve existing v1 records.

Run the isolated unit tests **on a development server**, with an explicitly
provisioned root-owned test directory outside all live state:

```sh
OPENKERNEL_ADAPTER_TEST_ROOT=/var/lib/example-adapter-tests \
  python3 -m unittest discover -s examples/openkernel-controller/tests \
  -p test_native_check_adapter.py -v
```

These tests inject fake native/official transports; they never make a submission
or read production credentials. Test receipts remain inside the isolated test
directory. Do not copy deployment IDs, account data, policies, source artifacts
or credentials into this public repository.

### Accumulated watcher state

The adapter admits the watcher's `state.json` up to 64 MiB in the dispatch audit
and scan. All generic protected reads, including policies, helpers and intent
receipts, retain their 16 MiB ceiling. Root ownership, ancestor permissions,
symlink rejection, strict JSON parsing, watch identity and dispatch/QA authority
remain required. Historical inputs are neither truncated nor rewritten.

This is bounded recovery headroom, not indefinite retention. Track state bytes,
supervision count and byte growth per review. The next migration should store
immutable input payloads by content hash, keep bounded references in active state,
and verify all referenced bytes before replay. It needs atomic publication,
backward-compatible reads, crash/restart and rollback proofs, with an explicit
retention policy that preserves historical inputs and uncertain intents. Do not
silently raise every file ceiling or discard history when the state grows again.

### Native request numbers and invalid evidence

New native requests may declare `requestCanonicalization: json-numbers-v1` in
their durable intent. Integral floats, including signed zero, are encoded as
integers before hashing and transport; booleans remain distinct. Nonfinite
numbers and values outside the safe integer magnitude are rejected before POST.
Unknown versions are rejected. The adapter still requires the complete request
digest, original definition, input, lineage and independent QA.

A legacy digest mismatch has one narrow recovery: a complete
`repairQADiagnostic.report` may be reloaded from its authenticated artifact when
only safe integral float/int representations differ and the reconstructed full
body reproduces the original intent hash exactly. Projected reports cannot use
this path. No receipt, candidate, patch or historical input is rewritten.

Nonfinite values in authenticated producer/QA JSON remain rejected. Their
diagnostic includes the exact artifact reference and a specific evidence error,
so a separately validated planner repair contract can request new producer and
independent QA work. This classification is not acceptance or permission to
submit. Nonfinite policy, state or unbound payloads remain controller errors.

### CHECK account waits and quota synchronization

An unresolved account submission blocks further official CHECKs. After auditing
the candidate, preflight reports a typed `account_wait` with
`ACCOUNT_HAS_UNCERTAIN_EFFECT`, `postAttempted: false`, no job ID and the exact
candidate binding. The candidate's own uncertain durable intent always takes
precedence. This permits CPU repair planning without claiming an official effect.

New CHECK intents record their quota observation and known official job IDs.
The next preflight conservatively combines that observation with local receipts
and the current GET jobs response before consuming another slot. Missing local
jobs, exhausted observed capacity or a UTC day transition during preflight/freezing
yield `CHECK_QUOTA_SYNC_PENDING` before any new intent or POST. Because GET jobs
is bounded, missing older jobs can conservatively delay CHECK until the UTC reset.
Historical intents are not rewritten; a POST with an uncertain 429 outcome is
never automatically resent or reclassified as a preflight refusal.

### Terminal diagnoses across partial scans

The 45-second scan budget may split the unresolved backlog into several batches.
The protected `terminal-diagnostics-v1.json` index retains at most 128 terminal
native rejections for one hour from their original observation. It does not
retain quota, eligibility, uncertain effects, acceptance or CHECK authority.
Only rejections emitted by a native audit before any POST intent qualify. The
index is bounded to 1 MiB and status publication to 256 KiB; overflow fails closed.
Policy or adapter changes invalidate cached authority. Historical watcher inputs,
dispatch intents and official receipts remain untouched.

A cached row carries `diagnosticProof` with its original `observedAt`, separate
`verifiedAt`, policy/code hashes, full native-detail hash and dispatch-receipt
hash. `verifyKind: native_identity` explicitly means that the adapter rechecked
the authenticated GET, completed attempts, fences, manifests, artifact inventory
and both dispatch receipts. It does **not** mean that the old diagnosis or raw
transcript bytes were audited again. Registry entries, including source aliases,
and both current ledgers dominate a cached rejection. Changed or unavailable
identity removes cached authority and preserves the owner for fresh observation.

The continuity consumer must validate this proof and its per-row ages before
using it; the status file's global timestamp cannot refresh an older diagnosis.
It must still rehash the original QA, producer evidence, candidate, base and
patch before requesting native repair. The repaired candidate needs new CPU
execution and independent QA. Cache timestamps are excluded from semantic
source proofs after validation, so verification alone does not cancel a pending
repair plan. This index reduces repeat deep audits; it is not an acceptance
cache or a substitute for official quota and effect reconciliation.

The CPU replay witness also permits one literal `cd /workspace &&` prefix before
`sha256sum CHECKER CANDIDATE && python3 CHECKER ...`. Both artifact operands must
then be absolute paths without parent traversal. The checker invocation, content
hashes, successful tool call/result pair and all shell-control exclusions remain
required. Other directory changes, substitutions, redirects, extra statements
and report-only claims remain unproven; this does not execute or approve code.

### NativeQA v2: bounded retries of the review

New definitions can opt into `parameters.nativeQAContractVersion: 2` on the QA
step and the exact dependency-evidence contract binding `cpuEvidence.checkerSHA256`
and `cpuEvidence.resultSHA256` to the current producer's `cpu-checks.py` and
`cpu-result.json`. The producer still has one attempt. QA has at most three.
This requires the compatible core gate; a version flag alone grants nothing.

The adapter verifies every QA attempt's frozen producer identity, complete
artifact inventory, consumer fence, input bytes, report bytes and controller
binding proof. Attempts must be contiguous. Earlier attempts must be stopped
failures specifically caused by `DEPENDENCY_ARTIFACT_BINDING`; unrelated failures
and legacy retries remain ineligible. Only the last completed QA with a matching
gate can enter normal semantic QA and execution-witness validation.

A stopped final binding failure can establish source provenance for a later
repair, with `qualityAccepted: false`. It cannot authorize CHECK or enter the
terminal acceptance path. Failed reports and attempt history are preserved;
historical hashes never replace current producer evidence.
Other final failure codes currently cannot provide v2 source provenance; recovery
from a semantic failure requires a separate contract and is not part of this
binding-retry change.

### Explicit reservations for a known uncertain CHECK

The default remains an account-wide block while an effect is uncertain. An
operator can opt into `uncertainCheckReservations: {version: 1, intents: [...]}`
for individually reviewed intents. Each entry pins `problem`, `sourceSHA256`,
`occurrenceId`, `intentEpoch`, exact `ledgerSHA256`, and an immutable
`originPolicy: {path, sha256}`. Unlisted uncertainty still blocks the account.

The native and shared ledgers must retain identical reviewed bytes. Historical
executor/transport authority, audit, source and complete 429 receipt are checked;
the transport qualification must demonstrate one POST, without internal retry.
These pins do not prove the historical identity of the external OKC account.
The source itself remains unresendable across occurrences and UTC days. No
receipt is rewritten, no uncertainty is declared resolved, and no SCORE is used.

Other candidates need fresh official quotas and a synchronized local consumption
floor. Each unresolved pinned intent reserves one extra CHECK on its own problem,
including after midnight. Reservations are stored separately from the used floor
to avoid charging the same reservation again after every acknowledged job. The
existing minimum quota margin, rate checks, lock and pre-intent rechecks remain.
Changed ledgers, stale observations, UTC rollover or an incomplete/ambiguous jobs
window require a new observation. A full 50-job window entirely inside the current
UTC day is deliberately insufficient; availability may wait for adequate coverage.

After a complete native audit, the scanner can attach a current, authenticated
`uncertainCheckReservation` proof to the original source and its aliases. A
compatible continuity consumer may then admit CPU work on other sources. The
original source remains `uncertain_owned`, occupies WIP, and is never resent.
Its projected `postAttempted: null` means uncertain, not that no POST occurred;
`ownSourceUnretryable: true` and `effectReconciled: false` make that boundary
explicit. The proof binds the current detail and policy hashes and the exact
reviewed intent. New uncertainty, changed evidence or stale proof restores the
default block. Neither this marker nor a CPU task grants CHECK authority.

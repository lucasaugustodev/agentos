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

## Continuity diagnostics across bounded scans

The continuity watcher can consume terminal rejection diagnostics whose native
identity has been reverified by the pinned adapter. Each cached row carries a
`diagnosticProof` with `verifyKind: "native_identity"`, its original `observedAt`,
fresh `verifiedAt`, policy and adapter hashes, full native-detail hash and
dispatch-receipt hash. This is a historical rejection, not a fresh audit of all
execution bytes. The original audit expires after one hour; rechecking identity
does not extend that expiry. The last identity verification must also fit the
watcher's observation window.

The watcher checks current native detail identity and rereads the candidate,
base, patch, CPU checker/result, verification and QA bytes before offering an
evidence repair. Invalid or expired proofs retain the candidate's reservation.
Observation timestamps are excluded from semantic plan hashes, so a partial
scan cannot invalidate a plan merely by reobserving unchanged evidence. Every
repair still requires a new CPU execution and independent QA; no cache entry
authorizes a CHECK, numerical acceptance or a performance claim.

A typed preflight `account_wait / ACCOUNT_HAS_UNCERTAIN_EFFECT`, or
`quota_wait / CHECK_QUOTA_SYNC_PENDING`, can reserve a candidate without blocking
unrelated CPU work. Both require explicit `postAttempted: false`, `jobId: null`
and a matching known source. An effect owned by that candidate overrides those
preflight states. An unacknowledged POST remains uncertain, including when a
transport reports a quota error; it is never silently retried or reclassified
as a preflight refusal.

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

`tools/native_check_adapter.py` adds a separate, opt-in authority lane for a
watchdog's completed producer → independent QA experiment. It does not alter the
existing manual approval path. An operator pins the adapter, qualified transport
and runtime helpers, watcher/config, canonical official files, seed provenance,
boards, workspace, model/account, expiry and account lock in an external policy.
Workers receive no submission credential or controller capability. Only `check`
is supported; scoring remains a separate decision. Quota and rate limits come
from the official account, without an extra daily token/check budget.

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
to a seed or successful audit. Each hop verifies both actors have actually stopped,
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

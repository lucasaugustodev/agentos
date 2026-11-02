# CPU replay observation (default OFF)

This is an opt-in **technical observation**, not a new acceptance contract. It
does not relax native QA v1/v2, repair the candidate, populate cases, approve a
report, change the official submission lane, or change model request limits.
There is no installed registration or enabled production definition in this
change. The two-step producer/QA DAG remains intact.

## Why a separate executor

The controller can record exactly which bytes ran, the real exit status, and
whether all processes stopped. A model report and a shell `; echo` suffix cannot
provide those guarantees. A successful process still does not prove that its
checker is meaningful, that negative controls discriminate correctly, or that
a kernel has passed GPU validation. Independent semantic QA remains necessary.

Failures called `CPU_CASES_NOT_REPLAYED` have different causes. In one real
qualification case the rows were identical but the old reader rejected object
`detail` fields. That is a separate reader contract defect, not evidence that
the model copied cases incorrectly. This executor does not fix that parser.

## Explicit capability and registration

An immutable QA step must have all of:

- `parameters.nativeQAContractVersion: 2` and its frozen dependency contract;
- `parameters.cpuReplayObservationVersion: 1`;
- `parameters.cpuReplayProfile` naming an allowed fixed profile;
- cycle `allowedActions` containing `native.cpu-replay.observe`.

The host must separately have `AGENTOS_CPU_REPLAY_REGISTRATION` pointing to a
root-owned, non-writable, non-symlink registration and a digest-pinned executor.
Absent registration means disabled. Model input cannot choose an executable,
shell, UID, environment, path on the host, limits, or arbitrary arguments.
The registered projects and profiles are allowlists. No profile is installed by
this patch; deployment and resource allocation require a separate review.

## Supported fixed profiles

`python-cpu-argv-v1` executes:

```text
/usr/bin/python3 -I -B /inputs/cpu-checks.py --candidate /inputs/candidate.py --output /output/cpu-replay-result.json
```

Its three inputs are the current producer's `candidate.py`, `cpu-checks.py`, and
`cpu-result.json`. This is a new explicit checker ABI, not a claim of backward
compatibility with every checker previously written by an agent.

`python-cpu-candidate-base-v1` executes:

```text
/usr/bin/python3 -I -B /output/cpu-checks.py /output/candidate.py /inputs/base-candidate.py
```

It adds the current producer's `implementation.patch` and the exact base source.
The checker, candidate, and patch are individually mounted read-only below the
writable output directory because supported existing checkers write their
result beside `__file__`. The original producer result is a separate read-only
input; it cannot stand in for a newly generated result.

For the base, the controller verifies the occurrence input's
`experiment.materials/base-candidate.py` bytes, length and SHA against
`experiment.sourceArtifact` and the canonical artifact bytes. It requires the
source artifact's actual producer attempt to be completed and physically
stopped, in the referenced occurrence and the same project. That identity and
the occurrence input digest are frozen in `baseSource` and checked again before
dispatch and before committing the observation. This does not replace the
official adapter's source provenance checks or grant official acceptance.

Unknown checker ABIs are unsupported. No shell interpretation, argument
inference, editing of a real checker, or silent fallback is allowed.

## Durable ownership and evidence

The existing `cycle_action_receipts` table records ownership before invoking
the executor. The executor has a separate protected intent before starting a
transient service. Re-entering the same request inspects that intent/receipt;
it does not run the program again. An uncertain tool in an occurrence blocks a
second tool invocation in that occurrence. Missing responses never imply that
the request was not dispatched.

Protected intents, receipts and cancellation tombstones use a fsynced staging
file and Linux `renameat2(RENAME_NOREPLACE)`, followed by directory fsync. A
reader sees no final file or all its bytes. Existing destinations are never
replaced, and unsupported filesystem semantics fail closed without a weaker
fallback. A killed publisher can leave a staging file, not a partial canonical
receipt that masks an incomplete execution.

Before launching QA, the controller independently invokes the registered
inspector and checks the returned receipt against the frozen request, profile,
registration, executor, unit, producer, occurrence and QA fence. It then checks
the current database authority again. A concurrent pause, new fence, changed
input or producer invalidates continuation without overwriting newer state.
The receipt is controller-owned context and a read-only input to the QA model;
it is not saved as a model-authored acceptance artifact.

The receipt contains actual exit code or signal, timeout/OOM observations,
bounded raw output hashes, timestamps and physical stop evidence. Cgroup
`memory.peak` is reported separately from RSS; unavailable measurements are
`null`, not an invented zero. The status is always `observationOnly: true` and
`acceptance: false`, even when the checker exits zero or prints `passed: true`.
Failures remain visible to independent QA. An unconfirmed stop prevents the QA
model from starting; it is not automatically retried as a new execution.

## Isolation and boundaries

Only the reviewed launcher may be privileged. Python checker/candidate code runs
as a dedicated non-root user in its own transient unit and bubblewrap sandbox:
no external network or credentials, read-only inputs, fixed writable output,
bounded files/logs, process count, CPU, memory, and wall time. The launcher
captures metrics and physical termination before releasing the unit. The
executor permits one tool run at a time. Persistent admissions retain their
original order and a 60-second polling lease, so a paused controller cannot
leave an immortal queue head. A request lock protects each identity; the global
execution lock does not prevent inspection of another completed request. The
model's existing memory policy and six-request limit are not changed.

This contains the **mediated** replay. Arbitrary additional `bash` programs
written by QA still execute under the agent's existing sandbox and can exhaust
its memory. Exposing this executor as a general mediated diagnostic tool is
later work, not implemented here.

The cgroup memory hierarchy still matters: a tool-level limit does not grant
extra host capacity or defeat a tighter ancestor limit. Do not enable this
profile without separately reviewing aggregate resource admission.

## Preparation, admission and controller recovery

Before IPC, preparation persists its request hash, registration and executor
pins, dependency snapshot, authority hash and a claim token. Waiting does not
create or fail a QA attempt. The bounded engine sweep resumes the same attempt
and fence with a new token, inspecting the original request first. The token
is checked before IPC, receipt commit and model dispatch; a superseded handler
cannot commit or start QA merely because its run fence is unchanged.

The executor can return an authenticated no-dispatch proof only under the own
request lock, with no execution directory/intent. BUSY alone is never that
proof. A later execute call uses the same request identity and creates the
exclusive intent before starting a unit; an existing intent always wins and
prevents rerunning the tool. Waiting claims use persistent bounded backoff and
fair ordering, not an unbounded in-memory sleep or a new model session.

Immediately before `worker.execute`, one database CAS records `qa_dispatching`.
CPU recovery never replays that model call. After this boundary, the existing
harness reconciliation owns the effect. Conversely, generic harness
reconciliation excludes unfinished CPU preparation: absence of a model process
cannot become a false `STOP_CONFIRMED` for a tool that is still owned.

A lost response after a full protected receipt was saved can be reconciled by
inspection. If the launcher dies before saving outputs, recovery does not
invent empty output or success. After acquiring the own lock and confirming
physical stop twice, it can persist a separate `cpu-replay-incomplete`
tombstone. That frees capacity for other request IDs and preserves
`CPU_REPLAY_RESULT_LOST` for the affected attempt without starting QA. A full
receipt always takes precedence. A missing intent also requires an absent
unit and a conservative tombstone; the same ID is never relaunched.

## Remaining adoption boundaries

This revision remains **default OFF and not approved for production activation**.
Registration changes must retain the original inspector authority or fail
closed until reviewed reconciliation. Missing metrics remain unavailable.
Admission history is bounded at 4,096 records; reviewed retention is necessary
before treating this as an indefinite service. No garbage collection is added
here. Preparations still reserve native execution capacity while waiting; this
is not proof of active model calls or of a separately tuned CPU resource pool.

Pausing or cancelling an occurrence prevents launching or resuming QA. A
cancel continuation uses the same request and a separate no-launch mode. Only
the own lock, absent execution intent and absent derived unit permit a durable
cancellation tombstone. Every later execute of that ID returns the tombstone.
An existing execution intent instead requires its full or incomplete terminal
receipt; cancellation waits if the unit is still alive or unknown. The
inspector never implicitly kills it. Preparation cancellation is recorded with
`modelDispatched: false`, not a fabricated harness stop or QA result.

When explicitly enabled, preparation stages the receipt as a read-only input
and names it in the QA prompt; it is not merely logged. Isolated tests prove
this staging. No production QA has consumed this observation contract, and
agent use or semantic benefit has not been demonstrated by these tests.

## Qualification and later acceptance contract

Tests must cover real nonzero exit status, forged output, tampered bindings,
lost replies, duplicate requests, late fences/pauses, descendants, bounded
output, timeout and controlled OOM without killing the controller. Qualify at
least one unchanged checker from an authenticated operation snapshot, including
its base/patch, under the same non-root sandbox; private artifacts stay outside
Git. Passing synthetic tests alone is not a claim that a real candidate passed.

A later, separately versioned acceptance contract can combine a controller
execution receipt with a model-authored semantic assessment. It must preserve
historical v1/v2 readers and require a real chain through independent QA,
NativeAudit and official CHECK before claiming operational improvement. No
historical report is rewritten or made successful by this observation feature.

Primary runtime references:

- [Linux cgroup v2 memory controls and counters](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [systemd service OOM behavior](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html)
- [Transient services, exit status and collection](https://www.freedesktop.org/software/systemd/man/latest/systemd-run.html)
- [Bubblewrap sandbox policy responsibilities](https://github.com/containers/bubblewrap)
- [Atomic rename and RENAME_NOREPLACE semantics](https://man7.org/linux/man-pages/man2/rename.2.html)

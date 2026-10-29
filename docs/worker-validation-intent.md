# Registered CI from a worker artifact

The worker cannot call the manager-only `request_validation` action. A controller
policy may authorize a stopped worker attempt to request the existing registered
candidate validator through `validation-intent.json`. This is a second authority
contract for the same validator, not another execution queue or a replacement
for manager authorization, task criteria, source provenance or independent QA.

## Configuration and admission

The authenticated `POST /api/objectives/:id/validation-policy` route accepts:

```json
{
  "version": 1,
  "enabled": true,
  "project": "agentos",
  "profile": "agentos.validate",
  "stages": ["verify"],
  "required": true,
  "requiredDatabaseTests": [
    {"file": "tests/example-pg.test.ts", "name": "exact Vitest fullName of an obligatory case"}
  ]
}
```

The example case is a placeholder, not an installed or automatically selected
test. The operator must select the actual required cases. No file-name heuristic
or aggregate pass count establishes database coverage. Nonrequired cases may be
skipped; every configured case must be present exactly once and pass.

Registration checks the existing framework project allowlist, an active goal,
enabled autonomy and absence of management/workforce ownership. The controller
persists a registration UUID, digest, authenticated channel and matching
`WORKER_VALIDATION_POLICY_REGISTERED` event. On admission the run's immutable
instruction snapshot captures that registration only for declared stages. An
unregistered setting is rejected. Registering/replacing policy cannot add this
obligation to a run that already exists. Disabling or replacing a captured policy
revokes that old request; it does not silently adopt new obligations.

The schema migration changes the admission trigger and requires the normal
reviewed schema preparation. Workers do not install DDL. Nothing backfills old
snapshots, changes old criteria or rewrites historical acceptances.

## Worker and controller responsibilities

Future prompts explicitly distinguish the manager action from the worker
handoff. With a captured policy, the worker writes `evidence/validation-intent.json`
and includes it in the ordinary final artifact manifest:

```json
{"version":1,"profile":"agentos.validate","reason":"Run the required registered CI for this candidate."}
```

There are no command, path, account, candidate, task or run fields supplied by the
model. The runtime generates `candidate.json` through its existing source
inventory mechanism. Without policy, the prompt requires an honest limitation
when database checks are unavailable and never promises this action as a tool.

After `completeRun` submits the attempt, the worker reconciler reads the current
attempt and the exact intent/candidate bytes, verifies the admission receipt and
confirms physical stop. Under the existing admission lock it reserves one
`candidate-validation:<actionId>` owner. `worker-validation-handoff:<runId>` is
only its durable receipt; it has no separate executor or retry queue. Missing or
invalid intent produces a negative controller check. Unconfirmed termination
retains ownership and waits for another observation rather than starting CI or
another worker.

The request has `authority: runtime_artifact` and `runtimeAuthority`:

- `version: 1`, `kind: runtime_artifact`, `profile: agentos.validate`;
- `policyRegistration`: complete authenticated registration frozen at admission;
- `intent`: artifact UUID, SHA256, size and exact canonical-base64 bytes;
- `snapshotDigest`: SHA256 of compact recursively key-sorted JSON of that policy
  registration, preserving array order. The separate existing `contractDigest`
  binds the task contract.

All producer identities, attempt, fence, context, workspace, candidate inventory
and hashes come from the controller. There is no fabricated `managerRunId` or
manager decision. The manager path retains its accepted coordination artifact
and action proof unchanged.

Existing freeze/execute/receipt reconciliation handles the reservation. Unknown
remote outcomes retain ownership and probe receipts only. Revocation, conflicting
ownership, new run/fence/context/contract or changed candidate refuses further
dispatch/acceptance. Neither command exit nor a worker's `passed: true` is proof.

## Acceptance and repair

The deterministic validator includes current CI state in its input identity.
Polling excludes pending CI; direct validation returns pending without writing an
acceptance. The final transaction rechecks the gate under the admission lock.
Only the existing authenticated evidence exporter supplies the controller gate.

Its optional coverage authority is:

```json
{
  "version": 1,
  "source": "registered_vitest_json",
  "profile": "agentos.validate",
  "candidateDigest": "<sha256>",
  "obligationsDigest": "<sha256 of the exact configured requiredDatabaseTests array>",
  "reportDigest": "<sha256 of actual Vitest JSON bytes>",
  "requiredTests": [
    {"file":"tests/example-pg.test.ts","name":"exact case fullName","status":"passed"}
  ]
}
```

Allowed case statuses are `passed`, `failed`, `skipped`, and `missing`. The producer
and request digests bind the report to this exact stopped candidate and policy.
The registered executor and broker must independently establish this coverage;
TypeScript never derives it from worker text or a generic `N passed` log. An older
valid CI receipt without coverage remains real historical evidence and yields
`CI_REQUIRED_COVERAGE_MISSING`, not acceptance. This is an intermediate limitation
until the registered profile produces and verifies the structured report.

Passing registered CI adds one necessary check, never overrides other criteria.
A negative gate preserves its receipt/log and feeds existing objective repair.
The next plan receives the most recent matching CI receipt/log as immutable
execution inputs for the affected requirement. No artifacts are appended to the
old attempt and no manager revision is invented. The final independent verifier
is preserved.

## Validation plan

Run only on the explicitly isolated development server with `/agentos_test` and
the existing validation lock. Focus:

```text
npx vitest run tests/worker-validation-intent-pg.test.ts tests/candidate-validation-pg.test.ts tests/candidate-validation-evidence-pg.test.ts tests/validation-idempotency.test.ts tests/task-instructions.test.ts tests/planner.test.ts
npx tsc --noEmit
```

The counterexamples cover old admissions, stage selection, absent/forged intent,
no early acceptance, concurrent duplicate validation, no fake manager identity,
uncertain stop/outcome, revoked policy, stale fence/context/candidate, mandatory
case omissions/skips, arbitrary executor SHA, original criteria and evidence
handoff without historical writes. Full reviewed validation and a real registered
CI run on a newly admitted final stage remain required before claiming end to end
autonomy.

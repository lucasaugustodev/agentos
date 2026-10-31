# Candidate readiness and independent QA

This opt-in contract fixes a circular instruction: an integrator was asked to
finish before the controller could run PostgreSQL CI, while its delivery required
those same checks to have passed already. A future integration may now declare
local readiness. Acceptance still requires physical stop, the registered CI of
the exact candidate, and all prescribed local evidence. Independent QA follows.

## Initial scope

- Only an incremental `agentos.validate` integration batch whose covered
  requirements equal the authenticated map. Unsupported or unmapped scope fails
  explicitly; the planner cannot infer omitted controls from prose.
- The existing worker validation policy must include `integrate`. An independent
  QA handoff refuses a policy that also includes `verify`: it does not silently
  suppress or replace an existing second CI obligation.
- The actual queue consumer must have adopted the controller release, with its
  existing service receipt and live PostgreSQL ownership locks. A new API or
  schema alone is insufficient.
- Existing attempts, `delivery.passed:false`, verification criteria, runtime CI
  protocol version 1, and historical acceptances retain their original meaning.
- No provider, model, quota, approval, maintenance or concurrency policy changes.

## Authenticated preparation

Register the existing worker validation policy first. Then an authenticated
operator session or API operator may POST a finite map to
`/api/objectives/:id/candidate-readiness`. Operations observation credentials do
not authorize this route. Registration affects future compiled plans only.

```json
{
  "version": 1,
  "enabled": true,
  "profile": "agentos.validate",
  "policyRegistrationId": "<existing-policy-registration-uuid>",
  "requirements": [{"id": "req-1", "digest": "<original-description-digest>"}],
  "obligations": [
    {"id": "local-source", "requirements": ["req-1"], "kind": "local",
     "name": "local-evidence.json", "field": "passed", "equals": true},
    {"id": "database-source", "requirements": ["req-1"], "kind": "registered_ci",
     "file": "tests/transaction-pg.test.ts", "name": "transaction rejects a stale writer"}
  ]
}
```

Requirement identity uses the original ID and `validationDigest(description)`:
SHA-256 of the JSON string representation of that original description. It does
not hash freely paraphrased verification instructions. Every requirement must
have controls. Every CI obligation must match the frozen policy's `(file,name)`
set exactly, without omissions or duplicates. The policy's original test order
continues to define the CI wire digest. Local obligations and CI obligations are
distinct structured entries; the LLM cannot choose what to defer.

The controller binds this map to a new plan and records provenance. A plan or
worker cannot self-declare either `candidate_readiness` or `candidate_ci` by
copying hashes. Replacement or revocation of the map/policy invalidates current
authority; it never upgrades an already admitted legacy attempt.

For an explicit subset batch, add `scope: {kind: "batch", originalRequirements:
[{id, digest}, ...]}` containing the complete original objective universe. The
map's `requirements` and its obligations describe exactly the selected batch;
all IDs and description digests must match that pinned universe. Registration,
planning and admission still verify the entire original universe, and the plan
must retain every original requirement and its incremental partition. Other
requirements remain pending or completed according to the existing canonical
incremental authority; accepting this batch does not complete them. The complete
registered CI policy remains required and is never filtered by batch membership.
Without `scope`, the original whole-objective contract and digest are unchanged.

## Execution and acceptance

1. Admission checks the current authenticated map and adopted consumer. A
   transaction-bound event identifies the exact task, next run, fence and attempt.
   The database trigger freezes contract version 2 for this integration only.
   Both scheduler and trigger compare future criteria to the applied plan before
   the first run: removing the marker cannot turn the task into a legacy task.
2. The integrator publishes actual local artifacts, a separate
   `integration-readiness.json`, and the unchanged version-1 validation intent.
   A ready report lists each local check and the digest of its actual bytes, plus
   the exact pending controller CI controls. It does not claim `passed:true`.
3. The controller rechecks artifact identity and local results, confirms physical
   stop and binds one existing CI owner to this candidate. Uncertain stop remains
   pending. Duplicate handoff does not create another validator.
4. Only authenticated, complete registered CI coverage of this candidate can
   satisfy integration acceptance. Evaluation seals current authority and
   artifacts before its final transactional compare-and-set.
5. An opt-in read-only QA task receives compact canonical CI evidence in its
   frozen context, bound to the accepted integration dependency, revision,
   workspace, candidate, policy and map. It independently executes the local
   controls and publishes its own evidence plus `verification.json`.
6. QA acceptance revalidates that dependency and CI authority and requires its
   runtime candidate to equal the accepted source. A copied receipt, changed
   candidate, missing local check, removed criterion or concurrent binding change
   cannot substitute for that proof. This does not add a second execution queue.

## Blocked preparation

An unadopted consumer waits in `waiting_resource` and is checked again by the
existing scheduler. Other eligible tasks may still dispatch. An invalid map or
policy preserves the task and records the precise cause and the owner
`authenticated-controller-configuration`. Repair requires an authenticated
configuration update; this prototype does not fabricate a run or a supervision
case before the first attempt. It does not claim automatic semantic repair of an
incomplete control map, nor ask a human by default.

## Verification limits

The PostgreSQL suites exercise real admission triggers, byte-backed artifacts,
fences, duplicate ownership, CI evidence validation and acceptance CAS. Their
external process inspector and CI exporter are explicitly controlled fixtures;
passing these tests is not proof that a production CI build or an autonomous
model task completed. A separately reviewed release and a live opt-in trial are
still required before claiming that operational outcome.

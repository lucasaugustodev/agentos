# Requirement completion contract v2

A task acceptance certifies that task's checks. It does not necessarily finish
the requirement the task supports. An offline preparation can be accepted while
an external result is still unavailable.

New model-generated plans use `completionContractVersion: 2`. Stored v1 plans,
frozen planner outputs and historical acceptances retain their original meaning.
Adoption for an existing objective happens through a new planning revision; it
does not rewrite an old acceptance. There is no schema migration.

When moving from v1 to v2, historical subdelivery acceptances, source and
artifacts remain preserved. Requirement completion without a v2 proof is
explicitly marked unproven in the planning request. The planner should reuse
that source and verify the missing evidence, not repeat accepted implementation.
The existing incremental compiler still requires a delivery producer in a new
batch; it does not yet schedule a standalone verifier targeting an old batch.
An evidence-only producer must declare that limited scope and retain the
accepted source, followed by the normal final independent verifier.

## Planning and evidence

The planner must declare `requirementContracts` for every original requirement.
Each contract freezes observables and, when needed, external dependencies.
The controller adds the `requirement_outcomes` criterion to final verifier tasks.
Intermediate verifiers remain supporting tasks. Later v2 revisions preserve the
frozen contracts and cannot silently reduce their predicates.

An observable names an artifact and optionally a JSON field/value predicate.
Its authority is explicit:

- `current_run`: an artifact published by this independent verifier's exact run.
- `objective_input`: an authenticated, immutable input registered through the
  existing objective-input API and associated with the requirement. A worker's
  copy of an external receipt cannot substitute for this authority.

The verifier publishes `requirement-outcomes.json`. Every covered requirement
has one outcome, with full or partial scope and references to the observable
evidence by name and SHA-256. Input observations also carry their canonical
`inputId`. The controller verifies artifact bytes, identity, frozen contract,
candidate inventory, run and fence, then stores the normalized proof inside the
existing acceptance checks.

The outcomes have separate meanings:

- `completed`: full scope, every observable satisfied and all declared external
  prerequisites satisfied.
- `partial`: an accepted observation of work that still needs implementation or
  evidence. The incremental planner can own the next delivery.
- `externally_blocked`: a declared external-input predicate is still missing or
  false. The existing gate can remain accepted without completing the goal or
  retrying that same gate indefinitely.

Self-references to `verification.json`, `candidate.json` or the outcome report
are not product evidence. The controller validates the declared predicates; it
does not prove arbitrary natural-language claims or turn a generic worker flag
into an official measurement. The planner must choose discriminating
observables. Externally performed operations require authenticated inputs from
the component authorized to perform and verify them.

## Waiting and continuation

The frontier, goal refresh and incremental planner use the same requirement
resolver. A final verifier's latest current proof determines each requirement;
an intermediate verification or an older full result cannot hide a later
pending result.

If all remaining work is externally blocked, the frontier returns
`await_external_dependency` and the objective reason is
`REQUIREMENTS_EXTERNALLY_BLOCKED`. No new retry or polling planner is created.
Other ready requirements can still progress.
The manager records this canonical wait separately from delivery stagnation;
missing new acceptances alone do not demand replenishment or replanning while
the external prerequisite is unavailable. Other material supervision obligations
and other objectives retain their existing controls.

The authorized external consumer registers its result through the existing
objective-input API with `requestReplan: true`, its immutable idempotency key and
the expected revision. The existing input event and feedback queue create one
planner owner. Replay does not create another event or planning run. Metadata
registered with `requestReplan: false` stays inert. Paused and cancelled goals
are not resumed by this mechanism. Replanning preserves historical acceptance
records and creates new verification work for the new result.

There is no new scheduler, event bus or external-service polling loop. An
`external_event` dependency is represented by an authenticated input receipt
from the existing integration, not by trusting an arbitrary worker message.

## Validation

`tests/requirement-outcomes.test.ts` checks evidence authority, exact hashes,
coverage, final-verifier selection, missing prerequisites and v1 compatibility.
`tests/requirement-outcomes-pg.test.ts` exercises real acceptance, frontier
projection, goal state, concurrent input replay, planner ownership, revision
fences and user pause/cancel under the isolated PostgreSQL test lane.

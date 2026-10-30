# Targeted validation in incremental plans

New planner responses declare `kind: delivery | validation`. A delivery may
produce code, research or analysis; it is not required to change source bytes.
Stored compiled plans and direct legacy compilation retain their existing
meaning. New generation rejects a missing kind rather than guessing from a
title, agent name or write path.

A validation must name one delivery in the same batch through
`validationTargetKey`, for the same requirement. The controller adds its
dependency even when the planner omitted `dependsOn`, selects `qa.integration`,
and compiles a `verify` task with the target's `sourceTask`. Existing execution
compilation makes this source read-only. The criterion is
`verification.json.passed == true`; reports and temporary data may be written
only under `evidence/` and `data/`. Other dependencies provide evidence and
cannot change the named candidate. A validation cannot target another
validation, create a dependency cycle, or authorize source writes.

Validation workspaces never become source producers for downstream deliveries.
A delivery gated on validation inherits the original delivery producer. Joins
and final integration take only delivery patches. Final verification depends
on integration and all targeted validations: their acceptance gates the final
result without contributing unchanged validation patches to integration.
Intermediate checks should have a specific purpose; final independent QA is
still present and is not replaced by a passing intermediate report.

`verifiedBatch` identifies a unique final verifier from persisted dependency
edges. It must follow every task, inspect a work/integration source whose
ancestry includes every source contribution, and have passing current
acceptances and canonical candidate metadata for every task. An intermediate
acceptance, two terminal validation branches, cycles, failed acceptance or
active/uncertain execution cannot advance the incremental frontier.

## What this change proves, and what remains

This fixes the semantic planning defect where a candidate check was compiled
as independent writable work against the old baseline. The tests cover schema
generation, persisted DAG edges, actual scheduler dependency admission,
producer workspace routing, the read-only runtime request, and batch
advancement without any live provider or production namespace.

It does **not** add a generic cryptographic source handoff. Current
`worker.prepare` receives source and consumer workspace IDs, not the accepted
producer run/fence/candidate digest. The existing published-rebase verification
binder handles a narrower workflow. Therefore the routing test is not proof
against post-acceptance source mutation. A subsequent change should freeze an
accepted producer proof, verify its candidate artifact and source digest under
workspace locks, and check the consumer's prepared baseline before model
launch. Do not claim such a guarantee from a matching workspace name alone.

Likewise, automatic replay of accepted normal `changes.patch` artifacts is a
separate contract. Recovery references are evidence, not composition authority;
this change does not promote them or fabricate repair reports.

## Isolated validation

Run only on a provisioned development server with its `agentos_test` database
and isolated configuration. Run TypeScript checking and these suites:

- `tests/validation-target-planning.test.ts`
- `tests/incremental-planning.test.ts`
- `tests/planning-subtasks.test.ts`
- `tests/planning-supervision.test.ts`
- `tests/objective-inputs.test.ts`
- `tests/runtime-delivery-contract.test.ts`

The new-generation missing-kind case is a counterexample for the previous
schema, which accepted it. The legacy QA fixture also demonstrates the old
parallel baseline binding, while the typed fixture requires the producer edge,
producer source and read-only validation. No synthetic receipt counts as live
autonomy or performance qualification.

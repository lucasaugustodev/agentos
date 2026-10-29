# Final delivery selection in the progress meter

Tasks created in one planning transaction have the same `created_at`. UUID
ordering cannot distinguish an intermediate check from final QA, or a source
join from final integration.

The progress projection now reads the persisted dependency IDs and plan
revision alongside its existing small execution metadata. It does not load
embedded inputs or alter acceptance, criteria, task state or runtime authority.

Within the current revision, the integration gate must be the unique
integration downstream of the other integrations. The verification gate must
be the unique verification downstream of all delivery nodes in that boundary.
Cycles, missing dependencies or ambiguous gates remain pending with
`selectionIssue`; the manager receives a replan indication while live
executions remain protected.

For old unversioned tasks, timestamps distinguish disconnected generations.
Selection within a connected graph uses dependencies. Equal timestamps across
disconnected candidates are ambiguous and never resolved by UUID. Versioned
plans use the highest explicit revision before considering timestamps.

A passed intermediate check cannot increment final verified delivery, reset
delivery stagnation or supply `lastDeliveryAt`. Candidate identity still comes
from the canonical runtime manifest and must match between the selected
integration and final verification. This is progress attribution, not an
additional acceptance or a generic cryptographic source-handoff guarantee.

Run `tests/operation-progress.test.ts` on the isolated development server. Its
counterexamples include identical timestamps, an intermediate UUID sorting
after final QA, equal and differing intermediate digests, pending and accepted
final QA, source joins, multiple terminal checks, cycles, and the actual
PostgreSQL metadata projection. Also rerun `tests/validation-target-planning.test.ts`
and the management projection suites before publication.

# Versioned canonical source outputs

New controller-created planning runs persist `feedback.sourceContractVersion:1`
at creation: initial planning, bootstrap, repair revisions and incremental
advancement. This adds a contract to that new planning attempt without editing
the original objective request, historical plan, existing task, instruction
snapshot or persisted proposal. Explicit objective requests may also carry
`sourceContractVersion:1`. Old planning runs without either opt-in retain their
original contract. A new run for an existing objective adopts the new version;
resuming an old run does not backfill it.

The model must declare a source contract on every delivery. Complete plans also
set `sourceContractVersion:1` and declare contracts on all their tasks. A compact
incremental validation names its target; the compiler inherits that target's
contract, combines contributor obligations at joins/integration, and retains
them at final independent QA.

```json
{
  "version": 1,
  "required": [
    {"path":"kernels/fused.py","kind":"file"},
    {"path":"kernels/reference.py","kind":"file","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
  ],
  "preserve": [{"path":"src/existing.py","kind":"file"}]
}
```

The hash above is an illustrative placeholder, not an admissible known reference.
`required` files must exist in the canonical runtime inventory; a required
directory needs at least one inventoried descendant. Optional SHA-256 applies
only to an exact file and must come from an authoritative known reference.
`preserve` entries must already exist in the baseline and keep their original
file bytes; directory preservation covers its original inventoried descendants.
Importing an absent reference therefore uses `required`, not `preserve`.

Report-only work uses `required:[]`, an explicit `preserve` array, and
`noSourceReason` of at least 20 characters. A contract with required source
cannot also claim report-only output. This forces an explicit semantic choice;
it cannot independently prove that the model understood the user's request.
No regular expression over the task description becomes source authority.

Paths are exact workspace-relative paths without aliases, globs or trailing
slashes. Reserved paths and runtime-inventory exclusions are rejected, including
`evidence`, `data`, cache/report directories and `.log`/`.pyc` files. A directory
whose name ends in `.log` remains a valid directory. Normalization sorts entries;
it never repairs unsafe paths or expands scope. Each required work/integration
output must fit an explicit `writePaths` entry. For example, requiring
`kernels/fused.py` with only `tools/` and `evidence/` writable fails planning.

The controller stores the normalized contract and the exact existing write scope
in `task.execution`. The ordinary immutable admission snapshot captures both in
`run.instruction_snapshot.task.execution`. Source files must be written to those
canonical source paths. Code saved only in `evidence/` cannot satisfy a source
output. Reports, diagnostic scripts and temporary state remain attempt-local.
Source verification supplements existing criteria and independent QA; it never
turns a model's `passed:true` into source or test evidence.

Worker prompts also distinguish actual CI capability: `request_validation` is
a manager action. A worker with captured registered policy emits the supported
`validation-intent.json` and stops for controller-owned CI. A worker without that
policy reports unavailable database coverage, without installing substitute
databases or inventing validation receipts.

Run the source contract planning suites on the isolated development server.
Counterexamples cover excluded paths, the kernels/tools scope mismatch, dropped
integration/QA obligations, report-only declarations, immutable admission, exact
hash conflicts, legacy history and version adoption only on new planning runs.

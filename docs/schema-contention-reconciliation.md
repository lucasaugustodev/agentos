# Canonical backlog preparation under live readers

The supervision installer previously acquired its case-table lock before the
task-table lock. A reader could own the task table while requesting the case
table, producing a real PostgreSQL deadlock. Verification binding had an analogous
settings/task inversion. Retrying the entire backlog installer could encounter
the same cycle again after earlier installer units had already committed.

These two DDL units now pre-acquire their declared relation set in sorted order,
with `NOWAIT`, before any statement in that unit changes a definition. The prefix
and original DDL remain in the same PostgreSQL query/transaction. If any relation
is busy, PostgreSQL aborts the unit with `55P03` and releases its earlier locks.
Existing bounded retries apply; timeouts, retry counts and runtime ownership do
not change. No reader or worker is cancelled. An absent relation is created by
the original DDL. The helper belongs to the backlog source fingerprint.

The seven-module base migration closure is unchanged. A completed base receipt
can therefore remain valid when only backlog preparation needs reconciliation.
This focused change does not fix every possible lock inversion in the base
migration or unrelated installers. Persistent contention still fails closed;
it is not evidence that repeating preparation indefinitely would make progress.

After a failed operational preparation:

1. Retain the original intent, candidate digest, execution unit and failure
   receipt. Confirm the unit's PID is zero and its cgroup empty; an uncertain
   process prevents reentry.
2. Under the existing publication lock, read the active pointer and all five
   schema receipts. Record a reconciliation linked to that same intent. A
   completed base plus an applying backlog is partial effect, not a rollback or
   no-effect result. Never edit schema settings to claim completion.
3. Reenter the exact candidate's canonical preparer only through an explicitly
   authorized reconciliation attempt with a new execution identity and a durable
   pre-dispatch record. Preserve completed phases and let the preparer perform
   the existing incomplete-phase checks. Do not rewrite the old execution result.
4. A source fix requires a newly validated candidate and its own exact intent,
   linked to the reconciled prior failure. Do not relabel changed bytes as a
   replay of the old candidate. The unchanged base closure allows its completed
   receipt to be reused; the changed backlog closure requires a new completion.
5. Verify canonical completion against the candidate's source and effective
   configuration before changing the pointer or stopping a service. Failure
   leaves publication pending. A symlink change never reverses committed DDL.

The focused PostgreSQL suite reproduces the original two-relation deadlock,
then exercises the actual installers with live readers. It checks `55P03`,
unchanged definitions, preserved reader sessions, incomplete backlog refusal,
completed base preservation and eventual canonical completion after the reader
releases its lock. Full release validation remains required before deployment.

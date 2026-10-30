# Planner summaries for large operator inputs

The authenticated objective-input registration API accepts optional
`planningSummary`: 20–3000 characters of operator-provided evidence. The request
digest and immutable receipt bind it to the full input bytes, their SHA-256, the
objective and its requirements. Changing a summary under an existing request ID
is an idempotency conflict. The existing secret checks also apply to summaries.

The planner still has a 6000-character per-document limit and a shared
12000-character context limit. When an entire input cannot be included, a complete
registered summary may use the remaining budget. The controller never generates
a summary by slicing source code or inferring content from a filename.

A summary-only entry retains the original `omitted` reason and byte identity,
adds `summary`, `summaryDigest` and `originalContentRead: false`, and leaves
`contentComplete: false`. `contentCharacters`, `summaryCharacters` and
`contextCharacters` distinguish original content from the summary and account for
the same shared budget. If a summary does not fit, `summaryOmitted: total-budget`
is recorded without truncation. A fitting original is preferred over its summary.

`PLANNING_DECISION_REQUESTED` records `summaryIncluded` and its digest separately
from the existing full-content `included` flag. This proves what the planner was
given; it does not prove that its decision used the evidence correctly.

Worker inputs remain the original immutable files. A summary grants no host-path
access, permissions, acceptance, or authority to modify a live attempt. It is an
operator claim to verify, not a substitute for source, test results or an external
measurement. Large existing inputs without a registered summary remain explicitly
omitted; the controller does not silently reinterpret historical receipts.

Validation uses both projection tests and the isolated PostgreSQL registration
and planner path, including receipt binding, conflicting retries, credential
rejection, budget exhaustion and preservation of original worker bytes.

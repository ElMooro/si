# Portfolio position mutation contract

The authenticated manager records holdings. It does not place orders, reconcile a broker account, establish account NAV or qualify an investment recommendation. The existing Worker route, native role, authentication secret reference, resources and schedules are unchanged.

## Inputs and arithmetic

Position creates require a supported ticker string, typed finite quantity and nonnegative unit cost. Booleans, numeric strings, nonfinite values, unsupported fields and values that exceed DynamoDB numeric precision fail before mutation. Negative quantities remain short records; explicit zero remains zero. Cost totals use exact supported Decimal multiplication. Updating only quantity or unit cost requires a valid stored counterpart. Missing basis never becomes zero. Optional stop values must be positive; an explicit null on edit removes stop or target. Target values are stored fields with an unverified denominator, not executable weights.

Canonical update fields and existing `new_*` aliases are accepted; conflicting aliases fail. The browser sends only changed fields and the exact `expected_record_etag` from the displayed record. Unchanged rounded numeric displays and unsupported legacy values are not echoed back as new measurements.

## Concurrency and acknowledgements

Create is conditional on absence and cannot replace an existing position. Update, stop change and removal first read consistently, optionally verify the caller's complete-record token, then condition on all observed fields and absence of known position-owned fields. Updates preserve unknown metadata. A stop edit cannot create a missing position. Conditional conflicts return 409 without automatic retries; ambiguous write acknowledgements return an explicit unconfirmed outcome and require a fresh read. Optional legacy callers without an edit token receive server read/write conflict protection but no stale-form guarantee.

This is not a general transaction across the account. A new unknown field added by an external writer is preserved by updates but is not an absence check for every conceivable schema. Delete conditions cannot provide a generic revision guarantee against such external writers. Exact record tokens bind the complete DynamoDB-typed record, including values that the legacy numeric display rounds. No generic account revision or execution permission is inferred.

Book success and snapshot refresh are different outcomes. Only an explicit Lambda asynchronous 202 acknowledgement is `queued`; it does not prove snapshot completion. Unknown refresh acknowledgement remains `unconfirmed` after a confirmed book write. No-op removal requests no refresh. Acceptance never invokes this native refresh path.

## Complete lists and page behavior

All queried pages are strongly consistent individually. Full lists retain every returned record within 100 pages, 10,000 items and 4 MiB bounds. Invalid rows, duplicate identities, cursor cycles, interrupted queries or exceeded bounds fail the complete request instead of returning a partial book as complete. Multiple pages are not an atomic point-in-time account snapshot. Empty arrays mean empty only after a validated complete response.

The manager reads complete bounded strict JSON. Failed reads clear prior rows and aggregates and disable edits. A late response cannot overwrite a newer request. Invalid or duplicate identities remain visible and cannot be edited. Every valid record shows computed quantity-times-unit-cost separately from the stored total's reconciliation status. Known signed basis states its input coverage and unverified currency assumptions; it is not NAV. Text is escaped, form labels are associated, and the wide table scrolls within its region at narrow widths. Feedback remains above the table.

## Evidence and limits

Nine complete invented predecessor failures, whole inert predecessor sources, 28 focused native cases plus eight existing integration checks, 13 focused browser behavior tests, and the isolated browser's complete invented calls and state transitions are retained. The current handler runs with invented DynamoDB, SSM and Lambda responses in the preview; network and subprocess access are blocked. Desktop and 360-pixel layouts, successful edits, stale edits, failed-list recovery and uncertain refresh were checked. Companion brief/history scripts are preserved but were not executed in the isolated preview.

Read-only native acceptance checks the exact receipt, whole deployed source closure, CodeSha256 and original resources without reading account packets or invoking producers. Actual private mutations and publication, brokerage/instrument/currency reconciliation, private publication ordering, legacy watchlist administrative mutations and investment qualification remain separate work. Capital sizing remains blocked.

# Watchlist administration integrity

This extends the existing authenticated administrative actions. It changes research-list records only and grants no investment, execution or account-reconciliation permission. Native resources, authentication, schedules, automatic source selection and the portfolio page remain unchanged.

## Individual actions

`add_watchlist` validates typed symbol, source and optional notes, then conditionally creates an absent item. It cannot replace an existing owner record or erase notes and metadata. Explicit unfamiliar source names remain distinct ownership labels. An explicit empty note is retained. Unknown request fields, non-string owners and unsupported identities fail before writes.

`remove_watchlist` reads the row consistently, accepts an optional complete `expected_record_etag`, and conditionally removes the observed record. Standard owner fields added after the read, manual conversions, note changes and other changes to observed fields cause a conflict. An absent row is a no-op. A successful acknowledgement must include the matching removed record; an ambiguous outcome is never automatically retried. Older callers without the form token have server read/write conflict protection but no stale-form guarantee.

Full list results now include watchlist record tokens and a deterministic `watchlist_etag` when the watchlist was actually read. The population token binds sorted stored identities and complete DynamoDB-typed record tags; query order does not change it. The token does not turn individually consistent pages into an atomic account snapshot.

## Bulk automatic-row clear

`clear_auto_watchlist` reads the existing automatic-sync checkpoint before reading every watchlist page. It selects only exact `AUTO_TIER_S` and `AUTO_TIER_A` owners. Manual, missing, malformed and custom ownership labels are preserved. An invalid selected identity, incomplete read, duplicate identity, cursor cycle or bound violation prevents all mutations.

The optional `expected_watchlist_etag` binds the caller's displayed population. One DynamoDB transaction condition-checks the observed checkpoint, or its absence, and conditionally deletes every selected row. This follows the [DynamoDB transaction API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html): all operations succeed or none do. The whole request must fit 99 selected deletes plus one checkpoint check and a conservative 3.5 MB encoded request bound. Larger requests are withheld, never split or silently truncated. Each request has a UUID client token; there is no application retry or unconditional/batch-write fallback.

Only an explicit typed HTTP 200 SDK acknowledgement reports deleted symbols/counts. A rejected transaction reports rejection; an uncertain transport/acknowledgement reports `WRITE_UNCONFIRMED` with no claimed deletions. Transaction permissions are not expanded. Existing IAM execution is not demonstrated by invented tests or package acceptance.

The checkpoint is condition-checked, never rewritten with invented source provenance. Its prior source identity and idempotency remain intact: the same committed automatic-source revision will not restore a cleared row. A later valid source revision can legitimately add automatic rows. The result promises deletion of the observed matched rows only, not a permanently empty watchlist. New arbitrary unknown attributes from external writers are not a general schema-independent revision guarantee. No private snapshot refresh is invoked by these watchlist actions.

## Acceptance

Eight complete predecessor failures and the whole original handler are retained inert. Twenty-seven focused cases plus the preceding 28 admin cases and eight existing integration checks run against the current handler with invented state. They cover exact ownership, complete pagination, stale tokens, concurrent owner conversion, checkpoint races, all-or-none behavior, bounds, uncertain acknowledgement and local AWS service-model validation. The current automatic-sync planner confirms checkpoint idempotency. AST checks ensure the position and HTTP functions remain unchanged. Complete events, responses, table states and mocked calls for all 43 dispatches are retained in `tests/fixtures/watchlist-admin-synthetic.json` without shortening the large-boundary cases.

Read-only native acceptance checks the exact receipt, full source closure and unchanged resources. It never reads private/current/provider/history packets or invokes a producer. Actual account edits, table transaction IAM execution, private publication ordering, source qualification and portfolio consequences remain unverified. All platform workstreams remain open; capital sizing remains blocked.

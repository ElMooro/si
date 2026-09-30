# Complete portfolio book reads

The snapshot reads each complete position and watchlist partition before fetching marks or publishing an accounting result. A malformed page is unavailable, not an empty account. This is a source-integrity contract; it does not reconcile a broker account, establish NAV or grant sizing authority.

## Complete native read

`query_pk` requires a typed page with an explicit item array. It reads all continuations with `ConsistentRead=True`, retaining source order and exact DynamoDB Decimal values. Every storage identity must match the requested partition and have a nonempty string sort key. Duplicate stored identities, repeated cursors, malformed continuations, missing item arrays, read errors and unsupported complete-record encodings fail the request. An explicit empty item array with no continuation is a valid empty result. Empty interior pages continue normally.

Each partition is bounded to 10,000 records, 100 pages, 8 MiB of complete encoded row-array evidence and a twenty-second acceptance deadline. Bounds never truncate the account. The acceptance clock is checked before and after each query and while processing rows; it cannot preempt an SDK socket operation. No error includes a private record or underlying response body. Unicode must encode as valid UTF-8. Binary/set or otherwise unsupported metadata fails rather than disappearing from the source evidence.

Completeness is not simultaneity. Pages and the two partitions are read individually; concurrent writes can still produce a mixed-time view. The snapshot identifies the read start/end window, per-partition counts and limits, and explicitly states `STRONGLY_CONSISTENT_PAGES_NOT_ATOMIC_SNAPSHOT`. This is not a reconciled account revision. The existing conditional automatic-watchlist sync happens earlier and remains a separate operation; a later failed book read does not undo that completed sync.

## Numeric and source evidence

Stored numeric values are no longer converted to binary floats before retention. `accounting.source_positions` and `accounting.source_watchlist` keep the complete original rows, with Decimal type and exact representation preserved by the existing source encoder. Descriptive valuation arithmetic still follows the separately documented holdings-accounting model and its numeric limitations. Source precision is not a claim of exact decimal valuation or predictive validity. Invalid symbols remain source evidence and are excluded by existing accounting eligibility rules.

Malformed/incomplete books prevent mark collection and both private/S3 snapshot publication. Genuine complete empty books can publish explicit zero. The whole payload still requires finite JSON before either sink is called. This change preserves the established publication path; ordering across concurrent snapshot writers and across the two sinks remains unresolved.

## Verification

Four full invented predecessor cases and the complete 41,506-byte original handler are retained inert. Sixteen new current-source cases cover malformed and partial pages, typed exact values, pagination, cursor and identity failures, bounds, read deadlines, encodings, whole-handler suppression and true emptiness. AST comparison limits this change to the reader and handler; sync, provider and accounting helper functions remain unchanged. The complete four before/after scenarios retain their responses and mocked publisher calls. Acceptance does not use actual account data.

The snapshot release remains on the exact offline candidate path. Its candidate ZIP must match reviewed source and pass 123 current invented tests plus four handler integrations before promotion, without native invocation or runner credentials in the child. Read-only native acceptance checks the exact receipt, source closure, live alias, original 512 MiB/180-second resources and both original hourly :40 bindings. Actual private publication, independently verified provider marks, account reconciliation, transaction execution and investment qualification remain unverified; capital sizing stays blocked.

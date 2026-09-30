# Portfolio quote collection bounds

The snapshot's complete book can contain far more symbols than ten parallel requests can finish safely within a native run. Per-request limits do not bound the entire workload. The predecessor eagerly submitted every symbol, retained an unbounded sum of complete quote bodies, and replaced an unexpected failed task with an unexplained null.

Three complete invented predecessor runs are retained inert: 41 queued tasks and 5,373,952 body bytes; 12 tasks that continued through 552 simulated seconds; and 12 failures reduced to null. No provider or account data was read to reproduce them.

## Collection contract

- Every supported requested identity remains in `accounting.source_prices`. Requests are deduplicated and dispatched in lexicographic symbol order; the original complete positions and watchlist remain separately available. Duplicates do not become separate quotes. Invalid or excessive request populations are refused whole.
- At most ten tasks are pending. The collector submits the next task only after completing work and reserving room for its whole maximum 128 KiB response. It retains at most 4 MiB of complete source bodies, including unusable complete responses. Smaller bodies release unused reservation. It never truncates a body or drops an acquired original to admit another request. Conservative reservation can leave unused room.
- A shared 45-second submission and mark-acceptance budget is capped by native remaining time minus a 30-second finalization reserve. Invalid native time metadata prevents all requests. Offline calls with no context use the 45-second maximum and identify that fact. These are operational budgets, not freshness or investment thresholds.
- Each request uses the lesser of its existing 20-second acceptance limit and the shared deadline. Its socket timeout is capped by remaining time and the existing eight seconds. Late complete responses remain inspectable but cannot supply marks. Incomplete responses cannot claim complete original bytes.
- Already-started tasks are drained before return. This is not a hard process deadline: DNS, operating-system stalls and in-flight blocking calls can outlast an acceptance clock. No thread cancellation or background publication claim is made.
- Unattempted identities carry `NOT_ATTEMPTED` and a fixed deadline or body-reservation reason. Unexpected task failures retain their identity and `COLLECTION_TASK_FAILED`, without exception text. Unknown request-attempt state remains null, distinct from false. No missing mark becomes zero.

`accounting.quote_collection` records the complete requested symbol order, task and unattempted counts, measured previous-close count, reason counts, retained bytes, limits, timing semantics and context-budget status. `COMPLETE_ATTEMPT_COVERAGE` means every unique task was attempted; it does not mean every quote was valid, current or suitable for trading. Empty input has explicit zero counts.

## Publication and acceptance

Existing accounting, complete-frame compatibility and mirror-size checks still run before either publication. An exhausted collection can produce an explicitly unpriced frame when the full frame fits those contracts. It cannot fabricate a partial account NAV or grant sizing permission. A whole-frame size failure leaves prior outputs unchanged. Automatic watchlist synchronization happens earlier and is not rolled back by a later refusal.

Twenty-three focused tests exercise the actual current handler, shared native budget, real threaded concurrency, exact 4 MiB boundary, every requested identity, released reservations, late responses, malformed task results and failures. All 146 snapshot unit cases plus four handler checks pass with invented data. Candidate validation remains offline; operation 6367 checks only the exact native package, receipt, alias and original resources/schedules.

Actual private publication, account reconciliation, original instrument/currency qualification, cross-writer ordering, hard wall-clock termination and peak process-memory guarantees remain unverified. Source-body bounds do not independently bound every expanded row or wire-serialization buffer.

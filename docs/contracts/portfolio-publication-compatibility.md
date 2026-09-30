# Complete snapshot consumer compatibility

The snapshot producer must not publish a frame that the existing portfolio risk model or browser snapshot binding cannot represent. The producer now validates every field before private-mirror and S3 publication, using the same typed JSON value encoding limits as those consumers.

## Reproduced failures

Three complete invented runs of the preceding reviewed source wrote both mocked sinks, but the current risk consumer rejected their outputs:

- A Decimal quantity `9007199254740993` became an unsafe binary64 quantity. Its exact stored Decimal evidence remained available, but that did not make the displayed quantity representable.
- An owner note with 130 nested arrays exceeded the consumer's depth limit.
- A source with 1,600,000 numeric risk flags produced a 16,012,951-byte spaced-JSON frame that passed the existing 20,000,000-byte mirror bound. The typed identity expands each number to nine bytes and exceeded its separate 32 MiB limit.

The full 16,035,164-byte invented predecessor fixture is retained as lossless gzip. The complete predecessor code is inert. Tests execute current reviewed source only.

## Boundary

`validate_snapshot_publication` counts the exact `typed-json-binary64.v1` encoded length without allocating another complete identity buffer. Objects include every UTF-8 key, arrays include every occurrence, and zero, false and null remain distinct values. Object order does not affect length. No field is omitted, rounded or truncated to make it fit.

The current consumer limits are 32 MiB encoded and 128 nesting levels. Numbers must be finite and safe in both runtimes; strings and keys must have valid UTF-8 representations. Unsupported types and non-string object keys fail with fixed diagnostics that do not copy private values. Cross-runtime vectors and the real current risk identity function verify byte-count parity.

The check precedes both publication calls and the `validation_only` return. Existing mirror-wire size validation remains. A refused publication leaves previous outputs in place; their timestamps are unchanged. This does not roll back an earlier automatic-watchlist transaction, provide an atomic account snapshot, or make separate publishers transactional. It does not validate source freshness, instrument identity, account NAV or investment performance.

## Acceptance

Fifteen focused cases include all three complete reproduced failures, a valid complete handler frame, exact boundaries, Unicode, types, signed zero, cycles, immutability and complete cross-runtime vectors. The snapshot suite has 146 unit cases plus four handler checks. Candidate ZIP validation runs those current invented tests without credentials, network or native invocation. Read-only operation 6366 checks the exact deployed package/receipt/live alias and original resources and schedules; it cannot read account data or invoke the producer.

The separate [quote collection contract](portfolio-quote-collection.md) bounds pending tasks, retained complete bodies and mark-acceptance time. Actual normal private publication, hard process deadlines, peak process memory, cross-writer ordering and portfolio qualification remain unverified.

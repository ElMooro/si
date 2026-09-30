# Portfolio retained-quote inspection

The portfolio page presents quote records and `portfolio-quote-collection.v1` already contained in its authenticated snapshot. It introduces no new data request, account mutation or native producer change. Previous-day bars are labelled **PREV CLOSE**, retain reported precision and do not assume a currency.

## Evidence and interpretation

- Every recorded identity remains inspectable, including malformed, missing, unavailable and unattempted records. Records paginate in groups of 25 without truncating the population. Genuine empty collection is distinct from absent or legacy evidence.
- Collection coverage is checked against the complete requested population, task states, identities, measured-result counts, failure counts and retained-body byte budget. Inconsistent metadata is visible. Attempt coverage is not data validity, freshness, source authenticity or investment qualification.
- The reported price requires supported metadata, a matching requested symbol, a positive finite representable number, a measured result and a valid previous-day bar-window timestamp. Currency and held-instrument identity remain unverified. Bar-window time and acquisition start/end remain separate.
- Explicit inspection verifies canonical base64, exact byte length and SHA-256 for the whole retained response within the native 128-KiB bound. Literal text preserves numeric spelling, large integers and whitespace; source markup remains inert. Invalid UTF-8 remains available as exact binary bytes. Complete recorded failure metadata remains accessible when no original body exists.
- Verification is local and on demand. The exact verified bytes form an octet-stream Blob. New frames, page changes, close, page hide or failed loads invalidate pending inspections and revoke old URLs. Repeated age-only rendering preserves an open inspection of the same frame.
- The selected original appears above the table. Opening and closing restore sensible keyboard focus; live status text announces verification. Narrow screens scroll the table internally without page-wide overflow.

## Acceptance and limits

Current reviewed native code produces seven complete invented frames with all book, research, quote-provider, publication and authentication I/O mocked. Cases cover complete, partial, binary, empty, corrupt, legacy and mismatched identities, with 105 watchlist rows and 106 quote identities in the complete case. Full original bodies and source hashes are retained in the test fixture.

Tests cover native-to-browser record preservation, exact original bytes, failure recovery, generation races, URL cleanup, input validity, keyboard focus and pagination. Browser checks cover a 1,280-pixel desktop and 360-pixel mobile viewport. Actual browser file-save completion, live authentication and actual private publication remain unverified. No actual account data or provider request is used for acceptance, and no native producer is invoked. Investment qualification remains blocked.

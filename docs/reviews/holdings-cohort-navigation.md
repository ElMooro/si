# Holdings cohort Previous/cache navigation — draft review

The existing qualified ownership panel only advanced forwards. This frontend-only repair adds Previous and reuses verified immutable page cache entries on both etf-holdings.html and flow-lookthrough.html. A displayed-page cursor is separate from the verified prefix, so revisiting a page does not recount identities or weaken cross-page duplicate/order checks. Dates and measurement selections reset navigation; cancelled or superseded responses cannot repaint the panel. Cache hits still check measurement expiry.

The eligible-fund denominator remains specific to the selected economic-date cohort. Displayed record ranges and other-date exclusions are explicit. Raw observations and fresh lower bounds remain unranked, with separate expiry rules. The existing selected 1–8 fund panel, IDs, source contract and navigation remain intact. No backend, provider, schedule, capital, scoring or allocation code changes.

## Validation

Four new regression groups failed against the previous implementation and pass with this change. The final focused suite has 17 passing tests, including repeated Next/Previous, pending-page cancellation by Previous/date selection/cancel/retry, delayed third-page responses, cached-page expiry and the unchanged 32-page request budget.

- Frontend suite: 2,239 passed.
- Deployment suite: 1,003 static and 15 mocked shell checks passed.
- Page syntax: 599 graphs, zero syntax errors.
- Wiring registry: 36 pages / 143 wired entries, zero missing or stale.
- Public/private boundary check passed.
- Local synthetic browser preview: both pages at 1440px and 390px, repeated cached navigation, retry/cancel, expiry and existing-panel isolation; no horizontal overflow or page errors. All requests intercepted locally. These are preview results, not live production acceptance.

A captured real public canonical packet generated 2026-09-30T22:48:28.445590+00:00 and its retained normalized artifacts were replayed locally. The September 29 cohort has 11 eligible funds and 21,144 records. The sequence 1 → 2 → 1 → 2 retained a 400-record verified prefix and the 11-fund denominator, with five local object reads (three proof/metadata objects and two pages), no repeated reads and no network requests. This captured evidence is not a new acquisition or freshness claim. No holdings records are committed in this PR.

## Bounds, cost and rollback

Existing page request/byte/cache limits remain 32 requests and 8 MiB per attempt, with at most 200 records per page; proof metadata has separate unchanged bounds. Cached revisits add no GETs. Source JavaScript grows by 1,866 bytes uncompressed (607 bytes gzip versus the branch base). There are no additional scheduled invocations, provider calls or backend storage writes. Normal static-site build/storage/transfer overhead applies when eventually deployed.

Rollback is a frontend-only revert of this navigation change and its cache tags. Preserve the previously released backend summary/replay compatibility. This draft is not merged or deployed; independent review and subsequent production acceptance remain required.

# Khalid shared snapshot performance — draft only

Baseline implementation: PR29 merge `cfa7f41305d1d4bf0e8f7b8dc8b31d5e6b70148d`.
Draft starts from main `7e7044328`. No backend source, qualification schema,
threshold, action, permission, cadence, infrastructure or vendor acquisition changes.
Independent exact-head review is required before any release.

## Problem and change

The Khalid controller and qualification panel independently fetched and parsed the
same 20.88 MB publication. The panel expanded and validated every row on each
search, filter and pagination event. The chart workspace also fetched again on
reopening. Ten rendered cards did not bound validation or transport work.

The existing shared renderer now owns one in-memory snapshot per document:

- Concurrent consumers share one pending fetch/parse. Every downloaded object is
  validated independently, including changed bodies carrying the same revision
  string. There is no revision-string lookup that can reuse another object's PASS.
- The owned packet, expanded projection and indexed result arrays are deeply frozen.
  One ticker index contains at most one entry per distinct ticker in that packet;
  there is no growing cache of searches, revisions or pages and no disk persistence.
- Reads check snapshot generation, monotonic observed wall clock and the earliest
  publication/PASS-source deadline before returning the existing projection.
  Source SLA equality remains valid; the first later millisecond is unavailable.
  Publication expiry is exclusive. Observed expiry or clock rollback is terminal
  for that snapshot, even if the clock later moves back into the original interval.
- Timer, visibility, pageshow and focus checks remain. Explicit refresh revokes
  prior authority immediately, aborts a superseded request and notifies both the
  page controller and panel. Late responses cannot replace the active snapshot.
  Errors/unknown versions clear qualification; no old PASS survives fallback failure.
- Transport reuse is limited to 60 seconds on a new consumer load; this is not a
  polling schedule or a new evidence SLA. Explicit refresh always bypasses reuse.
  Existing public-proxy fallback is retained for transport/HTTP failures; a successful response with malformed JSON fails closed without substituting another artifact. No bars, providers or producer calls.
- Dashboard and panel refreshes use the same store; script order establishes it
  before the dashboard controller. Chart close/Escape/replacement releases its
  subscription, timers and detached-control closures; the last subscriber cancels a pending request.
  Chart reopening can reuse a still-current snapshot. Refresh is available even
  for unavailable/unknown qualification results.

The original `jhSniperQualification` pure validator remains unchanged. Untrusted
callers of that function still get full validation. The optimized path is private
ownership of newly parsed, frozen data, not trust in mutable caller input. All
backend/requested fields are retained; numerical thresholds remain backend-owned.
Other chart consumers and the retained pure technical-measurement functions are
unchanged. No cross-document/browser-tab sharing is claimed.

## Real retained packet and measurements

The retained public packet was naturally published October 1 at
`2026-10-01T06:25:51.123970+00:00`, after PR29's `05:26:59Z` deployment.
It contains **3,643 candidates and 20,876,164 bytes**. Its source SHA-256,
complete per-interaction timings, counts and heap measurements are recorded in
`khalid-snapshot-performance-evidence.json`. The full input remains outside the
repository at `/workspace/scratch/pr29-release/natural-khalid.json`; a later live
packet is not an exact substitute. The original acceptance receipt/hashes and
natural acceptance remain in `/workspace/scratch/pr29-release/acceptance.json`.

The actual page/controller/shared renderer and chart workspace were replayed at
1440/390 pixels with that entire retained packet. All network requests were
intercepted, unrelated scripts were excluded, and the clock was fixed one minute
after publication. Instrumentation counts calls/JSON parses/full validations;
it does not change their result. No private or production UI actions occurred.

Final-source run (source hashes recorded; isolated from this lane's test gates):

| Context | Fetch/parse before → after | Full validations after interaction sequence | Median interaction ms before → after | Post-GC JS heap MB before → after |
| --- | --- | --- | --- | --- |
| Khalid 1440 | 2 → 1 | 26 → 1 | 43.4 → 10.6 | 39.99 → 23.58 |
| Khalid 390 | 2 → 1 | 26 → 1 | 41.2 → 11.3 | 39.99 → 23.58 |
| Chart 1440 | 1 → 1 | 20 → 1 | 27.2 → 1.2 | 20.72 → 23.30 |
| Chart 390 | 1 → 1 | 20 → 1 | 27.7 → 1.3 | 20.72 → 23.30 |

Sequences repeat AAPL search, clear, asset filter, readiness filter, next and
previous. Disabled chart pagination buttons correctly do no work. Dashboard
refresh adds exactly one fetch/parse/validation; chart close/reopen adds none
within the reuse window. All three runs had no browser errors.

Initial-load measurements are noisy and **not consistently better** across runs.
The final run's dashboard changed 1.76 → 1.09 seconds (1440) and 1.32 → 1.15
seconds (390); chart increased 1.01 → 1.25 and 0.89 → 1.21 seconds. All three
runs, including the first run concurrent with other checks, are retained.
Freezing/indexing is an up-front cost. Chart retains approximately 2.6 MB more JS
heap for the full validated index, while the dashboard eliminates a second copy
of the full data. These are local observations, not mobile CPU, production network,
edge-memory, capacity or p95 guarantees. The HTTP body remains 20.88 MB.

## Validation and bounded inventory

Nine new store tests cover single-flight/parse, deep immutability and indexed
parity, new malformed/unknown bodies with unchanged revision, source/publication
boundaries, terminal rollback/expiry, superseded responses, public fallback failure,
last-subscriber abort, and the one-minute reuse boundary. Existing full-validator
mutation tests remain unchanged. Actual page browser regressions still cover
20 timer/resume cases and now also cover same-revision unknown-version replacement,
recovery, a single shared initial request and chart subscription cleanup.

The chart observation preservation test permits only the two exact script cache-key
updates; its assertion that every remaining HTML byte matches is retained.
All backend source is unchanged; native scoring parity tests are rerun as a guard.
Final gates: 2,335 frontend, 70 native, 20 browser freshness cases plus replacement/recovery checks, 599 page graphs, 1,018 deployment and 15 shell checks; wiring, preflight and secret scan clean. The draft has exactly 12 changed files, listed in its PR description.

Reproduce with Node, Python and Playwright/Chromium installed:

```sh
node --test tests/*.test.js
python aws/lambdas/justhodl-khalid/tests/run_tests.py
python scripts/check_page_scripts.py
python scripts/gen_engine_wiring.py --check
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/usr/bin/chromium node tests/khalid-qualification-browser.cjs /tmp/kperf-browser
node tests/khalid-snapshot-benchmark.cjs /workspace/scratch/pr29-release/natural-khalid.json /tmp/kperf-benchmark
```

The benchmark requires the pinned baseline Git object and the retained packet; it
never retrieves sources automatically. Deployment/static tests run separately
with dummy credentials, metadata disabled and external sockets denied.

## Separate later proposal: backend transport duplication

The new qualification field adds **8,425,194 bytes** within the accepted natural
packet. This draft does not migrate its schema or drop any field. A separate
review could normalize repeated row-level clocks, source-health records and
provenance into referenced dictionaries, preserving complete evidence and old
consumer compatibility through an explicitly versioned transition. Such a change
requires producer/consumer parity, malformed-reference tests and real-packet
measurements. No implementation or authorization of that migration is implied here.

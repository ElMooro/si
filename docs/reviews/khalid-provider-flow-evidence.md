# Khalid provider-flow evidence — draft, not deployed

Khalid's legacy capital-flow discovery reader expects retired z-score/breadth/
pump fields. Native Radar contains dated provider observations instead and yields
zero legacy discovery records. This change makes those measurements visible in
an additive `provider_flow_research` output and panel under Opportunities.

## Authority and compatibility

The existing Radar input is reused as one provider root. There are no new reads,
subscriptions, vendor calls, schedules or investment votes. The informational
projection is attached only after decisions and the candidate ledger are built.
Scoring, confidence, source counts, rankings, readiness, risk/capital permissions,
allocations and lifecycle behavior are unchanged. Native data is not substituted
into old shadow votes; the legacy ETF producer and brief compiler are untouched.
The existing input contract remains compatible with old Radar packets; the new
section rejects old packets independently. Old Khalid outputs without the section
remain displayable. No required schema key is added. Registry additions document
informational consumption only; bundled JSON has no config/ twin in this repo.

## Evidence and semantics

Captured public Radar: 2026-09-29T22:30:05.863044+00:00, canonical compilation
2026-09-29T22:01:03.718009+00:00, common effective date 2026-09-28. Source expiry
2026-10-01T00:00:00+00:00. The labelled compressed fixture preserves all 300 funds.
Tests freeze the clock at 2026-09-30 19:15 UTC and separately test expiry. It is
captured real public data, not a current production-output claim.

The projection yields 289 available fund summaries and 46 configured baskets;
10 stale funds and ARKB's missing common-date observations remain unavailable.
XLU's 1/5/21 amounts are -21,738,274.80 / 484,462,042.70 / 817,371,620.75 USD.
SMH's five-observation amount is 867,505,866.50 USD. Real zero remains zero.
Dates are exact reporting observations, never calendar weeks or months. Group
subtotals remain explicitly partial; full totals are reconciled against accepted
fund windows and withheld on conflicts. Overlapping groups must not be added.
Leveraged/inverse funds stay separately labelled and cannot enter basket totals.
Tags are configured, unverified research labels, not full industry coverage.
Stock-context arrays and implied constituent purchases are not copied.

The reader validates native contract/engine, permissions, ticker identity, decimal
strings, acquisition/effective/processed dates, future timestamps, per-source
expiry, reporting grids, counts, missing dates and basket reconciliation. A loader
error, malformed input or expired root yields explicit unavailable evidence. The
browser rechecks expiry every minute and when controls change. References point
to retained public run/history artifacts; Khalid does not claim to have replayed
protected originals. No private Brain or original response bodies were fetched.

## Validation

- Engine suite: 65 tests, including six full-output baseline/final comparisons
  across native/legacy/missing Radar and valid/missing authoritative risk input.
  The baseline handler's SHA-256 is pinned; only the new evidence key may differ.
- Frontend: 2,157 Node tests passed, including six new evidence regressions.
- Deployment static/shell suite on updated main: 924 static and 15 shell tests passed.
- Shared provider research/acceptance: 29 tests and five subtests passed.
- Source/config validation, preflight, page-script syntax and engine wiring pass.
- Public-boundary fixtures pass without private data access.
- Browser preview at 1440 and 390: 1/5/21 selection, search, partial basket labels,
  stale funds, leveraged labels, keyboard focus, expiry suppression and old-packet
  compatibility passed; existing decision DOM unchanged; no page errors/overflow.
- Browser preview uses local page source, a captured real Radar projection and
  synthetic existing decision inputs. All external requests were intercepted.
  This is preview validation, not deployed/live Khalid acceptance.

## Cost and boundedness

Existing hourly :25 UTC cadence, 1024 MB / 180-second configuration unchanged.
No added S3 GET or PUT operation and no paid model/provider request. The added
projection for this capture is 310,597 compact JSON bytes (28,380 gzip bytes),
versus roughly 3.94 MB in the input. Raw histories/source rows are not copied.
At 24 runs/day this is about 7.45 MB/day of added overwritten output bytes, plus
client transfer when fetched. Local median projection time was 23.4 ms over ten
runs; this is not an AWS billing benchmark. Max 500 funds / 100 baskets accepted.

## Review scope

Claim: S-shopiz#kpf0930r8, branch agent/shopiz/khalid-provider-flow-evidence.
Current main was merged normally into the draft branch; no force-push. No merge
or production deployment is authorized here. Independent review is pending.
The review should focus on fail-closed semantics, expiry, identity, partial groups,
and the full-output parity proof. No unresolved investment criterion is decided.


## PR13 P2 review repair

Four mutation regressions each failed against reviewed head 925e66717 before
repair and now pass. Same-date latest observations must numerically equal the
one-observation aligned flow (equivalent Decimal formats and measured zero remain
valid). Cross-ticker reuse of a validated ticker-specific history key/hash holds
both funds; ordinary overlapping basket membership remains valid. Basket sums
use canonical sorted ticker order and local Decimal precision 50, including a
36-significant-digit regression. Missing/unknown classifications hold basket
aggregation with an explicit reason; inverse and leveraged classifications remain
separate. These are constructed test mutations, not claims of live corruption.

All 65 engine tests include the original six full-output decision parity cases.
The captured real projection remains 289 available funds / 300 configured funds,
46 baskets and 310,597 compact bytes. No handler, frontend, registry, schema,
schedule, cloud I/O or decision logic changed in this repair. The 1440/390 local
browser preview was rerun with the repaired projection; both passed, including
old-packet compatibility, expiry suppression and unchanged decision DOM.
Re-review is required before any merge or deployment.

# Stage 512 — FINRA contract repair, candidate contract

Read-only primary documentation on 2026-10-01; no FINRA query, metadata API, actual packet, account material, provider probe or producer invocation.

Primary references:

- https://developer.finra.org/docs — `corporateMarketBreadth`, `tradeReportDate`, `productCategory`, `advances`, `declines`, `unchanged`, `totalTrades`, `totalVolume`, 52-week high/low. The initial helper names did not match this contract. Incoming peer work corrected the dataset/date aliases before adoption; those names and compatibility constants are retained. Its aggregation still added all securities to its constituent categories, which this candidate repairs. Preserve category rows; all securities is an aggregate, not a fifth constituent to add to its subsets. Treasury `benchmark` is an on/off-the-run label, not a reference price. `trace` is not a documented dataset in this group; the separate TRACE API is outside this platform.
- https://www.finra.org/finra-data/browse-catalog/about-treasury/daily-data — daily aggregates cover prior-day transactions, published at 20:00 ET on business days. Treasury categories include Bills, FRNs, Nominal Coupons and TIPS, maturity and on/off-the-run classifications. VWAP is available for on-the-run nominal coupons. The existing 21:00 UTC producer cannot assume today's observations exist. Keep schedule unchanged; select only returned, valid observation dates in a bounded recent window.
- https://www.finra.org/media-center/reports-studies/2024-industry-snapshot/market-data — dealer-to-customer trading includes buys and sells. Its fraction of reported volume is not dealer inventory, positioning, directional buying or fund flow.
- https://www.finra.org/sites/default/files/3%20MPPFile-DownloadsCASpecsv4.5.pdf — historical corporate breadth file definitions describe issues and par value in millions. This is an older file contract, not sufficient by itself to assert the unit of today's API field; retain source-native volume with an explicit unverified unit until a current matching definition is obtained.

No vendor-parity rating, print coverage, stress threshold, equity forecast, sizing authority or ordinary live-source qualification is established by these documents or invented fixtures.

Preliminary prototypes `stage512-finra-candidate.py`, `stage512-bond-candidate.py` and their fourteen tests address only mixed dates and the exactly-30-row exception. Their invented old-schema/numeric-benchmark fixtures DO NOT establish FINRA correctness. Do not ship those prototypes as the completed repair.

Planned atomic implementation:

1. Documented dataset/field contract, strict calendar dates, non-finite/null distinction, category identity/duplicate checks, retained complete source rows, explicit bounded-query saturation failure, independently dated Treasury and corporate envelopes.
2. No invented per-print endpoint or calculated market total from a capped sample; keep compatibility function/fields unavailable with a reason.
3. Label dealer-customer share literally, retain benchmark labels, unavailable dislocation/positioning rather than a fabricated zero. Preserve existing field names as deprecated nullable fields with explanations and canonical additions.
4. History only compares observations from the same definition and earlier dates; no same-day run inflation or mixing revised definitions. No migration based on reading actual legacy/history packets during development.
5. Keep normal schedule and existing acquisition inputs; fix exactly-30-row boundary. Explicitly withhold Calls/sizing authority and separate legacy ETF heuristic from observed FINRA aggregates. Broader proxy date alignment and calibration remain a separate qualification task.
6. Whole-producer invented fixtures, HTTP/auth/SSM/S3/messages mocked. Test documented category rows, missing/duplicate/future/mixed dates, limits, empty data, benchmark labels, repeat/revised dates, history contamination, 30-row/31-row boundaries, and preserved unrelated proxy behavior. Inspect public consumer source and retain null-safe compatibility.

Candidate adopted after the chart repairs and a read-only native control baseline. Full integrated regression, exact release and normal source qualification remain pending.


# Findings to preserve beyond the bounded FINRA batch

- `intelligence/index.html` general `ago` formatter permits negative ages on future timestamps and uses generation times. The Bond TRACE panel now separates source dates and does not claim freshness. The general multi-pane timestamp formatter remains a separate task; browser fixture clock is frozen consistently with its invented 21:00 packet, rather than inventing a current publication.
- `justhodl-ai-website-synthesis` retains a direct Anthropic function plus `anthropic_shim`, which may issue a paid call when its external cost policy admits it. The shim catches policy exceptions without always blocking the underlying direct request. The fixed-income batch only withholds unqualified bond context; it does not invoke a producer or model, read policy/account state, or establish that ordinary synthesis calls are free. A subsequent deterministic/no-paid synthesis repair is required before this engine can be considered compliant and qualified.
- Main Bond TRACE ETF/OAS proxy still needs timestamp alignment, strict provider numerics, truthful horizons (90 label is 89 intervals; OAS 30 label uses 29), missing-input coverage, vintage/freshness, calibrated thresholds and actual-source capture/replay. Calls/sizing eligibility is explicitly withheld. The FINRA adapter and source contract do not validate that heuristic.
- A current exact definition for FINRA API volume units is still needed. The candidate retains source-native values with units explicitly unverified; no invented USD totals or institutional positioning are published.
- Native normal execution, original archives, out-of-sample validation and portfolio reconciliation remain open. No parity rating follows from code/tests/receipts.

# Market-history repair — pending deployment acceptance

This batch repairs the shared route configuration dependency without treating a route as evidence of instrument equivalence or complete history. The existing checked-in resolver is packaged with the function, with an identical config twin, duplicate-key/schema validation, and a hash in returned routing evidence. The unavailable public resolver object is not retried. A missing runtime symbol map still stops fallback. Existing licensed-series and intraday holds remain.

The TradingView bank service stops host/provider and remaining-batch fallback after explicit access refusals. The WebSocket upgrade validates the RFC 6455 accept value, retains bytes arriving after the headers, and does not negotiate unsupported compression. Missing Yahoo high/low values are not manufactured from close; absent volume stays null. Access-denied or corrupt existing banks and indexes cannot be treated as empty replacements.

New market-bank documents retain producer labels for each bar. Legacy rows remain explicitly unattributed, mixed history cannot carry only the newest provider label, and corrections are counted. These are parsed-row custody labels, not original upstream receipts. Full history, vendor equivalence, adjustments and point-in-time reconstruction remain unverified. Existing timestamps may receive corrections; this is not an immutable vintage archive.

The two original COT3 VIX aliases retain code 11700. A separate explicit choice opens the current official CFTC code 1170E1, keeping the original requested identifier and the historical-equivalence warning. Both canonical CFTC histories were independently replayed from their public original-response receipts (1,019 observations each); that does not establish continuity with TradingView's old alias. CFTC watchlist rows are positioning data, and ECONOMICS rows are macro data.

Read-only operation 6477 confirmed the deployed TradingView source and controls: the nightly universe-refresh Scheduler is enabled; the older hourly rule is disabled. Operation 6478 checks exact source bytes, exact commit receipts, and all original controls for both changed functions. No producer is manually invoked and no credential, account, private dataset, schedule or IAM change is part of acceptance.

Route totals remain 6,729/10,745, with 4,016 still unrouted. The explicit VIX alternatives do not increase the count of routed original identifiers. Native release, whole served chart/assets and isolated desktop/mobile acceptance are still required before this batch can be called live.

Protocol reference: https://www.rfc-editor.org/rfc/rfc6455
Source review: chart-vix-source-review.json; chart-vix-current-code-history-audit.json; chart-tv-bars-baseline.json.

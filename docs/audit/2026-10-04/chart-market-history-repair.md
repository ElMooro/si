# Market-history repair — deployed and checked

This batch repairs the shared route configuration dependency without treating a route as evidence of instrument equivalence or complete history. The existing checked-in resolver is packaged with the function, with an identical config twin, duplicate-key/schema validation, and a hash in returned routing evidence. The unavailable public resolver object is not retried. A missing runtime symbol map still stops fallback. Existing licensed-series and intraday holds remain.

The TradingView bank service stops host/provider and remaining-batch fallback after explicit access refusals. The WebSocket upgrade validates the RFC 6455 accept value, retains bytes arriving after the headers, and does not negotiate unsupported compression. Missing Yahoo high/low values are not manufactured from close; absent volume stays null. Access-denied or corrupt existing banks and indexes cannot be treated as empty replacements.

New market-bank documents retain producer labels for each bar. Legacy rows remain explicitly unattributed, mixed history cannot carry only the newest provider label, and corrections are counted. These are parsed-row custody labels, not original upstream receipts. Full history, vendor equivalence, adjustments and point-in-time reconstruction remain unverified. Existing timestamps may receive corrections; this is not an immutable vintage archive.

The two original COT3 VIX aliases retain code 11700. A separate explicit choice opens the current official CFTC code 1170E1, keeping the original requested identifier and the historical-equivalence warning. Both canonical CFTC histories were independently replayed from their public original-response receipts (1,019 observations each); that does not establish continuity with TradingView's old alias. CFTC watchlist rows are positioning data, and ECONOMICS rows are macro data.

Read-only operation 6477 confirmed the deployed TradingView source and controls: the nightly universe-refresh Scheduler is enabled; the older hourly rule is disabled. Operation 6478 checks exact source bytes, exact commit receipts, and all original controls for both changed functions. No producer is manually invoked and no credential, account, private dataset, schedule or IAM change is part of acceptance.

Route totals remain 6,729/10,745, with 4,016 still unrouted. The explicit VIX alternatives do not increase the count of routed original identifiers. Native source receipts, both complete deployed source/control inspections, the complete served chart and 49 local assets, and six isolated browser cases at desktop/mobile widths have passed. The read-only acceptance run is 37238673953; both native receipts name source commit 86901cfc8363b5b094ec6bc05712a58d8ea09587.

Protocol reference: https://www.rfc-editor.org/rfc/rfc6455
Source review: chart-vix-source-review.json; chart-vix-current-code-history-audit.json; chart-tv-bars-baseline.json.

## Accepted release and remaining access gap

Source commit: 86901cfc8363b5b094ec6bc05712a58d8ea09587. Validation: 4,429 frontend tests, 149 symdir tests, 13 market-bank tests, seven acceptance-operation tests, 1,075 deployment checks and 15 shell checks. Page syntax (600 public graphs), contracts, wiring and the 50-case accounting browser regression also passed. Six browser scenarios passed at 1,440 and 390 pixels using the actual served modules; the CFTC scenario replays the two real empty original responses and two real 1,019-row alternatives. Both screenshots were visually inspected.

The public FTSE:FTUSPLUT request no longer fails at missing resolver configuration. It reaches TradingView, which returns HTTP 400 at the WebSocket upgrade. The application returns an explicit provider-denied failure; no alternative host/provider was attempted. Further TradingView requests are stopped pending a legitimate source/access change. This is a verified configuration repair and an unresolved provider-access gap, not verified FTSE historical coverage. The packet and hash are recorded in chart-market-provider-access.json.

No routes are counted for speculative substitutes. All 4,016 unrouted identifiers remain in the remaining-items audit. Separate BIS FX research remains outside this accepted release.

# Exact CFTC chart definitions

All 346 COT/COT2/COT3 identifiers in the audited public watchlist now resolve to a
reviewed CFTC report, original contract code and official column. Six official
schemas supply the definitions. Leading zeros, plus signs, report families,
futures-only versus futures-and-options, trader counts versus contracts, and
All/Old/Other scope are retained. Legacy TradingView LT concentration labels map
to the CFTC schema's LE columns using the published metric definition. They are
not arbitrary numeric substitutions.

The scalar adapter paginates bounded requests against the selected official
report only. It retains complete response bytes and every source row. Duplicate
dates, malformed identities, suppressed values, incomplete pagination and denied
responses cannot produce a successful full history. Missing values remain null;
no missing weeks are filled. Combined contract units are futures equivalents.
Observation dates are never presented as release timestamps or point-in-time
vintages. Cache reuse requires the exact catalogue digest and definition; legacy
COT alias caches are excluded.

The actual chart handler, watchlist, visible symbol search and provider picker
preserve the canonical identifier. Native Git sends the 100 KB definition file,
its byte-identical bundled twin, Lambda helper and all consumers atomically.
The old complete sources and every reviewed edit remain in the preservation
fixtures. Incoming formula changes on main remain intact and unqualified by this
work.

Only public catalogue, directory and schema metadata was inspected. No actual
CFTC measurement history or application packet was read for this release. Routing
coverage does not establish contract existence, licensing, economic equivalence,
complete history, a current print, or eligibility for Calls or sizing. Those
remain explicit boundaries in the adapter output.

Validation uses invented observations, exact identity and failure cases, actual
native integration, and fully intercepted desktop/mobile browser execution.
Post-release acceptance must bind the native package, resource controls and
public receipt to the intended commit, plus the whole served chart source and
CFTC metadata directory. No producer invocation or schedule change is required.

Primary definitions: [CFTC explanatory notes](https://www.cftc.gov/MarketReports/CommitmentsofTraders/ExplanatoryNotes/index.htm),
[TradingView COT definitions](https://www.tradingview.com/blog/en/commitments-of-traders-reports-on-tradingview-29284/),
[TradingView Legacy metrics](https://www.tradingview.com/script/195p3YlK-Commitment-of-Traders-Legacy-Metrics/).
Official schema URLs, times and whole-body hashes are in the companion JSON.

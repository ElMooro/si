# Chart/provider access — first complete routing inventory

The user assigned complete watchlist and provider access to chart.html on October 4.
The authoritative public watchlist capture contains 492 lists, 25,846 member
occurrences and 10,745 distinct requested identifiers. Executing the current
route functions against every member leaves 4,569 without a handoff. A handoff
is not evidence that the requested instrument, historical values, venue, units,
vintage or publication availability are correct. No actual browser storage was read.

All 73 live provider catalogue entries were checked: 70 provider manifests were
received and three derived provider manifests returned 403. All 1,588 declared continuation pages for the readable manifests were received,
containing 788,041 object metadata entries. Continuations lack their own generation
ID, so this is not an atomic catalogue snapshot. Catalogue records are metadata,
not original measurements or a certification of complete history. Metadata
receipts and namespace counts are retained in chart-provider-coverage.json.

The source-reviewed /symsearch endpoint is on the declared data-proxy host.
Initial requests to the site host returned 404 and are retained as a failed
discovery step; no access-denied endpoint was retried via a different route.
Five proper directory requests succeeded. CFTC contract132741 and the DBnomics
FIGB_PA query had no indexed matches; that is an index result, not proof the
upstream providers have no data. ECB CISS and World Bank inflation appear as
datasets but the chart lacks a native dataset-to-series drilldown.

## Access corrections

The public runtime key data/tv-symbol-resolver.json returned403 even though its
complete reviewed JSON is tracked. The quote renderer required that key before
even supported bare-equity requests could run. The identical rule object now
ships in the existing quote module. A fresh copy is returned per initialization;
tests require semantic equality to the entire source JSON. Alias, licensing,
qualified-venue and identity checks remain intact. This does not qualify every
watchlist quote; it removes a broken dependency from already-supported requests.

The chart's URL-only fetch wrapper merged distinct header contexts and abort
signals and returned a cached403 even to no-store Retry. Explicit options and
Request objects now preserve native transport behavior. Bare string calls without
options retain their existing in-flight coalescing and45-second denial backoff.
Request-bearing callers may make separate requests; there is no new polling.

## Next implementation order and acceptance

1. Complete provider capability and dataset/series discovery, including pagination
   and explicit scan bounds; open exact series from chart.html without losing the
   requested ID. Keep non-scalar records inspectable rather than invent candles.
2. Add exact missing provider adapters, starting with CFTC report-family/contract/
   category/position identities. Search all available provider catalogues for each
   unresolved family; record exact matches, possible substitutes and unavailable
   entitlements separately. Do not turn low-confidence symbol-map guesses into truth.
3. Implement a constrained expression parser and aligned calculation provenance
   for watchlist ratios/spreads. Retain raw series, frequency, units, revisions and
   missing values. No eval and no fill-forward that invents observations.
4. Validate returned identity, historical completeness, observation clocks and
   chart behavior for all10,745 requested IDs. Fix storage and source gaps through
   the normal runner lane, preserving IAM and existing schedules.
5. Verify exact source/build receipts, served HTML/JS and controlled browser
   behavior; report provider/entitlement gaps explicitly. No blanket parity claim.

Reference principles were checked against primary documentation: [Bloomberg
reference data](https://professional.bloomberg.com/products/data/enterprise-catalog/reference/),
[ECB full series keys](https://data.ecb.europa.eu/help/api/data),
[FRED observation/vintage parameters](https://fred.stlouisfed.org/docs/api/fred/series_observations.html),
[World Bank metadata](https://datahelpdesk.worldbank.org/knowledgebase/articles/1886695-metadata-api-queries),
and [CFTC report classifications](https://publicreporting.cftc.gov/stories/s/r4w3-av2u).
These establish definitions and discovery methods, not JustHodl runtime coverage.

Prepared buyback repairs remain outside the checkout while chart/provider access
is prioritized. The broader institutional objective and all ten workstreams remain open.

Regenerating the page contract exposed pre-existing main-branch drift for seven
pages. Compiling against the exact original chart sources reproduces the same
registry byte-semantically; the access edits cause none of that drift. The generated
registry is reconciled with current source without changing the ownership compiler.
The deployment suite also exposed a Windows fixture issue: an unquoted backslash
path was inserted into a Bash script. The fixture now quotes the absolute
forward-slash path; production retention is unchanged.

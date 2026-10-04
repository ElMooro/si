# Watchlist economic-source qualifications

Base: `8abeed0ee1b2ca8039c7cff15807cd4c6b08730d`. Claim:
`S-shopiz#econ1004e7`. This additive check preserves all 318 providerRest routes,
all 461 extraChart routes, requested/resolved IDs and native chart behavior.
The original routing qualification did not certify economic equivalence.

The status above the chart and the watchlist card now share source-linked
qualifications for 36 exact requested/resolved pairs. Twenty-four have concrete
definition, frequency, currency or historical-continuity qualifications: four
core-inflation measures; DECA/JPCA/GBCA; eleven MYAGM money histories;
US/FR/IT manufacturing YoY; US industrial/retail YoY; and Germany GDP YoY.
Twelve other current-account pairs explicitly retain unverified definition and
continuity. Qualifications apply only while both IDs match the checked pair;
source changes cannot inherit another target's warning. Direct canonical requests
retain their existing routing and generic provenance qualification.

Definition/mapping qualification checked **2026-10-04**. This is separate from
the mapped source's latest observation period, which is **null/unverified** for
each target in this check. It is not a source refresh time or a vintage date.
Warehouse freshness and runtime source availability remain unknown; historical
release/vintage availability remains unverified. Displayed chart bars do not
certify these facts.

The supplied independent review identified fourteen discontinued BPBLTT targets
with source-page latest periods in 2011–2016, and eleven MYAGM histories ending
in 1998–2019. Current main has **fifteen** BPBLTT targets. Those group ranges do
not identify per-target dates or the precise fourteen-target subset, so they are
not assigned as individual observation dates or discontinuation flags. Only
DECA's pre-denial directly read FRED page establishes its discontinued flag.
JPCA/GBCA use the supplied individual frequency/currency findings and retain
unknown individual continuity. The other twelve receive explicit unknowns.

Primary requested definitions confirm the specific differences:

- [Japan Statistics Bureau](https://www.stat.go.jp/english/data/cpi/1585.htm):
  fresh-food-only core differs from food-and-energy exclusion.
- [Statistics Canada CPIX methodology](https://www.statcan.gc.ca/en/statistical-programs/document/2301_D64_T9_V1-eng.pdf):
  eight volatile components and indirect-tax effects differ from all food/energy.
- [Statistics Norway CPI-ATE](https://www.ssb.no/en/priser-og-prisindekser/konsumpriser/statistikk/konsumprisindeksen):
  tax adjustment and energy exclusion differ from all food/energy exclusion.
- [Central Bank of Turkey C index](https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB%2BEN/Main%2BMenu/Announcements/Press%2BReleases/2026/ANO2026-32):
  the requested exclusion basket also includes alcohol, tobacco and gold.
- [World Bank GDP metadata](https://databank.worldbank.org/metadataglossary/world-development-indicators/series/NY.GDP.MKTP.KD.ZG):
  annual growth differs from [requested quarterly YoY](https://www.tradingview.com/symbols/ECONOMICS-DEGDPYY/).

FRED metadata comes from the supplied independent review and pre-denial reads of
Japan core and Germany current account. A direct MYAGM1DEM189S read encountered
proxy tunnel **HTTP403**; that path was stopped, with no retry or alternate
access. FRED observation periods and metadata checked times were not inferred.
The per-pair fixture retains source URLs, exact mappings and evidence limits.

No fetch, endpoint, provider, polling or cadence is added. Request/cost impact:
zero additional automatic requests or recurring service cost. Source links open
only on user action. Synthetic browser fixtures intercept all requests and
preserve invented members/order/favorites/flags. No actual user storage was read
or mutated. Store schema2, compatible adapter and recovery originals remain
required; renderer-only legacy-authority rollback remains unsafe.

Only watchlist qualification display, its source-transition receipt, synthetic
tests/fixture and this audit are in scope. The previously published claim
8e7d1ee78/PR99 is retained; the shared claims file is untouched under PR98
coordination. The earlier c1863abe draft remains historical and unmerged.
The owner's 92 new routes and 150 existing transition rows are preserved. Chart engine,
volume, catalog, quotes, rail/store, observations/cache, HTML, other claim rows,
workflows, AWS, schedules and money-engine behavior are unchanged. Exact-head
independent review and normal Pages release receipts are recorded externally.
Production edge/nav/click qualification remains **HOLD** under the retained
JustHodl HTTP403 STOP. No TradingView parity, live freshness, actual lost-list
recovery or complete-system-green claim is made.

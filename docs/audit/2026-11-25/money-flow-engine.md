# The money-flow engine: found, and audited (2026-11-25)

Khalid, 2026-11-25 20:40 CT: "I know i have a flow engine that show where the money is flowing in and flowing out of.
Find me that engine first and lets audit it".

Read-only. Nothing was invoked, changed or written. Ops 6961 (can be run again) did the reading on the runner and in
a real browser; its report is `aws/ops/reports/latest/ops_6961_money_flow_engine_audit.md`.

## The engine

`justhodl-global-flow-desk`. Its own first line is "WHERE THE MONEY IS GOING (Khalid's mandate)". Built 2026-07-02
as the roof over the flow fleet: dollars by asset class, the eleven sectors ranked, institutions against retail, hot
money by country, a written brief.

| | |
|---|---|
| page | `justhodl.ai/global-flow-desk.html` (in the side menu and linked from six pages; no button on chart.html) |
| document | `data/global-flow-desk.json`, 464 KB |
| schedule | weekdays 23:37 UTC, EventBridge rule `justhodl-global-flow-desk-daily`, enabled |
| health | 15 runs in 21 days, 0 errors, about 53 s each, no failure in its log; not held |
| reads | `data/etf-true-flows.json` (the ETF creations engine, daily 15:45 UTC) and `data/tic-flows.json` |

There are 30-odd other flow engines and 16 flow pages; all 16 pages open without an error. This is the one built to
answer the question in one place. The chart's "Flows" button opens a different page (Inflows & outflows, 326 series).

## What the audit found

**1. It no longer says where the money is going.** Since about 2026-09-19 the handler runs a second compiler
(`flow_desk_research.py`, document 2.0.0). The first one is still in the file as `_legacy_unvalidated_handler` and is
never called. The four readings are published empty: `sectors.leaders []`, `sectors.laggards []`, institutions against
retail `None` ("investor_identity_not_measured"), hot money 0 countries ("national_hot_money_not_measured"), and the
brief is one fixed sentence about coverage. The page is titled "Compare the flows. Understand the exposure.", opens on
four counts (128 / 98 / 96 / 49) and a drop-down, and the word "inflow" appears once on it. The first engine's last
publication is 2026-09-18 22:50 UTC (57 days of history, 2026-07-02 to 2026-09-18, kept).

**2. The figures it still computes are right, to the cent.** All 96 funds' five-day figures were recomputed from each
fund's own stored history: 96 equal, 0 different. All 64 group totals equal the sum of their members. On SPY, IVV, TLT,
GLD and XLK, (shares minus shares the day before) times that day's NAV equals the stored flow on each of the last 40
rows. The Inflows & outflows desk's per-fund feeds show the same figure for all 96.

**3. It sees 96 of 128 funds, and the missing ones change the answer.** The 96 hold $4.07 trillion. Missing: every
Vanguard fund (17, VOO alone is $1.03 trillion), Schwab, ARK, Fidelity and Grayscale bitcoin, the six Global X funds,
VXX; QID and BITO for this week (a corporate-action day on 2026-11-19 is held for review, correctly). Three baskets
name funds the ETF engine does not follow at all: IAU, IYR, VNQ. Example: the US equity basket reads -$440 million on
6 of 8 funds; the two it lacks, VOO and VTI, took in $12.8 billion and $2.2 billion on the provider's figures.

**4. The site already holds a wider source it does not use.** `data/capital-flow-radar.json` (the provider's
creation and redemption flows, daily 22:20 UTC): 300 funds, 285 with a five-day figure, $6.78 trillion, including 13
of the 32 the engine lacks. It is kept beside the figures as "context" and never read into one. It needs guarding
before use: ROM shows +$1.88 billion on about $1 billion of assets and ARKK -$5.04 billion on about $6 billion, which
look like share-split or data errors (the issuer-based engine catches such days; the provider document does not).

**5. The two sources are one trading day apart, and the engine's own funds are on two different days.** The
provider's share count on a day equals the issuer's file on the NEXT reporting day for 51 funds (SPY 29 of 29 days,
IVV 29 of 29, TLT 30 of 30): the issuer's file shows a creation the day after the order. For six funds (GLD, SLV,
USO, UNG, CIBR, HACK) the issuer's date is the order's own day. So a window "to 2026-11-24" holds orders through
2026-11-23 for most funds and through 2026-11-24 for those six, and baskets add the two together. Same label, the
week to 2026-11-24: SPY +$643 million here, -$6.06 billion at the provider. Aligned by one day the two agree within 2%
for 32 of 71 funds (15 of 71 unaligned). For twelve ProShares leveraged funds the share counts never match on either
day; not resolved here.

**6. The first engine should not be switched back on as it was.** Its last two days said "institutions -70.5,
retail -0.6: RETAIL BUYING / INSTITUTIONS SELLING". That was a fault: one of its two institutional inputs (the share of
ETF groups in inflow, read from the radar) had turned to 0.0% when the radar's format changed on 2026-09-17, which
alone moved the score from -11.3 to -71.2 overnight. Its other institutional input, `data/dark-pool.json`, has not
been written since 2026-10-16 (40 days). And "institutions" and "retail" were proxies (a dark-pool index and ETF
breadth against the AAII survey and stablecoin supply), not a record of who bought.

**7. Smaller.** The window is five fund-reporting days, fixed; no 1-day, 1-month or year-to-date view is published
though the histories run back to 2000 (median 5,127 days). TIC beside it is the 2026-09-01 observation (monthly, as
published). Of the first engine's twelve other sources nine are written daily; `data/aaii.json` and
`data/structural-presignals.json` are older spellings that no longer exist (their replacements do).

## What the figures say (issuers' own files, five reporting days to 2026-11-24)

Each fund counted once, 96 funds: in +$15.95 billion, out -$14.95 billion, net +$1.0 billion.

- In: Treasury funds +$5.97B (8 of 12 funds), T-bill funds +$3.51B, growth +$515M (1 of 4), international equity
  +$340M (7 of 9).
- Out: commodities -$2.60B, bitcoin and ether funds -$2.24B (2 of 5), gold -$2.13B (2 of 3), mixed bonds -$1.15B,
  sector funds -$538M, US equity -$440M (6 of 8; see finding 3).
- Sectors: XLF +$901M, XLK +$453M, XLP +$294M, XLC +$215M; XLE -$1.18B, XLV -$848M, XLY -$297M, XLI -$273M.
- Countries: India +$359M, Canada +$157M; Mexico -$58M, Singapore -$43M, China -$39M.
- Single funds in: SGOV +$2.20B, BIL +$1.62B, IEF +$1.60B, IWM +$1.29B, TQQQ +$1.23B, TLT +$1.12B.
  Out: GLD -$2.52B, IBIT -$1.93B, RSP -$1.52B, XLE -$1.18B, QQQ -$1.10B, XLV -$848M.

## Not decided here (Khalid's call)

1. Put the answer back on the page: a ranked money-in / money-out board from the figures already computed, beside the
   research view (nothing removed).
2. Fill the 32 missing funds from the provider's flows, marked as the provider's, with a guard against split days;
   add IAU, IYR, VNQ to the ETF engine.
3. Put both sources on the order's own day and print which day the orders are from.
4. Institutions against retail and hot money: leave off, or bring back as clearly named proxies. The dark-pool
   engine's 40-day silence is a separate repair.
5. Longer windows (1 day, 1 month, year to date) and a button on chart.html.

---

# The enhancement (2026-11-25 / 26): the money map

Khalid, 21:22 CT: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and
covers as many asset classes and etfs as possible. And add a button on charts.html for it call it flow. Check if that
button doesnt exist first before you add it".

## What there is now

- **Engine `justhodl-global-flow-desk`** publishes a second document beside its research view (which is untouched, to the
  byte): the money map, `flow_map.py` 3.0.0. Every ETF the warehouse holds: 5,878 funds with rows, **5,770 current,
  $15.57 trillion** (the engine saw 96 funds and $4.07 trillion before). Six windows (1D, 1W, 1M, 3M, YTD, 1Y), each
  with net, money in, money out, funds in and out, percentage of assets and the window before.
  - 23 asset classes, one per fund, adding up to the total; the provider's own classes, categories and focus groups;
    sectors two ways (sector funds, and every equity fund's flow split by the weights it holds); 41 countries; regions;
    styles; bonds by kind and by maturity; 21 commodities; crypto; leveraged and inverse kept apart; 60 issuers.
  - Boards of the 40 funds with the most in and out, by dollars and against their own assets, in every window; funds
    that turned; 260 days of every class and sector a day at a time; each fund's own 130 days.
  - Documents: `data/global-flow-map.json`, `data/global-flow-map-funds.json` (34 columns a fund),
    `data/global-flow-map-history.json`, `data/global-flow-map/funds/<letter>.json`.
- **The warehouse** has every fund on the newest day: the Polygon bank's new by-date dataset
  (`etf-global-fund-flows-by-date`, one spec line; ops 6963). Before it, 1,628 of 5,789 fund files were current.
- **The page** `global-flow-desk.html`: the money map is the first view (`jh-money-map.js`); the research view that was
  the page is beneath it, unchanged, one click away (Research view, `#research`, or any `?run=` link).
- **The chart** has a **Flow** button beside Flows. Flows was not renamed or moved: it opens the Inflows & outflows
  desk as before. Nothing on the bar opened this engine's page until now.

## What a flow is, and why not the provider's own figure

The first dry run agreed with the provider's `fund_flow` to the dollar on 5,187 of 5,187 funds, and that was the fault.
Measured read-only on the whole warehouse (ops 6965):

| What the provider printed | Size | What the engine does |
|---|---|---|
| A share split as an inflow or outflow | 299 in 300 days, **$1.75 trillion**; Vanguard's five April 2026 splits alone +$401 billion (the NAV restated a day before the count) | Takes the split from the warehouse's own record of splits (258 of the 299), or reads it from a whole-ratio move of count and NAV; counts and NAVs before it are restated |
| A figure after a missing day | 502 of 1,123 such rows wrong, $14.3 billion in 63 days; a November buffer series printed -85% with its share count unchanged | Uses the share count's own path, which on consecutive rows is the provider's figure on 351,572 of 351,576 rows |
| A row of another fund | 10; Fidelity Solana printed +$2.02 billion then -$12.72 billion | Drops the row |
| A count that jumps and comes back within ten rows | 595 in 63 days, $36.9 billion each way: reset days, heartbeat trades, and the provider's 11 to 19 November fault that showed ARK Innovation at 271 million shares against its usual 80 | Counts what the trip left behind |

A jump that stays is money and is counted, however large. Alger Concentrated Equity (CNEQ) is the test: its count rose
from 30 to 203 million shares from August to October and is 26 million now. Outside sources agree it held about $7
billion in late October and under $1 billion now, so it is counted, not removed.

Nothing is estimated, carried forward or inferred. Who bought or sold (institutions, retail, foreigners) is not in this
data and is not shown.

## Limits, said plainly

1. **One trading day behind.** The provider posts a day's orders during the next day. The page prints the day.
2. **The engine runs Monday to Friday at 23:37 UTC**, so Friday's orders appear on Monday night. Not changed.
3. **108 funds ($115 billion) are left out as late** and listed; nearly all of it is Dimensional (DFAC, DFAS, DFAT, DUHP
   ...), whose rows stop in late September or early October in the provider's data.
4. **ARK and Alger funds, 11 to 24 November:** the provider's counts for these two issuers swelled and fell back. The
   round-trip rule nets ARK's; Alger's three funds also really lost most of their assets in the same weeks, and the two
   cannot be told apart day by day. Their rows carry a flag; their month figures are right, their week figures uncertain.
5. **Quarter-end weeks** (the third Friday of March, June, September, December) show very large flows into and out of
   the S&P 500, Nasdaq and sector funds within days. They are real share creations and are counted; they are under the
   40% a day that the round-trip rule looks for.
6. 14 recorded splits never show in the provider's counts (mostly leveraged notes); nothing is changed for them.
7. ETF flows are not mutual-fund flows or direct stock purchases.

## Decisions taken for you

- **Flows / Flow.** You asked for a button called flow and to check first. "Flows" exists and is a different desk, so I
  added "Flow" next to it and left Flows alone. If you would rather have one button, or another name for either, say so.
- **The money map is the first thing on the page**, the research view second. One click swaps them.
- **Memory 3008 MB and timeout 900 s** for the engine (1024 MB / 300 s before): the map reads 5,900 files. About 13
  cents a month.
- **Leveraged and inverse funds are counted apart**, so a bet against the market is never netted against a purchase.
- **China** counts mainland, Hong Kong and offshore (Cayman, Bermuda) registrations together when the fund is an
  Asia-Pacific fund; otherwise KWEB, FXI and MCHI would be in no country.

## Not done (your call)

- A Saturday run so Friday's orders show on Saturday.
- Institutions against retail, and hot money: still off (this data cannot say who). The dark-pool engine's silence
  since 2026-10-16 is a separate repair.
- The research view's own gaps from the audit above (32 missing funds, two datings) are as they were.

## To take it back

- The button: `docs/snapshots/chart-search-2026-11-10/README.md`, "2026-11-26: the chart's Flow button".
- The page: `git checkout fad9f69d -- global-flow-desk.html`, delete `jh-money-map.js`, `tests/money-map-page.test.js`,
  `tests/fixtures/money-map-135.json.gz`.
- The engine: `git checkout cc64470a -- aws/lambdas/justhodl-global-flow-desk/`, delete `source/flow_map.py`,
  `tests/test_flow_map.py`, `tests/fixtures/`; regenerate the manifest; push. The four documents can stay in the
  warehouse; nothing reads them but this page.
- The bank's by-date dataset: one line in `polygon-bank-spec.json`.

## Proof

- **Dry runs on the live warehouse, nothing written (ops 6964, four times).** The engine's own code on the runner: read
  in 43 to 53 s, built in 8 s, 876 MB. Checked by the op's own code, not the engine's: 4,997 funds with all six raw rows
  and nothing done to them equal the op's count path and the provider's figures; 328 funds with a row missing inside
  the week equal their count path; the site's 300-fund radar agrees on 260 of 260 comparable funds; every table adds
  up in all six windows; no fund lost more than it held; the issuers' own counts match for 80 of 92 funds with enough
  days (the 12 are ProShares leveraged funds, known from the audit).
- **Measurement (ops 6965, read-only):** the table above.
- **Published (ops 6966, 04:49 UTC):** the same build and gate, then the engine's own `publish()` from the runner: 29
  objects, read back. The Lambda was not invoked, so its once-a-day run was not spent. Its own first run with the map
  is 23:37 UTC on 26 November.
- **Tests:** engine 40 of 40 (21 new, on real rows: Vanguard's split, the November series, Fidelity Solana, ARK, Alger,
  T-Rex, BAIG); page 14 new, including that the funds behind every bar add up to the engine's row in every table and
  window (434 figures); deployment suite 1,257.
- **Live, in a real browser on justhodl.ai (ops 6967, 05:33 UTC on 26 November): 0 failed checks.**
  - The four documents as the site serves them, added up again by the op: 5,770 current funds; 23 asset classes in six
    windows, both totals, the top of both boards in every window and the last day of every class in the history: 0 wrong.
  - The page: tiles +$33.5B net, $84.5B in, $51.1B out for the week to 24 November, the document's figures to the digits
    printed; the bars are the document's rows in its order; the boards its boards (VOO +$12.8B, SPY -$6.06B); the table
    opens on the largest funds and sorts both ways; one month and percentage of assets switch as the document says;
    Technology's sheet lists its 55 funds, most in SOXX, turned round IYW; SPY's sheet shows its six windows and 128
    days of its own; the page's own adding-up: "the 5,770 current funds give every asset-class row and both totals in
    all six windows (150 figures)"; the research view opens in one click and loads its publication; no page error.
  - A phone (390 px): five tiles, 23 rows, no sideways scroll.
  - The chart: one Flow button between Flows and Heat; Flows as it was; a real click opens the money map in the chart's
    workspace; SPY's sheet sends SPY to the chart behind and closes the workspace; Flows still opens its own desk; on a
    phone a real tap opens it.

## What it says (week to Tuesday 24 November 2026)

- Net **+$33.5 billion** into 4,942 unleveraged ETFs: $84.5B into 1,451 funds, $51.1B out of 1,068. Month +$215B,
  year to date +$1.89 trillion.
- In: US equity +$19.3B (VOO +$12.8B, QQQ +$2.9B, VTI +$2.2B), Treasuries +$9.0B (SGOV, IEF, BIL), international
  developed equity +$5.3B, municipals +$2.4B.
- Out: crypto -$3.0B (IBIT -$1.9B), precious metals -$2.8B (GLD -$2.6B), broad commodities -$0.65B, energy commodities
  -$0.55B.
- Sector funds: technology +$3.45B, financials +$1.61B, staples +$0.88B; energy -$1.96B, health care -$1.95B,
  industrials -$1.22B. Counting broad funds by their weights: technology +$11.6B, financials +$5.7B; energy -$0.4B.
- Leveraged and inverse, apart: +$4.4B (SOXL +$3.2B).


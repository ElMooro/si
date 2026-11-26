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

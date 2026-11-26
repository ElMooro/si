# Twelve engines for twelve chart tabs: the audit, what was widened, what each page now shows

Khalid, 2026-11-26, on the twelve engines picked from the scan of the whole system: "Add these first and audit their
engine before you add them and see if you can expand it to cover more data and that all data from the engine is
displaying on the page".

Evidence: ops 6971 (each function as deployed, fourteen days of runs, every document the twelve pages read - every
field path, count and sample), ops 6972 (kept documents the engines could draw on, the warehouse's banks, the insider
purchases above $500 M), ops 6973 (dry run of the engine changes), ops 6974 onward (live proof). All read-only except
the go-live steps named below.

## What the audit found

| Engine (page) | Runs, 14 days | Found |
|---|---|---|
| `justhodl-equity-research` (`why.html`) | 520, 0 errors | On demand; 35,763 tickers cached (11.9 GB), 57 rewritten in the last 26 h; schema 2.9.3, 81 blocks, 1,341 fields for AAPL. The page names 636 of the 873 distinct field names. |
| `justhodl-insider-trades` (`insiders.html`) | 20, 6 errors (Nov 12-13 timeouts, repaired Nov 14) | EDGAR's "current filings" feed answers 400; the daily-index path carries the engine. **One misfiled line - 3,149,470 LONA shares "at $3,149.47" - was $9.92 B of the page's $10.69 B of insider buying.** The page named 106 of 358 field names and never loaded the cluster scanner's, the selling-cluster or the industry-cluster documents. |
| `justhodl-insider-cluster-scanner` | 15, 0 | Healthy: 2,281 purchases, 618 tickers, 388 clusters; already sets LONA aside. Not on any page the Insiders tab opened. |
| `justhodl-options-analytics` (`options-analytics.html`) | 970 (five-minute lane), 0 | 58 names, 3.7 s a run. The page never loaded the OPEX calendar or the IV history. |
| `justhodl-opex-calendar` | 974 (five-minute lane), 0 | SPY max pain only. |
| `justhodl-stock-valuations` (`valuations.html`) | 14, 0 | S&P 500 500/500, small/mid 400/400; the document lists 80 of the 400 small/mid names in its table and all 400 in its heat map. Page named 107 of 285 field names. |
| `justhodl-corp-bonds` (`corpbonds.html`) | 10, 0 | 8.9 MB document, full TRACE tape; page named 87 of 147 field names. |
| `justhodl-plumbing-aggregator` (`plumbing.html`) | 14, 0 | Header said "55 of 40 indicators with data": it counted the base list (40) against every scored row (58). |
| `justhodl-liquidity-agent` (`liquidity.html`) | 14, 0 | Healthy. Page named about half of each document's field names. |
| `justhodl-canary-warroom` (`canaries.html`) | 14, 0 | "12 mechanisms" - three of them (eurodollar, dollar, global stress) vote with no canaries because their source engines were rewritten in September to describe only (no percentile, no status). `justhodl-warroom-weights` last wrote on 2026-10-01 (monthly; nothing on 2026-11-01). |
| `justhodl-breadth-thrust` (`breadth-thrust.html`) | 14, 0 | Healthy; the page never loaded the breadth history (60 days of advance/decline ratios). |
| `justhodl-trend-reversal` (`trend-reversal.html`) | 61 (six chained batches a day), 0 | 642 names, fetched one by one. |
| `justhodl-catalyst-calendar` (`catalyst-calendar.html`) | 14, 0 | "125 events": 113 were earnings already reported. Ahead: 8 earnings dates, one FOMC meeting, one quad witching, one rebalance. Twelve contract awards fetched every run and all dropped for want of a date. The 2027 FOMC dates were a "typical pattern" placeholder. No chip for three of its own event types; past days labelled "+-35d". |
| `justhodl-onchain-ratios` (`btc-cycle.html`) | 14, 0 | Ethereum block empty every run (CoinMetrics refuses the request). The join with the kept CryptoQuant document asked for status "LIVE"; the document has said "EOD" since the plan changed, so nothing was joined. The page never loaded the CryptoQuant document (171 metrics). |

## What was widened

- **Catalyst calendar 2.0** (`catalyst_sources.py`, kept documents only): economic releases and central-bank events
  from the economic calendar, the wider earnings calendar sized by market value, IPOs, lock-ups, quarter-end, the
  Russell reconstitution, market holidays from the Polygon bank, every monthly options expiration and VIX settlement.
  The 2027 FOMC dates are the Fed's revised schedule of 2026-11-12 (October 26-27 is a strategy meeting with no
  decision; the last decision of the year moved to November 16-17). Contract awards get a date. The big-bank earnings
  row says its date is the usual one, not an announced one. The document gained `sources` (each source, its document,
  its age), `n_ahead`, `by_type_ahead`. The page has a chip for every type the engine writes, escapes provider text,
  reads past days as "35d ago", and a ticker opens the chart.
- **Options, 58 -> 155 names**: the market, sector, rate, credit, metal, oil, country and bitcoin funds with listed
  options and the large companies the first list left out. A name that returns nothing is listed in `not_analyzed`.
- **Insider trades 3.1**: every row is checked against the factor book's tape with the cluster scanner's own rule
  (`insider_hygiene.py`, a byte copy); a price that cannot be is set aside, published with its reason, and kept as
  filed so the next run checks it again. Nothing is dropped without a tape.
- **On-chain**: the join takes any kept CryptoQuant document that carries metrics, labelled with its own status and
  date, and Ethereum takes that document's Ethereum metrics.
- **Plumbing**: the header counts every indicator it scores.
- **Pages**: `jh-engine-data.js` adds an "Engine data" section to each of the twelve pages - every document its
  engines write, in full, as sections (every list a table that opens whole, sorts both ways on any column, filters and
  downloads) and as one row per value. Insiders gained the cluster scanner, selling clusters and industry clusters;
  Options the OPEX calendar and IV history; Crypto the 171 CryptoQuant metrics; Canaries the history, weights, replay
  and read; Breadth its history; Value and Canaries their reads.

## Looked at and left as they are

- **Trend reversal**: could cover the whole market (about 6,700 names) from the Polygon bank instead of 642 fetched one
  by one, but the bank's bars are unadjusted and a split would read as a reversal; it needs the leaders engine's split
  rule first. Not done in this pass.
- **Canary war room**: adding the credit desks (company bond spreads, CDS) or giving the three silent mechanisms a
  direction changes what the barometer says and breaks comparison with its 134 days of history. Khalid's call.
- **Stock valuations, equity research, corp bonds, liquidity, breadth, OPEX**: no engine change; their pages now show
  every field.

## The chart

Twelve buttons between Khalid and Alert: Research, Insiders, Options, Value, Corp bonds, Plumbing, Liquidity,
Canaries, Breadth, Reversal, Calendar, Crypto. None existed. Research opens the company report on the symbol the chart
is showing. Restore recipe: `docs/snapshots/chart-search-2026-11-10/README.md`.

## Proof

- **Dry run** (ops 6973, 14:49 UTC, nothing written): 0 failed checks. Calendar 355 events, 230 ahead (the live one
  had 125 and 12); options 155 of 155 names analysed, none without listed options, 13.2 s on the runner; insider price
  check 1 row of 3,628 set aside (LONA, "priced at 1,162.2 times the tape and worth more than the whole company"),
  30-day buying total $10,689,562,023 -> $770,400,742; on-chain join restored with 10 Bitcoin and 17 Ethereum metrics;
  plumbing's rows and layers both count 58.
- **Deploy**: the five functions at 14:56-14:59 UTC (deploy-lambdas run 45843179909, dispatched with the commit before
  the held-back change as `base_sha`). No function was invoked by hand.
- **Live** (ops 6974, 15:12 UTC, real Chrome on justhodl.ai, 65 checks, 0 failed): the twelve buttons are on the bar
  once each, in order, after Khalid; every earlier button is still there. Each was clicked; each opened its page in the
  workspace under its own name (Research on `why.html?ticker=NVDA`). On every page the Engine data section read every
  document of the page's engines - 29 documents in all - and its count of values equalled this op's own count of the
  same document (Insiders 70,241; Corp bonds 523,256; Value 36,671; Reversal 49,133; Liquidity 13,403; Research 10,670
  for NVDA). Every field listed that same count. A table on each page was opened whole and sorted both ways on a
  column (Value: 500 rows on P/E; Calendar: 355 events; Crypto: 171 CryptoQuant metrics). The chart threw no error.
  The first run (15:01 UTC) failed on one tab: Liquidity's first document is kept at the bucket's root and is served
  by the data proxy, not the site; the section now asks the address `liquidity.html` itself uses (kit 1.0.1).
- **The calendar** was published once from the runner with the engine's own code at 14:59 UTC (one write, no
  invoke); its own run is 12:00 UTC.
- **Each engine's own run** (ops 6975, re-runnable): options carried the change at its 15:00 UTC tick - 155 of 155
  names in 8.6 s on Lambda. On-chain runs 15:31 UTC, insider trades 21:10 UTC, plumbing 14:21 UTC; until then the
  pages show yesterday's documents (the insider page still totals $10.69B until 21:10 UTC).

## Cost

Options: 8.6 s a run against 3.7 s in the five-minute lane, about $0.40 a month against $0.13. The other four add
about a second a day each. One-offs: read-only ops 6969-6973 and 6975 on the GitHub runner, and one S3 write (the
calendar) from ops 6974.

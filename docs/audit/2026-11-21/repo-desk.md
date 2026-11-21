# 2026-11-21 (evening) - the chart's Repo button, the Repo desk, and what the audit found behind it

Khalid, 15:04 ET: "next thing im sure you have built a repo engine display a an icon for it in the chart.html make
sure it cover all the different aspect of the repo market and repo rate including their spikes in crisis".

Every number below is from an op whose report is in `aws/ops/reports/latest/`.

## 1. What existed (ops 6892)

Two engines, not one, and no page that read them together:

| engine | publishes | read by |
|---|---|---|
| `justhodl-repo` 2.9 | `data/repo.json` - the master board, 1,658 series in 19 groups, each with a history file | `repo.html` |
| `justhodl-repo-market` | `data/repo-market.json` - the overnight desk: rates, the SOFR distribution, spreads, Fed facilities, reserves, a 0-100 stress score | five engines; no page of its own |

## 2. What the audit found

**The board's venue data had stopped on 2026-08-04.** Reference rates and volumes, tri-party, GCF and DVP ended on
3-4 August, primary-dealer fails on 22 July, money-fund repo on 30 June, sponsored repo on 18 September. Cause
(ops 6893): `justhodl-ofr-stfm`, the bank those groups read, skips every mnemonic that is already in its `done`
list - so once it had banked all 442 series (6 August) its daily run did nothing, for three and a half months, in
5.5 seconds a day. 312 of 442 series were behind the source, the daily ones by 74 observations.

Also found:

- the "Dollar (FRED trade-weighted)" group, newest print 2026-08-07: banked once, never asked again; the board's own
  barometer measured "broad dollar, 30 days" on August data;
- the DTCC daily fails group had left the page: DTCC's file answers 404, and the engine only listed the banked
  history when the live fetch succeeded;
- the board asked FRED about every one of about 1,000 series on every run - most are annual or quarterly
  flow-of-funds lines - while a daily series such as SOFR was asked only once it was more than three days old;
- nothing on the board reached back before August 2014 on funding rates, so no crisis before 2018 could be read;
- 142 series the OFR bank holds were never on the board (money-fund repo by collateral and term with the yields funds
  earn, primary-dealer securities borrowed and lent);
- the desk feed carried reserves (2,979,656), the TGA (877,028) and swap lines (146) in FRED's millions under field
  names ending `_usd_bn`.

## 3. What was done, in order

| step | change | proof |
|---|---|---|
| OFR bank | every run refreshes the whole bank from OFR's five dataset calls (17 MB, 13 s) and writes a series file only when its observations changed; a truncated answer never replaces a banked history; the sponsored-repo pair the same way; `{"dry_run": true}` | ops 6894 dry run from the checkout: 442 to write, 531,285 banked observations kept, 12 revised, 0 gone, 13,109 new. ops 6895 after the deploy: 442 written, 442 of 442 level with the source, a second run would write 0 |
| board 3.0 | +125 banked series (two new groups), 13 policy / funding / Fed-lending series from FRED back to 1970, five derived spreads, banked DTCC history kept on a 404, FRED asked by each series' rhythm, the dollar complex refreshed | engine suite 4 + 9 + 3 + 16, the handler end to end on a stub bucket for the first time. ops 6896: 1,800 series in 21 groups, the venue groups at 18-19 November, run 309 s (was about 598 s) |
| desk feed | reserves, TGA and swap lines in billions as named; a `policy` block with the corridor's levels | published by the engine's own 21:52 UTC run |
| page | `repo.html` opens on a Desk view and has a Crisis spikes view (`jh-repo-desk.js`); every series chart marks its largest spikes | 17 tests on real numbers (ops 6897); node suite 5,119, deployment gate 1,233 |
| chart | `btn-repo` right after CDS opens the desk | ops 6898, real browser on the live site |

Numbers the board now holds, read back by ops 6896:

| measure | day | value |
|---|---|---|
| SOFR - interest on reserves | 2019-09-17 | +315 bp (the desk engine measures the same 315 on its own) |
| SOFR 99th percentile - SOFR | 2019-09-17 / 2018-12-31 | +375 bp / +325 bp |
| SOFR - fed funds | 2019-09-17 | +295 bp |
| TED spread | 2008-10-10 | 4.58% |
| fed funds - target | 2008-09-15 | +64 bp |
| Treasury fails to deliver | week of 2008-10-22 | $2.68 trillion |
| Fed overnight repo operations | 2020-03-17 | $152.75 billion |
| Standing Repo Facility | 2025-10-31 / 2025-12-31 | $50.35 billion / $74.6 billion |

## 4. What the desk shows

- **Desk**: the stress score and its nine parts; the corridor (ON RRP floor, IORB, the ceiling, the five overnight
  rates, the SOFR distribution); 74 tiles in eight sections - overnight rates by venue, spreads, venues and volumes,
  money-fund lenders, dealers, the Fed's side, collateral haircuts, Europe. Each number carries the date its source
  printed it; one older than its source's rhythm allows says how old it is.
- **Crisis spikes**: 14 named episodes (October 1987 to year-end 2025) against 12 measures - each cell the highest
  reading of the series inside the episode's dates - and, per measure, the largest spikes the data itself shows
  (distance above the median of the quarter before). The episode table only names date windows; every number is read
  from the series.
- The board, favourites, rename, notes and search are unchanged.

No public daily repo rate reaches back before August 2014. 2008 and earlier are shown through fed funds against its
target, the TED spread, settlement fails and the Fed's operations, and the page says so.

## 5. Still open

- **Tri-party haircuts end 2026-07-09.** They were parsed once from the NY Fed workbook by a one-off op that is no
  longer in the repo; they need a refresh lane of their own. The desk marks them "135 days old".
- **DTCC daily fails end 2026-09-08.** DTCC's file answers 404; the banked history is shown and marked.
- **Dealer repo financing is dated 2026-10-28**: `justhodl-nyfed-pd` holds back newer weeks under its own
  qualification rules; the OFR lines for dealer repo ended in 2021. The desk shows the 28 October figure with its age.
- **Sponsored repo** is about five weeks behind at the source itself.
- The first scheduled runs on the new code: `justhodl-ofr-stfm` about 02:29 UTC (should write only the series that
  moved), `justhodl-repo` 06:10 UTC (should bring the dollar group to date).

# 2026-11-22 - the Repo desk's four open items, and a fifth that showed up on the way

Khalid, 2026-11-21 22:44 ET, quoting the four items the desk's first report left open: **"do all these one by one"**.

Everything below was measured before it was built (ops 6899-6901), dry-run before it acted (ops 6901, 6902), and
checked afterwards from the bucket and with a real browser on the live site (ops 6903-6906, 6909).

## 1. A daily repo rate before August 2014

**Found.** The New York Fed published one on 2018-03-09: the volume-weighted mean rate of primary dealers' overnight
Treasury general-collateral repo borrowing, from the Desk's morning survey, daily from 1998-02-20 to 2018-02-28
(4,998 observations; `HistoricalOvernightTreasGCRepoPriDealerSurvRate.xlsx`). The Fed's own research uses it as the
proxy for SOFR before SOFR existed.

**Built.** `justhodl-repo` 3.1 banks the file once (`data/warm/nyfed-research/gc-survey-rate.json`; a missing bank is
rebuilt from the source) and puts it on the board as `GC_SURVEY`. Two derived series carry it into the present with
SOFR, against the Fed's policy rate (the single fed funds target to 2008-12-15, the middle of the range since):

| series | reads | a crisis on this side |
|---|---|---|
| `D_REPO_TARGET` | repo − policy rate, bps | cash is scarce: September 2019, period-ends |
| `D_TARGET_REPO` | policy rate − repo, bps (the mirror) | everyone wants Treasury collateral: 2007-08, Y2K, September 2001 |

The survey ended 2018-02-28 and SOFR began 2018-04-03; the five weeks between are left empty.

**What the data says** (7,154 daily observations, 1998-02-20 to today):

- Repo ABOVE policy: **+313 bp on 2019-09-17**. Nothing else in 28 years is near it (next: +77.5 bp on 2019-01-02).
- Repo BELOW policy: **+309 bp on 2007-08-21** (August 2007), +284 bp on 1999-12-30 (Y2K), +274 bp on 2007-12-31,
  +210 bp on 2001-09-19, +191 bp on 2008-03-19 (Bear Stearns), +189 bp on 2008-10-03 (Lehman; 2008-09-17: repo 0.25%
  against a 2.00% target).

On the page: the Crisis view's first two columns, a fifteenth episode (year-end 1999), and the text that said no repo
rate existed before August 2014 now says what does. **October 1987 still has no public daily repo rate.**

## 2. Tri-party haircuts

**Cause.** The 81 monthly series were parsed once, by an op, on 2026-08-16. Nothing ever read the source again.

**Built.** `repo_sources.refresh_haircuts()` inside the board engine: it reads the New York Fed's workbook
(`.../Interactives/Data/tri-party-repo/tri-party-repo_data`, December 2024 onwards under the reporting introduced in
November 2025; the older workbook, May 2010 - April 2025, is read only to rebuild an empty bank), asks only once the
newest banked snapshot is 36 days old, and writes only the series that changed. The bank is the permanent history: a
banked observation or an ended asset class the source no longer lists is kept. Workbooks are read with a
standard-library reader - no new dependency in the Lambda.

**Dry run (ops 6901).** The parse reproduces all 81 banked ids and titles and **13,457 of 13,457** banked
observations; where the two workbooks overlap the bank already held the new one's numbers.
**Run.** 73 series, +219 observations, 0 revised: August 11, September 10 and October 9, 2026. Four asset classes
ended in 2011 and four in 2015; they stay as they were.

## 3. DTCC daily fails

**Cause.** DTCC rebuilt its site: `www.dtcc.com/data/failsdata.csv` answers 404 since 2026-09-09. A real browser on
DTCC's new chart page showed the request the page itself makes: `https://cms-prod.dtcc.com/data/failsdata.csv?v=<ms>`.
Two traps: without a fresh `v` the cache in front of the file answered a copy a week old, and Python's default client
name is refused (403).

**Built.** The engine asks the new address with a fresh `v`, falls back to the old one, refuses a file whose trailer
count does not match its rows, and merges into the banked history. The two long-dead addresses an older block tried
on every run are no longer asked.
**Run.** 199 shared days equal, 51 new days; **317 days, 2025-08-14 to 2026-11-20, no gap.**

## 4. Dealer repo financing

**Cause - and yesterday's explanation was wrong.** `justhodl-nyfed-pd` was not "holding weeks back". It asked the New
York Fed for all 70 series in one request, and that request has answered **HTTP 422 on every run since 2026-11-12**
(four runs, each logged only as `unavailable: HTTPError`). Measured (ops 6900): 35 names in one request still answer,
40 do not. Its document feeds about ten engines and pages besides this desk.

**Built (4.1.0).** Five requests of 14 names. Each is its own original response - captured, hashed, replayed - and
must hold exactly the series it was asked for. `/primary-dealers.html` links all five and says which one holds a
series. Older retained snapshots, with one response, are shown as before.

**Dry run (ops 6902).** The whole engine from the checkout on a storage object that reads the bucket and keeps every
write in memory: completes, 0 writes.
**Run (ops 6903, one invoke outside its Thursday/Friday schedule).** Published the week of **2026-11-11**, and the
snapshot was reproduced from its 13 retained original responses.

- Dealer repo on Treasuries (cash borrowed): **$3,398.38B**, week of 2026-11-11.
- Dealer reverse repo on Treasuries: the New York Fed marks the main line (`PDSIRRA-UTSETTOT`) withheld (`*`) for the
  weeks of 2026-11-04 and 11-11. The engine publishes no total that needs it. The desk shows the last published
  week - **$3,002.18B, 2026-10-28** - under that date, marked old, instead of a blank tile.

## 5. Found on the way: FRED refusing a run took 1,034 series off the board

The first scheduled run on engine 3.1 (06:10 UTC) did everything in items 1-3, and published **766 series instead of
1,800**. FRED refused every request for about half a minute (the ninth dollar request failed at 06:10:23, the
discovery at 06:10:49; ten minutes later FRED answered everything, 40 requests without a pause included - ops 6907,
6908). The engine asks its FRED discovery exactly once: it got nothing, so the FRED group (1,028 series, the policy
rates among them) and the six spreads built on it were simply absent. The weakness is older than this work.

**Built.** A discovery that comes back empty, or under four fifths of the last good one, is asked again; if FRED still
refuses, the group is served from the last good inventory (`data/repo-state/fred-repo-inventory.json`, written only
when it changes) or, before one exists, from the bank's own documents; and on such a day nothing more is asked of FRED
for the group. `diag.fred_group` says which of these a run used.
**Run (ops 6909, one invoke outside the schedule).** 1,803 series in 21 groups, FRED group 1,028 asked of FRED itself.
The live board was short for about half an hour, 06:12-06:41 UTC.

## Proof

| what | op | result |
|---|---|---|
| the four items measured | 6899, 6900 | sources, the 422, DTCC's new address |
| dry runs | 6901, 6902 | 13,457 / 13,457 haircut observations; dealer engine completes with 0 writes |
| dealer engine, one run | 6903 | week of 2026-11-11, reproduced |
| dealer page, real browser | 6905 | five original responses linked, no page errors |
| board after its runs | 6904 | 1,803 series; survey rate; 73 haircut series at 2026-10-09; DTCC to 2026-11-20; dollar group at 2026-11-13 |
| board repair after FRED | 6907-6909 | whole again at 06:41 UTC |
| the desk, real browser | 6906 | see the report |

Tests: board engine - repo sources 14, board build 5, scope 9, history writes 4, donors 16; dealer engine - research 19,
legacy 11; pages - repo desk 19, dealer page 12; node suite 5,123; deployment 1,233.

## Runs outside the schedule (disclosed)

- `justhodl-nyfed-pd` once, 04:27 UTC (it had published nothing since 2026-11-06; its next scheduled run was 2026-11-26).
- `justhodl-repo` once, 06:37 UTC, after FRED's refusal left the live board short. Its 06:10 UTC run was its own.

## Still open

- The New York Fed withholds dealers' Treasury reverse repo for the latest two weeks; nothing to fix on our side.
- October 1987: no public daily repo rate.
- Sponsored repo is about five weeks behind at its source; ECB MMSR's newest is 2026-09-15.
- The FRED discovery reads the first 1,000 of the 2,012 series FRED tags "repurchase agreements" (alphabetical). The
  rates and facilities are covered by name; which series fall in the second thousand was not examined. Not changed.
- Other engines that ask a source exactly once and drop a whole group when it refuses were not looked for.

## After the first proof

ops 6906's first pass (07:21 UTC, 0 failed checks) showed one thing worth changing: the chart behind "Repo − policy
rate" listed 2008-12-16 (−2.5 bp, up from −85.5) as its second-largest spike. Repo climbing back from far under the
policy rate is a return to normal, not a squeeze; the Crisis list beside it already left it out. A measure's own chart
now lists exactly what the Crisis view lists (`chartSpikes()`). Second pass, 14:23 UTC: 0 failed checks.

One trap for the next proof: the site build replaces every shared script's `?v=` with a content hash, so a proof must
ask the served page which script address it names. ops 6906 waited 13 minutes for the repository's string before that
was understood; the page had been live all along.

## Taking it back

Page: `git checkout 537ec91a -- repo.html jh-repo-desk.js tests/repo-desk.test.js tests/fixtures/repo-desk.json.gz`, then
`python3 scripts/build_page_data_contracts.py`, commit and push. The engines are independent of the page. Engine 3.1's
banks are additive (`gc-survey-rate.json`, the three new haircut months, the DTCC days, the FRED inventory); the board
simply stops carrying a series whose code is reverted. The dealer engine must not go back to 4.0.0: its single request
is refused.

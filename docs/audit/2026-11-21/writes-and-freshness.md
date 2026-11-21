# Writes and freshness - what was fixed, what was measured, what is open (2026-11-21)

Khalid, 2026-11-20 22:20 CT: "Yes do 1 then 2 then 3 thrn have my data update as needed to stay fresh and make sure my
bill stays the same." The three steps came from ops 6870 (where November's money goes): S3 write fees, each paid twice
(the live bucket and its archive copy, $0.01 per 1,000 files together), were 37% of the month.

Every number below is from a committed report under `aws/ops/reports/latest/`.

## 1. The history book writes only what changed - done, live

`justhodl-bank-histories` v1.2.0, deployed 2026-11-21 04:47 UTC (ops 6871 dry run, ops 6873 first runs).

| | before | after |
|---|---|---|
| files written per pass | all of them: 381,200 | the ones that changed |
| passes a day | 1, and 3 every other day | 1 |
| files written a day | 110,000 to 352,000 | 0 on a day the bank did not change; about 90,000 on a day it did |
| first live pass | - | 109,043 written, 210,051 left alone, 0 failed, 722 s |
| a second pass with nothing new | everything again (one run: 123,623) | 0 written, 21 blocks confirmed without being read |

Causes: the write-if-changed hash covered `as_of` (the FMP bank manifest's `updated_at`, a new value on every bank run),
so no document ever matched; and every block went "pending" again after 20 hours while one run reached 8 of 25, so the
backfill conductor kicked two more full passes every other day (the $4 / $7 alternating S3 days).

Proof of both verdicts on documents read back from the bucket (ops 6871): 240 of 240 "unchanged" documents equal the
stored object in everything but the stamp; 66 of 66 "changed" ones differ from it.

Also fixed on the way: a failed put used to be recorded as written (only the daily full rewrite hid it); the engine's
own test suite had been red since 2026-10-28 (a fixed date in a fixture) and would have blocked any deploy of it.

## 2. The other writers - measured; little is left

ops 6872 compared the newest two versions of the keys each engine wrote on 2026-11-20 (about 222,000 writes):

| | files | |
|---|---|---|
| rewritten with the same data | 127,000 | 110,600 of them the history book (fixed above) |
| really changed | 67,000 | factor book 27,000-37,000, Polygon end-of-day 11,400, ... |
| a key written for the first time | 21,000 | research originals and evidence, by design |

Unchanged rewrites outside the history book come to about $5.50 a month over six engines: `worldbank-full` ($1.90),
`eba-risk-dashboard` ($1.37), `polygon-bank` ($1.12, only Polygon's `request_id` differs), `justhodl-repo` ($0.69, same
bytes), `fdic-bankfind` ($0.25), `dol-full` ($0.20). Small, bursty writers are estimated loosely by this method.

Not changed, and why:

* **The factor book** (`justhodl-bank-factors`) has the same stamp-in-the-hash pattern (run record: 27,127 written, 0
  unchanged) but most of its documents do change every day (price, returns, ranks), and `bank_enrich` hands the
  document's own `as_of` to consumers as a label. A fix there is a reader change for a small saving.
* **`justhodl-repo`, `justhodl-polygon-bank`, `justhodl-fmp-bank`, `justhodl-worldbank-full`**: their source files sit
  behind the privacy guard (the contact address in a request header). They were not read or edited. With Khalid's say
  the first two get a ten-line check each.

## 3. Slow sources - a blanket weekly move would save almost nothing

ops 6874 measured all 41 engines on slow official sources: together $19.49 a month of Lambda and $2.89 of writes.
Two of them are nearly all of it:

| engine | runs in 7 days | a month | why |
|---|---|---|---|
| `justhodl-worldbank-full` | 240 x 677 s | $11.60 + about $2.50 writes | its weekly pass re-downloads all 29,490 indicators (about 5 s each); it continues itself 12 hops at a time, the chain resumer picks it up, and the daily tick adds a chain a day until the pass is done |
| `justhodl-eia-bank` | 85 x 771 s | $4.68 | still filling: 50.7 M of 218.9 M rows banked; the conductor kicks it 12 times a day |
| the other 39 | | under $3.50 together | BLS, BEA, Census, CFTC, EBA, FDIC, DOL, 13F, TIC, BOJ ... cents each |

A weekly schedule for the 39 cheap ones would save cents and would show payrolls, CPI, claims and COT up to six days
late. They stay daily. The World Bank walker is already weekly; its cost is the pass itself.

Open for Khalid (either is one word):

* **"monthly"** - the World Bank pass once a month instead of once a week: about $9 a month less, World Bank series up
  to a month behind their source (annual and quarterly data, revised a few times a year).
* **"skip unchanged"** - teach the walker to ask the World Bank which sources changed and download only those: the
  same saving with weekly freshness, but it is an edit to a file behind the privacy guard.

`justhodl-polygon-bank` ($6.97 a month, 88 runs a week) and `justhodl-eia-bank` are backfills Khalid ordered on
2026-10-25 ("100% of the data"); they end when the history is in.

## 4. Freshness - the feeds that are really late, and why

The freshness monitor reports 667 of 1,554 feeds past their limit. An alarm that is always on says nothing, so ops 6876
joined every such feed to the engine that writes it, that engine's live schedule and its runs:

| what it is | feeds | |
|---|---|---|
| no engine claims the file | 162 | leftovers, 156 of them older than five weeks |
| slower by schedule | 53 | weekly and monthly engines measured against a 26-hour limit |
| no engine runs it | 31 | the writer is retired or has no enabled trigger |
| unknown to the monitor | 305 | no age readable |
| **the engine runs every day and the file is old** | **115** | 90 engines; 28 of them end their runs in an error |

ops 6877 read those engines' own logs. The largest single cause is one provider:

* **FMP's bandwidth quota.** FMP Ultimate allows 150 GB of responses per rolling 30 days for the whole key. The
  2026-10-25/26 backfill spent it; since then FMP answers 429 to most calls: `fmp-ratios` 314 of 314 refused,
  `stock-screener`, `earnings-blackout`, `positioning-analog`, `russell-recon-frontrun`, `vol-target-unwind`,
  `earnings-iv-crush`, `sellside-views`, `cot-tracker`, `watchlist-debate`; and the bank-first shim turns price answers
  down as stale because the bank itself could not refresh. The clusters of feeds that stopped on Oct 31 and Nov 3-4 are
  this. Each day a little quota ages back in - and the conductor's kicks of the two FMP engines took it within the hour.
  **Changed today:** the conductor gives `justhodl-fmp-bank` and `justhodl-fmp-walker` no extra kicks any more
  (`KICK_CAPS`, commit 5d56a595); their own daily runs carry the backfill, the fleet's live calls get the rest. The
  October backfill leaves the 30-day window around Nov 24-27. Still Khalid's call since Oct 26: FMP's bandwidth add-on.
* **Engines that refuse to publish on old inputs** (by design): `forensic-screen`, `share-flows` ("acquisition older
  than 48 hours"), `hiring-velocity`, `volatility-squeeze-hunter`, `dark-pool`, `estimate-revisions` (Benzinga 403).
* **Their own faults**: `signal-genealogy` (`complete_artifact_exceeds_budget`, 18 of 18 runs), `insider-aggregate`,
  `sector-flow-state`, `port-cargo`, `research-backtest`, `causality-scanner`, `bagger-engine`, `ecb-derived` (12 of
  12), `feed-catalog` (8 of 9), `trade-nowcast`.

Not done here: per-file limits for the 53 slower-by-schedule feeds and a clear-out of the 193 leftovers, so the monitor
goes quiet enough to mean something; and the engine-by-engine repair of the last group.

## 5. One snapshot document per symbol - shipped 2026-11-21 19:24 UTC (history book 1.3.0)

What the history book still wrote on a day the bank refreshes was real change: nine small snapshot documents per
symbol, six of them carrying price-linked values - about 90,000 of 135,000. Version 1.3.0 keeps every snapshot family
of a symbol in one document.

Order it shipped in, each step proven before the next (Khalid's say 2026-11-21 12:12 ET):

1. the reader (`aws/shared/fmp_bank.py`, reads both layouts) to all 225 importers - ops 6887 downloaded every package:
   224 byte-identical to the repo reader, `justhodl-bond-desk` held back by its red test;
2. the writer - ops 6888: dry run 27,127 documents in 19.9 s with the state file untouched; first run 129 s, 27,127
   written, 0 failed, 62.6 MB; 333 of 333 families on 60 symbols equal, row for row, to the nine documents they
   replace; the nine one-family blocks retired from the manifest; a second pass writes 0 in 0.9 s;
3. the bond desk - its test was red for a real reason (stale compiler pins, see `crypto-chart-and-decisions.md`
   section 4); fixed, deployed 19:40 UTC, 225 of 225.

A refresh day now writes at most 27,127 snapshot documents instead of about 90,000.

## 6. The bill

Writes by everything but the history book run at about 105,000 files a day (Nov 14, 19, 20).

| S3 write fees (both fees) | files a day | a month |
|---|---|---|
| November so far (ops 6870: 7.1 M writes + 6.6 M archive copies in 21 days) | about 330,000 | about $98 |
| after step 1, on days the FMP bank refreshes | about 195,000 | about $58 |
| after step 1, on days it cannot (most of the last week) | about 105,000 | about $32 |
| with the one-snapshot-document change, bank refreshing | about 130,000 | about $39 |

The month was on pace for about $275 against the $200 Khalid expects (ceiling stated 2026-11-21: $250). Step 1 alone
moves the pace to roughly $235; the snapshot change (about $19, shipped) and the monthly World Bank pass (about $9,
applied by ops 6885: `rate(7 days)` -> `cron(0 1 1 * ? *)`) take it to about $205. These are estimates from measured
write counts; the bill itself confirms them only as the days are billed.

The first full day on the new engine is 2026-11-21; ops 6870 re-read on 2026-11-22 confirms the day's cost.

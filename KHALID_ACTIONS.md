# KHALID — Manual Action Queue

This file tracks items that require **your direct action** (registration, credentials, one-click workflows). Claude maintains and clears items as you complete them.

---

## 🔴 Time-sensitive

### 0n. "Fix it all - 100% of the data pulled and used" - the provider banks are live (2026-10-25); your decisions below

* **Done today (all read-only verified or deploying):** `justhodl-fmp-bank` (every FMP bulk file + eod-bulk since 2000;
  first kicks banked 145 objects / 1.7 GB, 24 bulk endpoints served - `key-metrics-bulk` and `ratios-bulk` included;
  v1.1.0 streams gzip, 600 s read timeout, EOD gets its budget share), `justhodl-fmp-walker` (~160 non-bulk FMP datasets
  incl. 1-minute bars for the chart universe; v1.1.0 after the first run died without a state write - memory bounded),
  `justhodl-polygon-bank` (31 datasets beyond the tape), `justhodl-provider-bank` (Trading Economics, CoinMarketCap,
  NewsAPI, Nasdaq Data Link, CryptoQuant splits), `justhodl-provider-coverage` (`data/provider-coverage.json`), the
  bounded backfill conductor `.github/workflows/fmp-backfill.yml` (daily 02:41 UTC, self-disables when every bank is
  STEADY). `aws/shared/fmp_bank.py` answers 70+ `/stable` paths from the banks before the provider. implied-prob and
  trade-journal no longer call the not-entitled `/v2/last/trade`. Doctrine: `docs/providers/WAREHOUSE_BANKS.md`.
* **Your word needed:**
  1. **Massive/Polygon tier**: trades/quotes (`/v2/last/trade`, `/v3/quotes`) answer NOT_AUTHORIZED on this key and
     Benzinga has 403'd since July. Either upgrade (Stocks Developer/Advanced; Benzinga add-on) or leave them off -
     the engines that used them now read the entitled snapshot / the FMP calendar shim.
  2. **Quiver**: no key in SSM (`/justhodl/quiver/api-key`). `justhodl-lobbying-intel` serves a 120-day-old cache daily.
     Buy a key (also unlocks govcontracts / offexchange / wallstreetbets / insiders / patents / sec13f) or switch it off.
  3. **Trading Economics / CoinMarketCap / NewsAPI / Nasdaq Data Link plans**: each dataset the key cannot reach is on the
     coverage card as a refusal code (TE 403 on calendar history, CMC 1006 on historical endpoints, NewsAPI 426/
     rateLimited, NDL 403 per database). Those are plan lines - decide per provider whether the plan or the subscription
     changes. Nasdaq Data Link in particular: if it is paid, cancel unless the entitled-database list shows something FRED
     does not give.
  4. **`justhodl-estimate-revisions`** is contract-bound to the Benzinga calendar shape and fails daily; the rewrite onto
     FMP `/stable/earnings-calendar` (banked daily by the walker) is a contract change - say "rewrite it" and it ships.
* **Done later the same day - "pulled AND used":** 209 of the 229 engines that built their own FMP URLs are now
  bank-first through `aws/shared/fmp_bankfirst` (one guarded install block each; every engine's own tests + the 1,238-test
  deploy gate green; deploy lanes A/B dispatched 18:46 UTC). The shim answers only a call it can reproduce exactly and
  only when the bank is at least as fresh as the provider would be (prices: the last completed session, never the
  partial bar; fast feeds: 1 day; bulk: 2 days), and steps aside for the rest of the window when the bank would cost
  more than a live call. The reader indexes each bulk CSV once per container inside a memory budget (a per-symbol loop
  of 1,000 names costs ~2.5 s once, then ~0.1 ms per name). `justhodl-eia-bank` (the whole EIA v2 catalog) is deployed;
  birth op 6650 and today's bounded backfill are running. Full rules and the not-converted list (9 byte-pinned engines,
  katlin's live alias, 2 engines with pre-existing failing tests): `docs/providers/WAREHOUSE_BANKS.md`.
* **Also done:** `justhodl-fmp-walker` v1.2.0 - a daily-complete price lane (every chart name, US active stock/ETF, index,
  USD forex pair, top crypto and commodity refreshed in full every run) so price asks are answered at the last completed
  session; `justhodl-tv-bars` v1.3 `mode=fmp-append` (23:58 UTC) - the 13 non-US and 4 index chart names stuck since Oct 2
  advance from the FMP warehouse, proof-gated (ops 6653 creates the schedule and runs the first append).
* **Next on the lane (no decision needed):** the Polygon aggregates bank + shim for the 131 engines that call
  `/v2/aggs/ticker/.../range/1/day` per ticker (design in `docs/providers/WAREHOUSE_BANKS.md`; `justhodl-polygon-full`
  already holds every session's grouped file); ledgered patches for the 5 remaining byte-pinned engines; a requests/
  urllib3 adapter for the 3 engines not on urllib; the first coverage scoreboard (06:47 UTC) read back tomorrow.

### 0m. "Yesterday cost $30" (2026-10-23) - explained, two leaks fixed, crawlers blocked, archive copying halved; open: 9 secrets, 2 servers (docs/audit/2026-10-23/aws-bill-oct22.md)

* **Oct 22 = $32.55 = $25.09 one-time + $7.46 normal.** The one-time part is archive backfill #2 (your "continue"):
  2,086,159 replication writes into Standard-IA $20.99, Batch Operations $2.34, reading IA copies $1.76. Job finished
  (2,086,159 / 2,086,159, 0 failed); it does not recur.
* **October so far: $1,097.93 net** = usage $2,869.72 - AWS credits $1,783.38 (posted on the Oct 12-16 loop days) +
  Business Support+ $11.58.
* **Fixed today:** (1) Apple's crawler (Applebot, 17.166.x = *.applebot.apple.com) was rendering why.html ~6,000 times a
  day and downloading its eleven data files straight from S3: 10.2 GB on Oct 22, a 30 GB/day pace on Oct 23
  ($0.9-2.7/day). Denied at the bucket (ops 6626), proven: Applebot 403, Safari/Googlebot 200, all 51 public probes
  unchanged. (2) The log-ingestion alarm cost ~$19/month as a Metrics Insights query; same alarm now on the
  account-level metric, $0.10/month (ops 6627, equivalence proven hour by hour).
* **The AWS anomaly e-mail of Oct 23 (S3, Oct 21-22, $38.88) = the two archive backfills you approved; over** - Oct 23
  copied 83,148 files ($0.83, the normal daily archive copying), no Batch Operations job queued or running; marked
  "planned activity" in Cost Anomaly Detection (ops 6642, 2026-10-24).
* **Fixes measured (Oct 24, ops 6644):** S3 data transfer down ~93% (40 GB on Oct 23 -> ~2.9 GB/day on Oct 24); Apple's
  crawler refused on every request (~5,800 a day); the expensive alarm line is gone ($0.51-0.64/day before, $0 on Oct 24).
* **November projection: ~$173 of AWS usage + $29 Business Support+ = ~$202** (band up to ~$225 if the Cloudflare-path
  egress stays at today's pace).
* **Your decisions (2026-10-23 13:21 CT) - done / open:**
  - Business Support+: **kept**, as you said.
  - Crawlers: **blocked from the data at Cloudflare and at S3** (Worker commit 14261948 + ops 6628), proven live (ops 6629):
    crawlers get 403 on both hosts and in a real Googlebot render; Chrome, the runner's headless Chrome, the engines'
    own agents, scripts, ChatGPT-User/Claude-User and Google Sheets all still get data.
  - Cleanup (ops 6630): **20 dead alarms deleted** (their metrics silent 30 days; definitions backed up). Two items wait
    for you: (1) the **9 old Secrets Manager secrets** ($3.60/month) - the runner has no Secrets Manager rights (and its
    Oct 20 seal stops it granting itself any); delete them in the console: Secrets Manager -> each secret -> Actions ->
    Delete secret -> 30-day waiting period (restorable until then; billing stops at once); (2) the **two stopped servers** ("web server", "OpenBB Server", $1.28/month) -
    terminating deletes their disks for good and your Oct 20 seal stops the runner from taking a backup copy first:
    say "delete without a copy" or "keep".
  - **Archive: done - option A (your "A", 2026-10-24).** B (narrowing) proved impossible with S3's rules, so new archive
    copies are now stored as Intelligent-Tiering instead (ops 6637, 14:48 UTC): every file is still copied, the lock is
    unchanged, the copying costs about half ($29.00 -> ~$16.44/month now, ~$27 instead of ~$47 averaged over a year).
    Proof: the test file reached the archive as Intelligent-Tiering; real engine files checked by ops 6643.

### 0l. The 13:20 UTC alarm re-check - all three DONE (2026-10-22 17:37 UTC): futures-curves fixed, orphans were alias targets, policy A applied

1. **futures-curves - FIXED and publishing again** (commit 4b55630f, ops 6614). Root cause: Massive now allows about
   5 futures requests a minute on your key (the first 5 catalog calls answer 200, the rest 429 with an empty body; the
   empty body died in evidence retention as the hidden ValueError, every run since Oct 11). Now: live requests paced
   to the limit (12.5 s apart), 429 / empty body = provider failure (never retained), run budget 840 s, Lambda timeout
   900 s. First paced run: complete - 46 requests in ~9.5 min, 42/42 datasets, 7 products, 21 contracts, published;
   `data/futures-research.json` regenerated 16:45 UTC, the first time since Oct 10. Caveat: the 16:40 lane tick and the
   16:42 verification invoke overlapped that run and shared the 5/min budget - the 16:40 run published a weaker packet
   over it at 16:48 (25 of 42 datasets, 15 contracts). **Day 2, 2026-10-23: the daily run (its enabled rule is
   `polygon-futures-curves-daily`, 13:20 UTC) ran alone and clean - 46 requests in 9.5 min, 42/42 datasets, 7 products,
   21 contracts, published 13:29 UTC; the public head is the complete packet again; 1 invocation, 0 errors, 0 throttles.** Worth your word: a one-line guard so a less complete packet never publishes over a more
   complete one of the same definition date. The engine left the 5-minute lane
   (11 members now; a 9-minute run cannot repeat every 5 minutes) - its daily 13:00 UTC run covers the feed. Still
   worth a look in the Massive dashboard: whether your plan is meant to include futures at a higher rate (then the pace
   can come down). Issue #122 closed.
2. **The "20 orphan rules" were not orphans** - justhodl-fleet-integrity read `...:function:<name>:live` targets as a
   function named "live" (ops 6613 resolved all 20: the functions and aliases exist and run - katlin 102 invocations in
   7 days, public-archive-index 104, fleet-freshness-monitor 64 ...). Nothing was disabled. fleet-integrity v1.2.1 is
   alias-aware; its re-run reports D4 = 0 (new defects 269 -> 248: D8 errors 174, D7 concurrency 57, D2 timeout-clipped
   7, D14 unscanned 6, D6 never-ran 4 - the integrity alarm stays red on those until they are worked).
3. **LLM voices - policy A, your word at 13:22 ET: keep the no-paid-AI policy ($0) and make the monitoring honest.**
   The facts first: both providers answered 200 to this morning's health ping (keys valid, credit present); the alarm
   was red only because the ping runs once a day and the alarm expected a reading every hour. Done (ops 6615):
   `justhodl-llm-providers-down` now evaluates one datapoint per 24 h, missing data is not a failure - it fires only
   when `ProvidersUp` < 1 (definition backed up under backups/alarms/20261022T173528Z/); the contract gate accepts
   `llm_status: "unavailable"` as the declared no-paid state for ka-analysis / khalid-analysis (commit f48075f0), so
   those two sev1 rows are gone - sev1 55 -> 44 (ROW_COLLAPSE 33, MISSING_KEYS 11: bond-trace, correlation-breaks, esi,
   gap-est-snapshots, global-m2, katlin, liquidity-inflection-decisive-call, muni-ratio, put-call-extreme, sloos,
   taiwan-moea - the pre-freeze set, next piece of work). Pages keep saying "offline (deterministic desk read)".
   Reopen only if you want a voice back (option B: one voice under a daily dollar ceiling).
Cleared by itself: `justhodl-schedule-drift` (OK since 07:31, 0 undeclared); wl-series-api released and quiet. Still
red and known: contract-sev1 (55 sev1 - 41 ROW_COLLAPSE, 12 MISSING_KEYS, 2 llm_status; STALE sev2 falling 719 -> 292).

### 0k. chart.html - BACK 2026-10-21 14:39 UTC; bar bank on Polygon 16:44 UTC; auto-hide fixed 16:47 UTC (docs/audit/2026-10-21/chart-restore.md)

* **What was wrong**: every desk opened but the chart was blank - every bar request goes data-proxy worker ->
  `justhodl-symdir` Function URL, and the 2026-10-20 flood caps (ops 6558) put symdir at reserved concurrency 2; a
  chart page fires dozens of bar requests at once, so past the second one the URL answered 429 and the worker 502
  (1,790 in a four-minute probe). Fixed by the runbook's own rule - that one function raised to 30 (ops 6598,
  bounded: ~$0.36/hour per concurrent copy under a flood, fuse and runaway alarms unchanged). Proof: the live
  headless-Chrome probe (ops 6597, 14:39 UTC) shows `AAPL 342.94 -0.52 (-0.15%)`, 11,549 bars, detail populated,
  zero 502s; all 22 desk buttons open their desks.
* **Engines behind the chart refreshed**: 5 freeze-stale writers re-run once (on-chain, DIX, GF value, ETF desk,
  squeeze); dark-pool refused a second attempt for the day ("request already attempted") - its 21:40 UTC run
  stands. New engine **`justhodl-fmp-ratios`** (daily 22:38 UTC): `data/fmp-ratios.json` had no writer since the
  2026-09-15 one-off harvest, so the Valuation / Financials desks AND `fmp_book` (the FMP cache 238 engines read
  first) served five-week-old ratios. First run 15:08-15:15 UTC: 506 names (core 40 + S&P 500 + the names you opened
  on the chart), 0 errors, AAPL P/E 39.2 as of today.
* **Bar bank - DONE 2026-10-21 16:44 UTC (your call: Polygon US append)**: `justhodl-tv-bars` v1.2 `mode=polygon-append`
  - one Polygon grouped-daily call per missing session (every US stock/ETF in one answer, split-adjusted), bars stamped
  09:30 ET exactly like the TradingView rows, append-only, never a hole, per-bar label `polygon-aggs:daily-adjusted`.
  Schedule `justhodl-tv-bars-polygon-append` 22:52 UTC Mon-Fri (declared; ops 6600). First run: 288 names advanced
  from Oct 19 to Oct 20 (11 calls, 0 failures); the worker's `/ohlc` for AAPL now ends 2026-10-20 close 339.50.
  Tonight's run adds Oct 21. The TradingView path is untouched (it takes over again whenever TV answers); 2 names
  with gaps older than 15 sessions and the 14 non-US-namespace names at Oct 2-4 wait for TradingView.
* **Top and side bars - FIXED 2026-10-21 16:47 UTC** (`jh-chart-autohide.js` v=20261021-ah2): the 📌 / Shift+H pin was
  stored for ever, so once pressed the bars stayed on at every load; and a bar that slid in under a resting pointer
  never got a mouse-leave, so it stuck open. The pin now lasts one tab session and hiding is position-based. Proved
  on the live page (ops 6601): hidden on load, each edge reveals its bar, hidden again ~0.4 s after the pointer
  leaves - in a fresh profile and in one carrying the old pin.
* **"Why is US10Y 2 days old" - FIXED 2026-10-21 19:10 UTC** (ops 6602 live probe): the chart routed TVC:US10Y to FRED:DGS10,
  and FRED's H.15 prints each day's Treasury yield the NEXT business day at 16:15 ET - so at 13:30 ET on a Wednesday
  FRED's newest print is Monday's; the two fresher routes (TradingView bank, banked Yahoo copy) were both dead with
  the TradingView refusal. Now the market yield symbols route to market sources: US10Y/TVC:US10Y -> ^TNX, US05Y ->
  ^FVX, US30Y -> ^TYX (CBOE yield indices through Yahoo, asked live before any banked copy - candles with bodies, as
  you set up on Sep 14), US02Y/US20Y -> Treasury's own par curve (ustpar, the exact series FRED mirrors a day later;
  T-1 until Treasury's 15:30-17:00 ET post, then same-day after the 22:29 UTC bank run). FRED:DGS* stays FRED when
  you ask for it by name. Proof on the live page: TVC:US10Y `5.13 +0.01 (0.20%) O 5.10 H 5.13 L 5.08 C 5.13` for
  2026-10-21 (source yahoo); US02Y 4.79 for 2026-10-20 (Treasury); FRED:DGS10 5.15 for 2026-10-19 (FRED, by design).
  The watchlist's US10Y row now carries the same live route. Two doctrine tests that pinned "US10Y is a FRED alias"
  were updated and ledgered (tests/fixtures/chart-macro-line/*us10y*).
* **Next, on your word**: a live bar on chart.html during the session - the worker already serves Polygon's forming
  session (`/ohlc?...&tail=1`), the chart engine just never asks for it; ~15 lines in jh-chart-engine.js. Caveat: on
  the Polygon Basic plan (end-of-day only) the tail stays empty until the close.
* **Also found, yours to decide** (details in the audit doc): `justhodl-estimate-revisions` fails every run since
  Oct 10 - its Benzinga earnings calendar (`api.polygon.io/benzinga/v1/earnings`) answers unauthorized (not in the
  Polygon plan?); `justhodl-share-flows` fails since Sep 26 because the GitHub job
  `capital-structure-source-refresh.yml` has failed at its "plan" step every day since Oct 8 (another lane's
  producer); `justhodl-earnings-pead` fails on "no selected universe". The chart's page code still hammers four
  dead fallbacks (`/api/klines`, `/api/yahoo`, `/api/trades`, `/data/series/*.json`: 1,500 wasted requests per
  session) and calls the edgar-insiders Function URL directly (404s) - page cleanups I can do on your go.

### 0j. This morning's alerts (2026-10-21 08:07-08:17 CT) - what they were, what was done

* **ANOMALY e-mail, `justhodl-wl-series-api` held**: the Lightpanda/1.0 crawler looped over forensic / bottom / why /
  share-flows pages (~1.3 pages a second) and every page view fired the nav drawer's `?diag=1` telemetry beacon
  (ops 3276) at that Function URL - 9,480 invocations in 8 h, none of them real series requests. The beacon is gone
  from the served site (Pages run 39607928969), the hold was released at 13:32 UTC (ops 6595; reserved concurrency
  back to 2). The sentinel did exactly what it should; the crawler is already denied at the bucket (ops 6549) and its
  page reads come from the edge cache.
* **justhodl-schedule-drift (21)**: 21 live triggers were never declared in config/schedule-manifest.json (four
  "residual cost control" rules, fifteen 2026-10-09 risk / CDS-desk schedules, the governor's tick, the disabled
  katlin rule). Declared (ops 6593/6594, repo + S3); the alarm clears at the reconciler's next 07:30 UTC run.
* **justhodl-contract-sev1 (46) and justhodl-integrity-new-defects (263)**: freeze aftermath for the most part
  (719 STALE sev-2 files, 38 ROW_COLLAPSE sev-1 = engines that ran on four-day-old inputs, 170 engines with errors,
  58 concurrency drops from the thaw). Re-checked 2026-10-22 13:20 UTC after one full cycle. Already real and yours
  to decide: the two LLM-status mismatches (the voices run against exhausted Anthropic credits / a 429-ing GLM -
  this is also the `justhodl-llm-providers-down` alarm), four engines that hit their timeout on every run
  (analytics-snapshot 456/600 s, causality-scanner 558/600 s, imf-full 850/850 s, outcome-checker 736/900 s - a
  timeout raise or a split, engine changes), and 20 orphan rules targeting functions deleted on Oct 20 (harmless;
  proposal: disable them).

### 0i. Polygon intraday lane - LIVE since 2026-10-21 04:22 UTC; pass 3 (providers' prints, no bursts) - APPLIED 2026-10-22 01:51 UTC (ops 6605)

The 12 light Polygon feeds refresh every 5 minutes 09:00-16:55 New York time, Monday-Friday (one Scheduler schedule,
exempt from the once-a-day rule by name; the $50 cap and the anomaly sentinel unchanged; ~$1-2/month). First session
2026-10-21: every member invoked ~96 times; `justhodl-polygon-futures-curves` failed and retried on every tick (its
traceback is read at the 13:20 UTC check - an engine change needs your go). Note: the stocks tape is 15-minute delayed
on the current Massive/Polygon plan; real-time needs Stocks Advanced ($199/month) - your call, nothing changes until
you say so.

**Pass 3** (docs/ops/PIPELINE_ORDER.md "Pass 3"): the census re-ran at 01:36 UTC once every daily engine had run after
the thaw - files a day old or more at their engine's run: FRED 43/101, FMP 46/146, Polygon 29/73, ECB 8/19, SEC 5/18,
NY Fed 8/20, Cboe 5/17, each cluster before its provider's print, so the arithmetic holds; one correction (Cboe prints
later than assumed: 17:30 ET). Dry run ops 6604 at 01:44 UTC: 135 retimes, 0 drift - pullers before their print 51 -> 2
(the two session-window FINRA readers, by design), provider minutes over the per-minute cap 36 -> 0 (FMP's busiest minute
19 -> 4, FRED 5 -> 2, ECB 4 -> 2, SEC 3 -> 2), stale reader-writer edges 22 -> 19, mean move 33 min, max 303. Nothing
faster than daily; no caps, budgets, IAM, lifecycle or archive touched. Applied by ops 6605 at 01:51 UTC: 135 triggers retimed, the
11:30 UTC late tick created, both S3 manifests equal the repo, 138 planned triggers read back as planned, 0 not as planned
(one engine ran twice on apply day: cusip-map-rebuild, 01:28 and again 03:30 after the SEC print). **First light (full day,
23:59 UTC): 172 of 173 due triggers fired at their new minute** (the one left is stock-xray, whose 00:35 slot was before the
apply), both late ticks 6/6 and 3/3, the lane's 11 members 96-97 runs each with 0 errors. The day's errors in moved windows
are engine matters, not the timetable: dark-pool 3 at 23:31 (a planned source not available at 23:31 - watched one more
evening), estimate-revisions (Benzinga calendar), research-backtest (its own read deadline). **Day 2 (13:29 UTC Oct 23):
41/41 due triggers at their new minutes, none at the old; stock-xray at 00:35, cusip at 03:30 only; late ticks 6/6 and 3/3;
sev1 44 -> 24 at the gate's 13:02 run.** Docs: docs/ops/PIPELINE_ORDER.md "Pass 3".

### 0h. Pipeline order - APPLIED 2026-10-21 02:59 UTC on Khalid's go (docs/ops/PIPELINE_ORDER.md)

Readers fire after their writers. ops 6584 (dry run, 02:49 UTC) found 82 retimes ready and nothing drifted; Khalid
said go at 22:56 ET; **ops 6585 applied it at 02:59 UTC**: 82 triggers retimed, the late tick live, both S3 manifests
equal the repo, 0 cadence-floor violators, every planned trigger read back at its new time. What changed: 82
triggers moved LATER by 31 min on average (max 67), each inside its own
session window (6 cross a window end by at most 30 min); three daily-morn engines that read other daily-morn
engines - `justhodl-morning-brief-tg`, `justhodl-nobrainer-rationale`, `justhodl-sector-earnings-diffusion` - run
from a late tick at 11:15 UTC; 22 same-minute races -> 0; 116 "reads yesterday's file" edges -> 32; mean input age
10.95 h -> 9.96 h. Nothing moves earlier, nothing is disabled or deleted; every trigger's previous definition is
backed up to `s3://justhodl-ops-private-857687956942/backups/pipeline-order/<stamp>/`. 23 triggers are listed as
left alone (they would need more than an hour or a window change) - a second pass can take them later. Proof of the
new timetable: ops 6586 (read-only "first light") runs at 12:05 and 23:35 UTC on Oct 21 and checks that each retimed
trigger fired at its new minute and the late tick invoked its three members.

### 0. Confirm the AWS alert email (1 click) - fleet-alerts CONFIRMED 2026-10-21; the governor topic still needs one

ops 6582 (2026-10-21 02:10 UTC) found the `justhodl-fleet-alerts` e-mail subscription **confirmed**. It then
subscribed the same address to the governor's own topic `justhodl-spend-governor` (the runaway alarms' notices):
a second **"AWS Notification - Subscription Confirmation"** e-mail is waiting for your click (link lives 3 days; if
it has expired, tell Claude and it is re-sent). The address is never written anywhere by Claude - the runner copies
it from your AWS Budget's e-mail subscriber.

The October 16 confirmation link was never clicked and expired, so **no e-mail alerts reached you during the
$2,800 month**. On 2026-10-20 (ops 6558) a fresh e-mail titled **"AWS Notification - Subscription Confirmation"**
for topic `justhodl-fleet-alerts` was sent to the same address as your AWS Budgets e-mails. Click **Confirm
subscription** (the link lives 3 days; if it has expired, tell Claude and it is re-sent). Only you can click it.

**Since 2026-10-20 21:32 UTC there is a channel that needs no click:** `.github/workflows/governor-alerts.yml` turns
every spend-governor event (engine shut down by the $50 cap, anomaly hold / release, fuse trip, BRAKE / FREEZE,
cadence fix, error storm, operator hold / release) into a GitHub issue labelled `governor` within the hour, and
GitHub e-mails you as the repository owner (issues #116 and #117 were the first two, real events). Close an issue
once you have addressed it. AWS Budgets' own 50/80/90/100% and $30/day e-mails and the Cost Anomaly e-mails reach
you without confirmation as before.

### 0b. New Telegram bot token (optional, 2 min)

`justhodl-guardrail-notify` gets 401 Unauthorized from Telegram. Create a new token with @BotFather and tell
Claude; the SSM parameter is swapped by an ops script (never paste the token in chat or in the repo).

### 0d. Two engines are locked off by the $50/month cap - decide before Nov 1

`justhodl-miss-calibrator` ($1,551 of Lambda cost in October) and `justhodl-event-coordinator` ($432) crossed the
new per-engine cap and are held under CAP: the Nov 1 thaw will NOT restore them. The loop they caused was fixed on
Oct 17 (ops 6520/6521, deploy gate `scripts/event_graph.py`). If you want event routing back on Nov 1, tell
Claude "release the coordinator and the miss-calibrator" and it runs
`{"action":"release_cap","function":"<name>","confirm":"<name>"}` for each; the $50 counter then starts from zero.

### 0e. Two engines have pages and tests but no schedule - say if you want them running

`justhodl-tape-truth` (tape-truth.html) and `justhodl-industry-case` (industry-case.html) were kept out of the
2026-10-20 deletion because pages and tests bind them, but neither has ever had a trigger, so their pages show
whatever was last written. Say "schedule tape-truth and industry-case daily" and Claude adds one daily schedule
each (the $50 cap and the anomaly sentinel cover them like any engine); otherwise they stay as they are.

### 0g. SageMaker stopped and the account sealed - DONE 2026-10-20 (root-only to undo)

SageMaker is at zero (three endpoints deleted, configurations saved; factory switches off; the seven factory
workflows disabled and kept out of the Nov 1 resume). Three sealed IAM deny policies now make it impossible for the
runner or any engine to launch SageMaker, EC2, RDS, Bedrock, EMR and the other by-the-hour services, to mint
credentials, or for an engine to rewrite functions (docs/ops/RUNAWAY_GUARDRAILS.md, Layer 0). **To bring the factory
back one day**: sign in as the root user, IAM -> Policies -> `justhodl-deny-rogue-compute` -> detach from the
group `justhodl-runner-ecr` and the role `lambda-execution-role`; then tell Claude to re-enable the factory switches
and workflows. Nothing else in the system can lift it, by design.

The fleet was thawed on 2026-10-21 00:30 UTC under the full guardrails (ops 6581: stage NORMAL, the five CAP holds
kept, the stage machine asleep until the Nov 1 rollover). A daily Claude health run (06:13 CT) now triages the
governor's issues, fixes what the gates allow, and files "Daily health run <date>" as a GitHub issue.

### 0f. Bucket: "keep my data, stop the balloon" - DONE 2026-10-23 00:35 UTC: the archive is a complete copy (3.46 M objects / 1,032 GB; backfill #2 on your "continue")

Three pieces live: (1) the sealed archive bucket `justhodl-warehouse-archive-857687956942` receives every write to
the live bucket (delete markers never replicated); its policy denies deletes and any weakening to everyone but the
**root user**. The backfill job 523b902a finished 2026-10-22 with 1,282,030 / 1,282,030 succeeded, 0 failed - **but
its manifest came from a truncated listing**: the live bucket's own S3 Inventory (2026-10-21) counts **3.38 M current
objects / 1,023 GB**, not the 1.28 M / 434 GB of the census, and the archive holds 1.33 M of them (439 GB). Missing
almost entirely: `data/providers/` (1.14 M objects, 294 GB - the series page store) and `data/warm/` (779 k, 207 GB -
the warm bank), plus `data/series-cache/`, `data/statement-research/`, `data/price-observations/`, `data/symdir/`,
`data/research-*`, `data/raw/`, `data/vintage-research/`, `data/xbrl-raw/` (ops 6611: 38% of a 3,073-key sample of
current objects was in the archive). **Backfill #2 ran on your "continue"** (ops 6612, job 646ccb69: 2,086,159 /
2,086,159 succeeded, 0 failed, ~8 hours): the archive now holds 3,456,698 current objects / 1,032 GB - 102% of the
inventory census (the surplus is the day's live replication), data/providers/ 1,137,345 = the inventory exactly, and a
3,109-key sample is 100% present. Cost from here ≈ $13/month of Standard-IA plus replication PUTs ≈ $18-20/month all in
(the earlier "$10-12" assumed the truncated 434 GB).
(2) old copies capped at the 10 newest per file bucket-wide (current files untouched by any rule) - the inventory of
2026-10-21 still lists 8.4 M old copies (7.2 M of data/miss-calibrator-proposals.json, 573 k of the audit key), but
they are already expired and unbilled (the bucket bills ~1.03 TB = its current objects); S3 removes them from the
listings asynchronously, so **no manual purge** - the proposal is withdrawn; the daily inventory is the check that the
count rule trims the rest (student-state.json 46 k, crypto-intel.json 11 k, factory/ 18 k ...) to 10.
(3) the governor's storage fuse (v1.5.0). Details: docs/ops/WAREHOUSE_ARCHIVE.md.

### 0c. Site egress - DONE 2026-10-20 (worker routing live)

212 hard-coded S3 data URLs, 13 base constants and 6 preconnects in 158 pages now read `https://justhodl.ai/data/*`
(Cloudflare Worker, edge-cached); Pages run 39501122425 deployed it, ops 6567 verifies the served pages. Eight
byte-pinned pages (alpha-scoreboard, bonds, correlation, master-rank, opportunities, why, private-artifacts.js,
jh-ticker-research.js) still read S3 until their preservation chains are extended. The egress saving is confirmed
from the access logs after Nov 1 (the fleet is frozen until then, so October traffic is not representative).

### 1. PAT Rotation — ⚠️ 6 DAYS OVERDUE (was due 2026-05-25)

The fine-grained PAT on `justhodl-dex-scanner` Lambda was set with 30-day expiration on 2026-04-25 and is now past expiration. Dex scanner may be failing silently.

**Steps (3 min):**
1. Generate fresh fine-grained PAT at https://github.com/settings/personal-access-tokens/new
   - Name: `justhodl-dex-scanner-202605`
   - Expiration: 30 days (or your preference)
   - Repository access: **Only select ElMooro/si**
   - Permissions: **Contents → Read and write** (only)
2. Visit https://github.com/ElMooro/si/actions/workflows/rotate-dex-scanner-pat.yml
3. Click **Run workflow**, paste the new PAT, click **Run**

Workflow auto-validates, swaps, smoke-tests, and rolls back on failure. Old PAT can then be revoked at https://github.com/settings/tokens.

---

## 🟡 Unblocks autonomous work

### 2. FINRA Gateway Registration — unblocks the short-interest data-source fix

The Polygon `/stocks/v1/short-interest` endpoint is dead post-2018 (confirmed via ops 1014-1020). The replacement scaffold is already built at `aws/shared/finra_si.py` (376L, OAuth2 + token caching + 4 public functions). It needs credentials in SSM to activate.

**Steps (~5 min real-time + 2-3 business day FINRA approval):**

1. Register at https://gateway.finra.org/
   - Sign Up → **Developer / Data Consumer** account type
   - Free
   - Wait for approval email (typically 2-3 business days)

2. Once approved, log in at https://gateway.finra.org/developer
   - Click **Create Application**
   - Name: `JustHodl-SI`
   - Scope: `data.public.read` (covers `equityShortInterestStandardized` dataset)
   - Copy the assigned `clientId` and `clientSecret` (**shown only once**)

3. Add the credentials to AWS SSM Parameter Store:

   ```bash
   aws ssm put-parameter \
     --name /justhodl/finra/client_id \
     --value YOUR_CLIENT_ID \
     --type SecureString --region us-east-1

   aws ssm put-parameter \
     --name /justhodl/finra/client_secret \
     --value YOUR_CLIENT_SECRET \
     --type SecureString --region us-east-1
   ```

**After step 3:** ping Claude with "FINRA creds in SSM" and the next session will patch `justhodl-short-interest` to use the new module, verify end-to-end via `health_check()`, and the entire squeeze-pretrigger ecosystem starts producing real 2026 setups (instead of running on schema-fixed but data-stale state).

---

## 🟢 Optional — provider alternative if FINRA approval delays

If FINRA approval takes too long, the alternative paid SI providers (also pre-scoped):

| Provider | Cost | Pros | Cons |
|---|---|---|---|
| Ortex | ~$25-50/mo | Real-time SI + utilization | Recurring cost |
| S3 Partners | ~$30+/mo | Best institutional data | More expensive |
| IBKR Short Sale Locator API | Free if IBKR account | Real-time | Different schema |

If you want to go this route, ping Claude with the provider name and credentials, and the FINRA scaffold can be adapted (the public API surface in `finra_si.py` stays the same).

---

## 🟢 Low priority — long-standing TODOs

### Canary Grid Phase 3 — Korea/China exports + Swiss unemployment

The `justhodl-canary-grid` Lambda (leading ex-US early-warning engine, 9 FRED signals, 4 sub-grids → 0-100 early_warning_level) has had 3 signals dark since launch because DBnomics series codes need to be found manually:
- Korea exports
- China exports
- Swiss unemployment (the original FRED `LRHUTTTTCHQ156S` series was dead; freshness guard caught it and auto-excluded)

The engine has a freshness guard that excludes >95d stale signals, so this isn't urgent. But filling in those 3 signals would bring the early-warning grid from 9 → 12 signals.

To find replacement DBnomics codes: visit https://db.nomics.world/ and search by country + indicator name. The DBnomics search API times out from our Lambda runner, so this needs manual lookup. Once found, add the codes to the Canary Grid Lambda's `SIGNALS` dict.

---

## Completed (for audit trail)

_(none yet — file initialized 2026-05-21)_

---

## 🟡 Unblocks autonomous work — newer items

### 3. PatentsView API Key — unblocks `justhodl-patent-velocity` engine

The Lambda `justhodl-patent-velocity` is deployed and scheduled (daily 17 UTC), but PatentsView migrated from no-auth Legacy API to PatentSearch API in Feb 2025 which requires a free API key. The Lambda currently writes a stub to `data/patent-velocity.json` indicating `needs_api_key`, and `/patent-velocity.html` shows an actionable "Register for API Key" link.

**Why it matters:** Patent grant velocity is a 12-24 month leading indicator for new product launches, M&A activity, and strategic pivots. Top quant funds (Susquehanna, RBC, Two Sigma) use patent analytics in tech / biotech / industrial momentum strategies. Universe is curated to ~80 high-IP filers.

**Steps (~5 min + manual approval wait):**

1. Visit https://patentsview.org/apis/ → service desk request for API key (free)
2. Once received, drop key in SSM:
   ```bash
   aws ssm put-parameter \
     --name /justhodl/patentsview-key \
     --value YOUR_KEY \
     --type SecureString \
     --region us-east-1
   ```
3. Add to Lambda env:
   ```bash
   aws lambda update-function-configuration \
     --function-name justhodl-patent-velocity \
     --environment "Variables={PATENTSVIEW_API_KEY=YOUR_KEY}" \
     --region us-east-1
   ```
4. Trigger a fresh run:
   ```bash
   aws lambda invoke --function-name justhodl-patent-velocity --invocation-type Event /tmp/out
   ```

**Note:** PatentsView is migrating to USPTO Open Data Portal (data.uspto.gov) as of March 20, 2026. Once that migration stabilizes, consider switching to the ODP endpoint instead. The current Lambda still uses `search.patentsview.org/api/v1/patent/` which is the surviving v1 endpoint.

## 2026-06-10 — 18-edge buildout
- **FINRA data API key (free)** — register at developer.finra.org → put key in Lambda env `FINRA_API_KEY` to activate the Ignition dark-pool pillar (anonymous tier returns symbol-stripped stale rows).
- **FMP 13F holdings tier** — `/stable/institutional-ownership` per-symbol holdings need a higher FMP plan; activates Ignition 13F cluster-initiation pillar (currently parked).
- **FHLB discount-note issuance feed** — no free machine-readable source; paid (e.g. Bloomberg) or scrape fhlb-of.com to complete the 7th crisis canary.

## 2026-06-10 — Brain-gap audit (ops 1580): paid/manual feeds
- **MOVE index real feed** (brain 74×) — proxy live (DGS10 realized vol in canaries); real MOVE needs ICE/Bloomberg.
- **Cross-currency basis** (brain 67×) — JPY/EUR xccy basis needs Bloomberg/Refinitiv; free slice live as SOFR−IORB + EFFR−SOFR + swap-lines.
- **ICI weekly MMF assets** (brain 56×) — quarterly FRED live; weekly = ICI xls scrape or subscription.
- **FINRA margin statistics file** (brain 25×) — Z.1 quarterly proxy live; FINRA monthly file is xls download.
- **NY Fed GSCPI** — xlsx-only publication; needs openpyxl layer in a lambda.
- **Swap spreads (negative-swap-spread signal)** — SOFR swap curve not on FRED; needs ICE/Refinitiv.

## 2026-06-11 — BTC.D / TOTAL2 historical depth
- Free tiers give CURRENT global metrics only. Engine self-accrues daily snapshots (data/_altseason/global-history.json) and attempts a CG mcap-proxy reconstruction. For TRUE long BTC.D/TOTAL2 history: CMC Standard plan (global-metrics/quotes/historical) or CryptoCompare key.
- FREE fast path: create a CoinGecko **demo API key** (free tier) and set env COINGECKO_KEY on justhodl-altseason — unlocks market_chart 401s, lighting BTC.D/TOTAL2 proxy history to 2013+, both event studies and regime splits retroactively.


## Finviz suite (2026-06-11) — built & live
- Market Map (justhodl-market-map, cron 25 14): S&P500 treemap + 11 sectors + 12 industries + 8 themes. Pages: market-map.html, groups.html, themes.html.
- Insider Radar (justhodl-insider-radar, cron 40 14): FMP Form-4 source WORKS (no plan-unlock needed) — 1000 rows/run. insider.html. Polygon 60d fallback for micro-cap decline-join.
- Alert Sentinel (justhodl-alert-sentinel, cron 45 14): Telegram diff alerts; alerts.html. Fires only on change (idempotency verified).
- Rotation +SMH/SPY ratio (v1.2.1). api-docs.html publishes 10 public endpoints + tiers.
- OPTIONAL next (Phase 3): news/catalyst scoring engine (Module 5 — only partial via EDGAR), NDX/Russell map universe toggle, per-ticker quote snapshot page, screener-presets page, pattern-zoo (cup&handle/flag scans beyond current coil/crossing detection).

- **TELEGRAM 403 (2026-06-12):** Alert Sentinel sends now return 403 Forbidden (boot messages at 14:xx delivered fine, then 403) — the bot was most likely blocked/stopped in Telegram after the test boots. FIX: open the bot chat (token 8679881066:...) and press START / unblock. Sentinel v1.1.1 queues alerts (state not advanced on failed send) so nothing is lost — first successful send delivers everything pending.

- **HP-Score follow-ups (2026-06-12):** add to the rubric when keys/feeds allow: insider-OWNERSHIP %% (FMP insider-ownership endpoint), institutional ownership %% (13F), short-interest %% of float (finra-short join — overlap engine already has it), float size. Going-concern / toxic-financing text flags could join from sec-filings-intel. TAM-vs-revenue stays human judgment.
- **S&P SBC/rev (2026-06-12):** key-metrics-ttm lacks stockBasedCompensationToRevenueTTM (AAPL None). Fill via per-name cash-flow fetch or probe km keys containing "tock"; HP layer already has true SBC from cash-flows.

- **PROBE TRUTHS (ops 1634, 2026-06-12):** FMP /stable/ insider-ownership, institutional-ownership, institutional-holders all 404 — factors #11-ownership%/#12 need 13F parsing or another vendor. /stable/shares-float WORKS (floatShares/freeFloat/outstandingShares) and data/finra-short.json carries a full per-ticker `tickers` map → **NEXT BUILD: HP v1.5 short-interest join** = SI fields from finra + float from shares-float → SI%-of-float per HP name, doc setups (high-SI+improving = catalyst bump; SI 20%+ soft flag). S&P SBC key exists (stockBasedCompensationToRevenueTTM) — emit bug fixed in v1.4.2.
- **OPS RUNNER LESSON (2026-06-12):** GH runners kill silent sync lam.invoke waits at ~350s (ConnectionClosedError). Any ops invoking a long lambda MUST use InvocationType=Event then poll the S3 output feed (generated_at > start mark). Function-URL CORS AllowMethods members max 6 chars — never include "OPTIONS" (preflight implicit).
- **DRD research paper (2026-06-12):** JSON chronically malformed across repair rounds. Consider per-section generation or stripping profile description for DRD-class names.

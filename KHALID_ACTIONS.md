# KHALID — Manual Action Queue

This file tracks items that require **your direct action** (registration, credentials, one-click workflows). Claude maintains and clears items as you complete them.

---

## 🔴 Time-sensitive

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
(one engine runs twice on apply day: cusip-map-rebuild, 01:28 and again 03:30 after the SEC print). First light: the 13:20 UTC
check reads the morning half, a 23:58 UTC check the evening half - you get both in a few lines.

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

### 0f. Bucket: "keep my data, stop the balloon" - DONE 2026-10-20 (docs/ops/WAREHOUSE_ARCHIVE.md)

Three pieces live and proven: (1) a sealed archive bucket `justhodl-warehouse-archive-857687956942` receives every
write to the live bucket (delete markers never replicated); its policy denies deletes and any weakening to everyone
but the **root user** - if you ever need to remove something from it, that is a console action as root, by design;
backfill of the existing 1.28 M objects: **running since 2026-10-21 02:21 UTC** - ops 6583 listed the live bucket
itself (1,282,031 current objects, 18,765 of them pre-versioning "null" versions) and started S3 Batch Replication
job `523b902a` from that manifest, current versions only (the S3-generated manifest would have copied the loop's
old versions too; that job was cancelled before it moved anything); ops 6576 reads its progress, the Oct 22
scheduled task re-checks;
(2) old copies capped at the 10 newest per file bucket-wide (current files untouched by any rule); (3) the governor's
storage fuse (v1.5.0). The daily S3 Inventory lands in the private bucket within 48 h; it is the check that the
573k storm copies of the audit key and the `data/` storm copies have been expired by the new rule - if S3 is slow
about it, Claude will propose a one-time manual purge of those old copies (never of a current file).

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

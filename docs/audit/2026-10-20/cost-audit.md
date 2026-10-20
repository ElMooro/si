# Deep cost audit, 2026-10-20 - the $2,800 October, the $600 cap, once-a-day engines, cost per engine

Khalid, 2026-10-20: "a deep deep audit ... I accrued a $3000 charge last month due to an anomaly ... never ever
... bankrupt me ... every single [engine] runs once every 24 hours ... duplicate or wasting money ... never goes
past $600 ... all the engines shut down automatically when it goes past $600 ... dollar cost per engine."

Everything below was done on the account through the ops runner on 2026-10-20 (ops 6546-6558) and is verified
by read-back in `aws/ops/reports/latest/`. Live per-engine figures: the **JustHodl engine costs** dashboard
(artifact, 926 rows, sortable) built from the same data.

## 1. What happened in October

| Period | $/day | What |
|---|---|---|
| Sep 15 - Oct 11 (27 normal days) | 23.89 | the fleet at its old cadence (~$726/month) |
| Oct 12 - 16 | 336 / 516 / 556 / 588 / 537 | miss-calibrator <-> event-coordinator event loop; Lambda ~$450/day + S3 noncurrent copies 0.9 -> 145 TB |
| Oct 17 - 19 (frozen) | 10.13 | 922 of 926 functions held at 0 by the spend governor (ops 6525) |

October to Oct 19: **$2,803.62** on the budget; the loop alone cost ~$2,530. Root cause and the fix of the loop
itself: `docs/ops/POSTMORTEM_2026-10_EVENT_LOOP.md` (ops 6520/6521, deploy gate `scripts/event_graph.py`).

Why nothing stopped it for four days: the guard alarm fired three minutes into the loop but reached only an
unconfirmed e-mail subscription and a dead Telegram bot; the spend governor did not exist yet (v1.0 shipped
Oct 16, the fast fuse Oct 17). The October 16 e-mail confirmation link was never clicked and has since expired,
so **no fleet alert e-mail reached Khalid at any point in October** (re-sent 2026-10-20, ops 6558).

## 2. What changed today (all verified on the account)

| Ops | Change | Proof |
|---|---|---|
| 6549 | Bucket policy denies the Lightpanda crawler (46 GB/day of the site egress) | `ops_6549_deny_scraper_user_agents.md` |
| 6551 | Worker keeps a revalidated edge copy of the large publications (58 GB/day of origin pulls) | release 12246d7f, `ops_6550_edge_revalidate_probe.md` (REVALIDATED, never stale) |
| 6552-6554 | **Every engine at most once per 24 h**: 341 triggers (189 retimed to daily, 152 duplicates switched off, 1 shared-rule target removed); S3 fan-out + schedule manifests in line with the repo; 143 engine configs + manifests committed (08a656cb) | `ops_6554_cadence_apply.md`: enabled triggers able to fire >1/day **210 -> 0**; scheduled firings/day **8,081 -> 759**; every rule/schedule backed up to `backups/cadence-floor/20261020T180417Z/` |
| repo | `tests/deployment/test_cadence_floor.py`: any engine config, manifest entry, fan-out membership, GitHub workflow cron or Worker cron faster than daily fails the Lambda deploy; pages.yml 96x/day (each run purged the whole Cloudflare cache) -> daily; factory-exam 48x -> daily; market-bank-refresh 3 -> 1 pass/day | gate passes locally (1,138 tests) and on the runner |
| 6555 | Budget **JustHodl-Monthly-Hard-Cap-600** (same 50/80/90/100% + forecast alerts as the old 500); governor role rights for the sentinel; fuse interval 6 h -> 2 h; Daily-Spend-Guard $40 -> $30; legacy $150 budget deleted | `ops_6555_cap_600_substrate.md` |
| 11c30bad | **Spend governor v1.2.0**: reads the $600 budget; diffuse-storm fuse; daily cadence sentinel (retimes anything faster than daily that reappears, reversible); S3 egress alert; 38 tests | `ops_6556_cap_600_verify.md`: version 1.2.0, cap 600, mode enforce; sentinel walked 827 enabled triggers live, 0 violators; -500 budget retired |
| 6558 | fleet-alerts e-mail re-subscribed (confirmation sent); reserved concurrency 2 on the 102 public, heavy Function-URL engines, recorded as the governor's Nov 1 restore value | `ops_6558_alerts_and_url_caps.md` |

### How the $600 cap works from Nov 1

`docs/ops/SPEND_GOVERNOR.md`. The governor ticks hourly, reads the budget's actual spend and Cost Explorer, and
holds lines **below** the cap because billing lags a day and some charges continue when everything is off:
with today's measured baseline the brake (shed the top spenders) comes at ~$440 and the freeze (everything but
4 exempt functions at concurrency 0) at ~$480; at a $10/day run-rate the lines sit at $450 / $490. Once it
freezes it stays frozen until the 1st, a cap raise, or an operator release. Independent of the cap: the runaway
fuse (one function far above normal), the fast fuse (CloudWatch alarm -> seconds), and new in v1.2.0 the
diffuse-storm fuse (fleet >= $75/day with no single hot function). Budget Actions cannot stop Lambdas, which is
why a function and not an AWS Budget Action does this.

### Per-engine kill switch (governor v1.3.0, ops 6559, same day)

Khalid: "any engine or events that cause $50 in a month shuts down automatically and inform me and won't turn on
again unless I manually turn it on and address the issue." Every hourly tick now meters each function's
month-to-date Lambda cost; at $50 the function is held at concurrency 0 under ledger stage CAP, Khalid is
e-mailed "ENGINE SHUT DOWN: <engine>" with the release command, and nothing automatic lifts it (not the 1st, not a
budget raise, not a restore; a hand change is re-held). Release: `{"action":"release_cap","function":F,
"confirm":F,"reason":"..."}` on `justhodl-spend-governor`; per-engine limits in `control.json` `engine_caps`.
Proven live on the canary (`ops_6559_engine_cap_verify.md`). The first month-to-date read also found the two
October loop engines over the line - **justhodl-miss-calibrator $1,551 and justhodl-event-coordinator $432 of
Lambda cost this month - and locked them under CAP**: they will NOT come back with the Nov 1 thaw until Khalid
releases them (the loop itself was fixed on Oct 17, ops 6520/6521; releasing is his call).

## 3. Cost per engine (normal window, $/day; projection at once-a-day cadence)

Full table: the dashboard (926 rows; search, sort, flags) and `aws/ops/reports/latest/ops_6557_cost_attribution_data.md`.
Method: Lambda compute + requests from CloudWatch Duration/Invocations at list prices (reconciles within 3% of the
Lambda line on the bill); S3 request charges from the bucket access logs; CloudWatch / Cost Explorer API calls,
custom metrics and the SageMaker endpoint attributed to the engine that makes them.

| Engine | Runs/day before -> after | Measured $/day | Projected $/day | Why |
|---|---|---|---|---|
| justhodl-fleet-error-monitor | 25 -> 1 | 7.58 | 0.12 | GetMetricStatistics for every function every 5 min in the window (783k calls/day = the whole "CloudWatch" line); hourly since Oct 16, daily now |
| justhodl-ai | 169 -> 3.3 | 2.48 | 0.88 | Cost Explorer at $0.01 a call on each run (cached 6 h since Oct 16) + the owned coder async endpoint ($0.70/day) |
| justhodl-symdir | 50 -> 5.9 | 0.87 | 0.87 | self-invoking symbol directory walker: most runs are chained, not scheduled |
| justhodl-engine-fusion | 24 -> 1 | 0.85 | 0.85 | 108 custom CloudWatch metrics ($0.30 each a month) - the metrics cost, not the runs |
| justhodl-provider-catalog | 24 -> 1 | 0.55 | 0.03 | 78k S3 requests a day scanning 57 sources every hour |
| justhodl-sdmx-walker | 385 -> 3.1 | 0.34 | 0.27 | BIS/Eurostat/OECD full-dataset walks (four payloads = four daily jobs) |
| justhodl-tradingview | 73 -> 3 | 0.23 | 0.01 | three schedules (hourly, 20-min, daily) for one job |
| justhodl-news-velocity | 24 -> 1 | 0.14 | 0.01 | hourly GDELT pulls |
| justhodl-polygon-options-flow | 0.9 -> 0.7 | 0.13 | 0.12 | daily already |
| justhodl-daily-report-v3 | 25 -> 2 | 0.12 | 0.01 | hourly + daily twins |
| justhodl-cost-anomaly | 1 -> 1 | 0.12 | 0.12 | daily, Cost Explorer calls |
| justhodl-13f-positions | 24 -> 2 | 0.10 | 0.01 | hourly SEC holdings |
| justhodl-student-rsi | 1,440 -> 0.7 | 0.09 | 0.00 | ran every minute |
| all 926 engines | 8,081 -> 759 scheduled firings | **16.34** | **4.99** | |
| platform (site egress, storage, factory, DynamoDB, alarms, secrets, DNS, ECR) | | **7.56** | **4.67** | egress estimate after the crawler block + edge copy, to be confirmed from the access logs |
| **account** | | **23.89** (~$726/mo) | **9.66** (~$294/mo) | against the $600 cap |

The two heaviest "engines" were monitoring engines polling AWS APIs ($10 of the $24 a day), not market engines.

## 4. Duplicates and waste (report only - nothing deleted on heuristics)

* **Same job, several triggers** (152 switched off today): e.g. `x-hourly` + `x-daily` rule pairs, a rule plus a
  Scheduler schedule for the same function and payload, a fan-out tick membership plus the function's own rule
  (22 memberships cut; each function now sits in exactly one tick). Switched-off triggers keep a daily expression
  so a State flip cannot bring a 5-minute engine back.
* **Near-duplicate code** (source similarity): `justhodl-top-signals` / `bottom-signals` / `dxy-predict` /
  `macro-predict` / `liq-indicators` (0.83-0.94 pairwise), `justhodl-liquidity-reversal` and `blackswan-watch`
  close behind; `justhodl-email-reports-v2` vs `fmp-stock-picks-agent` (0.77); `justhodl-khalid-metrics` vs
  `justhodl-ka-metrics` (0.64; the khalid-metrics hourly schedule was a duplicate of the daily-morn tick and is
  off). Candidates to merge into one engine with a parameter; cost today is small (each < $0.01/day).
* **43 functions with no trigger and no invocation in 27 days** (idle): aiapi-market-analyzer,
  autonomous-ai-processor, ecb, justhodl-activist-13d, justhodl-ai-council, justhodl-alert-router, justhodl-ask-desk,
  justhodl-asymmetric-scorer, justhodl-biotech-catalysts, justhodl-calibration-snapshot, justhodl-chat-api,
  justhodl-daily-macro-report, justhodl-earnings, justhodl-earnings-transcripts, justhodl-ecb-full-catalog,
  justhodl-email-reports, justhodl-eurostat-history, justhodl-fleet-auditor, justhodl-history-api,
  justhodl-industry-case, justhodl-insider-cluster-scanner, justhodl-portfolio-admin, justhodl-prediction-markets,
  justhodl-prompt-iterator, justhodl-push-api, justhodl-regsho, justhodl-retail-attention, justhodl-screen-backtest,
  justhodl-sentiment-surveys, justhodl-signal-registry-ingest, justhodl-soma-cusip, justhodl-spend-governor-canary
  (keep: the governor's self-test), justhodl-subscribe, justhodl-tape-truth, justhodl-tv-notes-ingest,
  justhodl-usaspending, justhodl-uspto-patents, macro-report-api, multi-agent-orchestrator,
  nyfed-financial-stability-fetcher, nyfed-primary-dealer-fetcher, nyfedapi-isolated, openbb-system2-api.
  They cost nothing while idle (code storage is free under 75 GB); some are API handlers behind REST/HTTP APIs
  that simply had no traffic. Deleting is Khalid's call per function.
* **Other standing waste**: 2 stopped EC2 instances with 16 GB of EBS ($0.04/day); 3 SageMaker endpoint configs
  (the owned coder async endpoint $0.70/day is the only one billing); 173 CloudWatch log groups that never
  expire (19.5 GB stored, ~$0.60/month - set 30-day retention when convenient); the S3 access-log bucket holds
  142 GB / 12M objects (~$3.30/month; a lifecycle rule of 60 days would hold it flat); `justhodl-dashboard-live`
  noncurrent versions are growing again since the lifecycle rule was disabled on Oct 19 (storage $0.64 ->
  $1.13/day in the frozen window).
* **Site egress** ($4.70/day normal, up to $10/day in October): a public bucket read directly by hundreds of pages;
  the crawler is blocked and the Worker now serves large publications from an edge copy; the remaining ~13 GB/day
  of direct browser reads need the pages routed through the Worker (KHALID_ACTIONS.md 0c).

## 5. What Khalid must do

1. **Click "Confirm subscription"** in the AWS e-mail for `justhodl-fleet-alerts` (sent 2026-10-20, expires in 3 days).
   The "ENGINE SHUT DOWN" e-mails go through this topic.
1b. Decide whether `justhodl-event-coordinator` and `justhodl-miss-calibrator` (locked by the $50 cap for
   October's loop) come back on Nov 1: say "release the coordinator" / "release the miss-calibrator" and Claude
   runs `release_cap`; otherwise the event bus stays silent after the thaw.
2. Optional: a new Telegram bot token for `justhodl-guardrail-notify`.
3. Decide on routing the pages' data reads through the Worker (0c) and on re-enabling the noncurrent-version
   lifecycle rule on the live bucket.

## 6. Caveats

* Once a day is a floor on cost and a ceiling on freshness: intraday engines (options flow, news velocity, VIX
  curve, early movers) now publish once a day. An engine that needs more is added to
  `config/cadence-floor.json` `exempt_triggers` with a reason; the governor still caps it.
* Functions with several distinct payloads (ai-brief-router's five briefs, the SDMX walker's four agencies) run
  once per payload per day.
* The engine projections assume event-driven and self-invoked runs stay as they were; the platform projection
  for egress is an estimate until the access logs confirm it (tomorrow's logs).
* The fleet stays frozen until the 2026-11-01 00:07 UTC tick; the freeze-resume workflow re-enables the 11
  scheduled GitHub jobs right after. The first full once-a-day day is Nov 1.

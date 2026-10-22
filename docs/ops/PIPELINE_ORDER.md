# Pipeline order - readers fire after their writers

Khalid, 2026-10-20: "the pipeline-order retime (dry run first, this week)". Dry run ops 6584 clean on 2026-10-21
02:49 UTC; Khalid: "go" (22:56 ET); **applied by ops 6585 at 02:59 UTC** (see the end of this page).

## Why

Since the cadence floor (ops 6554) every engine runs once a day, so WHEN it runs relative to the engines whose
files it reads decides whether a page shows today's numbers or yesterday's. The map (engine-manifest.json reads and
keys, 765 scheduled engines, 2117 reader->writer edges, 229 feedback pairs ignored) showed two faults in the timetable:

* **race** - 22 edges where the reader fires inside its writer's running time (0..margin min after it): it reads the
  previous file or a half-written one, and which one changes from day to day.
* **reads yesterday** - 116 edges where the reader fires in the hour BEFORE its writer: the input is ~24 h old when a
  shift of minutes would make it minutes old.

## Rules (scripts/pipeline_order.py)

* readers move **later only**, at most **60 min**, inside their own session window (post-close 21:00-01:00 UTC,
  overnight 01:00-09:00, pre-market 09:00-14:30, US session 14:30-21:00) plus a 30 min grace past the window end:
  a pre-market engine stays a pre-market engine because its time may encode an external feed the map cannot see.
* margin(writer) = 2 x its mean duration (never more than its timeout; the timeout when no run is on record) + 3 min,
  clamped 5..30 min (ops 6546 account data, 926 functions known).
* fan-out tick members, cadence-floor exempt triggers and non-daily crons are pinned. Members of one tick that read
  other members of the same tick (fired in parallel by `justhodl-scheduler`) move to a **late tick**: a new
  `jhk-tick-<tick>-late` EventBridge Scheduler schedule a quarter hour later (the classic rule bus is at its cap).
* a moved reader is somebody's writer: the solver runs to a fixed point, and a writer whose own readers could not
  follow (pinned or capped) is **kept in place** rather than made to feed them yesterday's file - no edge gets worse
  (0 kept in place, 0 unresolved regressions in this plan).
* at most 4 triggers land on one minute; feedback edges (A reads B, B reads A) are a one-day lag by design.

## The plan (config/pipeline-order-plan.json)

| | before | after |
|---|---|---|
| races | 22 | 0 |
| reads yesterday (within 60 min before the writer) | 116 | 32 |
| input < 1 h old | 176 | 284 |
| mean input age | 10.95 h | 9.96 h |
| median input age | 10.50 h | 9.25 h |

82 triggers move (mean 30.9 min, max 67; 25 of them <= 15 min, 55 16-60 min, 2 > 60); 6 cross a window end by at
most 30 min; by window: overnight 1, post-close 33, pre-market 38, session 10. 3 daily-morn members move to `daily-morn-late` (11:15 UTC):
`justhodl-morning-brief-tg` (reads fed-speak), `justhodl-nobrainer-rationale` (reads asymmetric-hunter, deep-value-screener, eps-revision-velocity), `justhodl-sector-earnings-diffusion` (reads eps-revision-velocity).

### Moves

| window | trigger | move (UTC) | min | fires after |
|---|---|---|---|---|
| overnight | `justhodl-liq-indicators` | 04:10 -> 05:07 | 57 | symbol-dictionary |
| post-close | `justhodl-alerts-digest-close-daily` | 21:00 -> 21:46 | 46 | portfolio-snapshot |
| post-close | `justhodl-signal-logger-6h` | 21:00 -> 22:07 | 67 | ai-brief-router, correlation-breaks, crypto-cot, crypto-etf-flows, crypto-exchange-flows, crypt |
| post-close | `justhodl-regime-conditional-router-2h` | 21:00 -> 21:26 | 26 | eurodollar-stress, master-allocator |
| post-close | `justhodl-forced-selling-bounce-sched` (MON-FRI) | 21:05 -> 22:05 | 60 | breadth-divergence, breadth-thrust, put-call-extreme, vix9d-vix-inversion |
| post-close | `justhodl-corr-break-trades-2h` | 21:15 -> 22:06 | 51 | correlation-breaks |
| post-close | `justhodl-spx-ma-daily` (MON-FRI) | 21:15 -> 21:16 | 1 | market-tape |
| post-close | `risk-regime-sched` | 21:15 -> 22:06 | 51 | breadth-divergence, correlation-breaks, crypto-liquidity, repo-market, settlement-fails, tail-r |
| post-close | `tradingview-native-price-rotation` | 21:15 -> 22:07 | 52 | finviz-universe |
| post-close | `justhodl-katlin-permission-refresh` | 21:17 -> 22:10 | 53 | accumulation-radar, crypto-etf-flows, crypto-exchange-flows, dark-pool, deal-scanner, etf-fund- |
| post-close | `justhodl-master-allocator-3h` | 21:20 -> 21:21 | 1 | eurodollar-stress |
| post-close | `justhodl-readthrough-sched` (MON-FRI) | 21:20 -> 22:11 | 51 | supply-chain-graph, supply-chain-graph |
| post-close | `justhodl-nyfed-pd-weekly` (THU,FRI) | 21:30 -> 21:36 | 6 | settlement-fails |
| post-close | `justhodl-symdir-ustbank` (MON-FRI) | 21:30 -> 22:29 | 59 | finviz-universe, provider-catalog, provider-catalog |
| post-close | `industry-rotation-daily` | 21:35 -> 22:21 | 46 | dark-pool, etf-fund-flows, finviz-groups |
| post-close | `justhodl-provider-catalog-hourly` | 21:38 -> 22:13 | 35 | tradingview |
| post-close | `justhodl-credit-composite-daily` | 21:40 -> 21:42 | 2 | nyfed-pd |
| post-close | `treasury-rehypo-daily` (MON-FRI) | 21:40 -> 21:42 | 2 | nyfed-pd |
| post-close | `justhodl-proven-portfolio-daily` | 21:45 -> 22:01 | 16 | short-book, short-book |
| post-close | `justhodl-short-book-daily` | 21:50 -> 21:56 | 6 | accumulation-radar |
| post-close | `justhodl-supply-chain-graph-daily` | 21:55 -> 22:06 | 11 | finviz-universe |
| post-close | `justhodl-sector-emergence-daily` | 22:00 -> 22:05 | 5 | etf-fund-flows |
| post-close | `justhodl-rebalance-daily` | 22:05 -> 22:35 | 30 | capital-flow-radar |
| post-close | `justhodl-best-setups-hourly` | 22:10 -> 22:53 | 43 | capital-flow-radar, earnings-confluence, resilience, rotation-dashboard, wl-fusion, wl-fusion |
| post-close | `justhodl-phase-detector-daily` | 22:10 -> 22:50 | 40 | resilience |
| post-close | `rotation-dashboard-sched` | 22:10 -> 22:11 | 1 | risk-regime |
| post-close | `macro-confluence-daily` | 22:15 -> 22:53 | 38 | etf-constituents, rebalance-radar |
| post-close | `justhodl-asset-compass-daily` | 22:15 -> 22:50 | 35 | cross-asset-rv |
| post-close | `justhodl-catalyst-chain-daily` | 22:15 -> 22:16 | 1 | readthrough |
| post-close | `justhodl-stock-xray-daily` (MON-FRI) | 22:30 -> 22:58 | 28 | best-setups, best-setups, resilience |
| post-close | `wl-fusion-sched` | 22:35 -> 22:48 | 13 | wl-engines |
| post-close | `justhodl-theme-cascade-backtest-daily` | 22:45 -> 22:53 | 8 | etf-constituents |
| post-close | `justhodl-global-flow-desk-daily` (MON-FRI) | 22:50 -> 23:35 | 45 | money-flow-state |
| post-close | `justhodl-sector-capital-fusion-sched` (MON-FRI) | 22:55 -> 23:35 | 40 | chart-patterns, money-flow-state |
| pre-market | `justhodl-ticker-deep-research-hourly` | 09:05 -> 09:49 | 44 | convergence-radar |
| pre-market | `justhodl-momentum-leaders-hourly` | 09:25 -> 09:49 | 24 | convergence-radar |
| pre-market | `coinbase-premium-sched` | 10:35 -> 10:57 | 22 | cq-feed |
| pre-market | `justhodl-industry-boom-daily` | 10:50 -> 11:35 | 45 | backlog, eps-revision-velocity |
| pre-market | `risk-gate-daily` | 11:05 -> 12:06 | 61 | eurodollar-plumbing, freight-pulse, portwatch, tradingview, yen-carry |
| pre-market | `justhodl-liquidity-inflection-daily` | 11:15 -> 12:06 | 51 | eurodollar-plumbing |
| pre-market | `justhodl-alpha-daily-brief` | 11:30 -> 12:26 | 56 | debate-engine, debate-engine |
| pre-market | `tradingview-vault-daily` | 11:35 -> 11:55 | 20 | cot-feed, symbol-resolver |
| pre-market | `justhodl-confluence-meta-daily` | 11:50 -> 12:50 | 60 | ai-brief-router, alert-backtester, apex-fusion, bottleneck-boom |
| pre-market | `gov-sources-daily` | 11:50 -> 12:01 | 11 | tradingview |
| pre-market | `justhodl-kb-matcher-daily` | 12:00 -> 12:11 | 11 | liquidity-inflection |
| pre-market | `sovereign-stress-daily` | 12:00 -> 12:06 | 6 | eurodollar-plumbing |
| pre-market | `systemic-stress-daily` | 12:00 -> 12:06 | 6 | eurodollar-plumbing |
| pre-market | `justhodl-auction-desk-buyback` (MON-FRI) | 12:10 -> 13:06 | 56 | symdir |
| pre-market | `intraday-pulse-minute` | 12:13 -> 12:21 | 8 | indicator-bus |
| pre-market | `justhodl-debate-engine-daily` | 12:15 -> 12:21 | 6 | indicator-bus |
| pre-market | `euro-fragmentation-daily` | 12:20 -> 13:05 | 45 | cb-injection |
| pre-market | `justhodl-hot-stocks-digest-am` | 12:30 -> 12:54 | 24 | gdelt-buzz |
| pre-market | `justhodl-prepump-alerts-router-30min` | 12:30 -> 13:25 | 55 | polygon-futures-curves, polygon-fx-regime |
| pre-market | `justhodl-carry-surface-sched` | 13:00 -> 13:16 | 16 | polygon-fx-regime |
| pre-market | `justhodl-spx-beaters-weekly` (SAT) | 13:00 -> 13:41 | 41 | share-flows |
| pre-market | `justhodl-symdir-codelists` (MON-FRI) | 13:00 -> 13:46 | 46 | sdmx-walker |
| pre-market | `justhodl-symdir-fredupdates` (MON-FRI) | 13:00 -> 13:46 | 46 | sdmx-walker |
| pre-market | `justhodl-symdir-warm` (MON-FRI) | 13:00 -> 13:46 | 46 | sdmx-walker |
| pre-market | `intraday-arm-daily` (MON-FRI) | 13:25 -> 13:40 | 15 | upside-radar |
| pre-market | `justhodl-flow-confluence-daily` | 13:25 -> 14:20 | 55 | bond-vol, insider-cluster-scanner |
| pre-market | `justhodl-buyback-engine-daily` | 13:30 -> 13:40 | 10 | share-flows |
| pre-market | `justhodl-my-brief-daily` | 13:30 -> 14:21 | 51 | bond-vol |
| pre-market | `justhodl-nowcast-desk-daily` | 13:30 -> 14:21 | 51 | bea-economic-agent, bls-labor-agent |
| pre-market | `convexity-scorer-daily` (MON-FRI) | 14:00 -> 14:50 | 50, crosses | best-ideas |
| pre-market | `justhodl-plumbing-aggregator-hourly` | 14:00 -> 14:21 | 21 | bond-vol |
| pre-market | `justhodl-weekly-ai-review-sunday` (SUN) | 14:00 -> 14:16 | 16 | index-recon |
| pre-market | `premortem-engine-daily` (MON-FRI) | 14:00 -> 14:50 | 50, crosses | best-ideas |
| pre-market | `justhodl-ma-target-predictor-weekly` (SUN) | 14:00 -> 14:08 | 8 | insider-cluster-scanner |
| pre-market | `justhodl-screen-builder-universe-daily` | 14:00 -> 14:08 | 8 | insider-cluster-scanner |
| pre-market | `justhodl-sizing-engine-daily` | 14:10 -> 14:35 | 25, crosses | ai-brief-router |
| pre-market | `ai-rerating-radar-daily` | 14:15 -> 14:36 | 21, crosses | beneish |
| pre-market | `justhodl-narrative-vs-tape-4h` | 14:27 -> 14:35 | 8, crosses | dislocation-detector |
| session | `justhodl-boom-radar-daily` | 14:30 -> 15:25 | 55 | beneish, options-confluence |
| session | `alert-sentinel-daily` | 14:45 -> 15:11 | 26 | research-papers, stock-valuations, stress-index |
| session | `justhodl-crisis-plumbing-refresh` | 14:54 -> 15:13 | 19 | daily-report-v3 |
| session | `jsi-6h` | 15:03 -> 15:05 | 2 | bank-stress |
| session | `justhodl-attention-confluence-daily` | 15:10 -> 16:05 | 55 | 13f-positions, smart-money-cluster |
| session | `crisis-composite-hourly` | 15:15 -> 15:36 | 21 | canary-grid |
| session | `justhodl-bond-desk-daily` (MON-FRI) | 15:15 -> 15:55 | 40 | etf-true-flows |
| session | `justhodl-master-ranker-weekday-close` (MON-FRI) | 16:15 -> 17:05 | 50 | 13f-positions, divergence-engine-v2, future-intelligence, smart-money-cluster |
| session | `justhodl-theme-cascade-twice-per-hour` | 18:20 -> 18:35 | 15 | velocity-acceleration |
| session | `justhodl-symdir-titles` | 20:38 -> 21:05 | 27, crosses | vix-curve |

### Left alone (would need more than 60 min or a window change) - a second pass can take them

| trigger | window | writer (gap now, time it would need) |
|---|---|---|
| `finviz-signals-sched` | post-close | finviz-universe (gap 1380, needs 22:05) |
| `industry-rotation-daily` | post-close | resilience (gap 1416, needs 22:50) |
| `justhodl-alerts-digest-daily` | pre-market | episode-reference (gap 1380, needs 10:05) |
| `justhodl-attention-confluence-daily` | session | smart-money-cluster (gap 1390, needs 17:00); 13f-positions (gap 1424, needs 16:32) |
| `justhodl-auction-desk-buyback` | pre-market | symdir (gap 1400, needs 13:51); symdir (gap 1400, needs 13:51); symdir (gap 1400, needs 13:51) |
| `justhodl-best-setups-hourly` | post-close | insider-sell-cluster (gap 1403, needs 23:35) |
| `justhodl-bond-vol-hourly` | pre-market | daily-report-v3 (gap 1393, needs 15:13) |
| `justhodl-canary-warroom-hourly` | session | factor-regime (gap 1402, needs 21:33) |
| `justhodl-confluence-meta-daily` | pre-market | carry-surface (gap 1414, needs 13:26); vol-surface (gap 1400, needs 13:35) |
| `justhodl-corr-break-trades-2h` | post-close | cross-asset-rv (gap 1401, needs 22:50) |
| `justhodl-flow-confluence-daily` | pre-market | beneish (gap 1431, needs 14:36) |
| `justhodl-forced-selling-bounce-sched` | post-close | market-extremes (gap 1385, needs 23:05) |
| `justhodl-history-snapshotter-daily` | pre-market | insider-cluster-scanner (gap 1395, needs 14:08) |
| `justhodl-industry-boom-daily` | pre-market | estimate-revisions (gap 1415, needs 12:05) |
| `justhodl-katlin-permission-refresh` | post-close | stealth-accumulation (gap 1390, needs 23:05); industry-rotation (gap 1429, needs 22:29); etf-constituents (gap 1405, nee |
| `justhodl-liquidity-inflection-daily` | pre-market | vintage-fred (gap 1386, needs 13:09) |
| `justhodl-nowcast-desk-daily` | pre-market | census-economic-agent (gap 1431, needs 14:35) |
| `justhodl-prepump-alerts-router-30min` | pre-market | catalyst-classifier (gap 1405, needs 14:05); divcut-warning (gap 1391, needs 14:19); insider-cluster-scanner (gap 1415,  |
| `justhodl-signal-logger-6h` | post-close | correlation-breaks (gap 1380, needs 22:05) |
| `justhodl-spx-beaters-weekly` | pre-market | beneish (gap 1392, needs 14:36); earnings-quality (gap 1403, needs 14:23) |
| `justhodl-symdir-titles` | session | finviz-universe (gap 1385, needs 22:05) |
| `khalid-adaptive-sched` | pre-market | daily-report-v3 (gap 1398, needs 15:13) |
| `risk-gate-daily` | pre-market | euro-fragmentation (gap 1381, needs 13:10) |

## Rollout (scripts/pipeline_rollout.py)

1. fresh snapshot of every scheduled rule / schedule (payloads hashed);
2. retimes one item at a time, precondition-checked (`cadence_apply.apply_actions`: live must equal the plan's
   `before`, else "drift" and left alone); the full definition is backed up to
   `s3://justhodl-ops-private-857687956942/backups/pipeline-order/<stamp>/` before the write; read-back after;
   nothing disabled, deleted or moved earlier;
3. the late tick: `jhk-tick-daily-morn-late` (Scheduler, group default, `cron(15 11 * * ? *)`, target
   `justhodl-scheduler` with `{"tick": "daily-morn-late"}`, role `justhodl-scheduler-role` - the dry run proved with
   IAM simulation that it may invoke the scheduler);
4. the S3 copies of `config/fanout-manifest.json` (the three members leave `daily-morn` for `daily-morn-late`) and
   `config/schedule-manifest.json` (82 retimes + the new schedule entry), previous copies backed up first;
5. second snapshot: every planned trigger reads back at its new expression, the late tick is ENABLED, and the cadence
   floor still holds (zero enabled triggers able to fire more than once a day) - or the run fails.

Repo side (same commit as the go): `python3 scripts/pipeline_order.py --apply-repo` rewrites the declared crons in
43 engine `config.json` files, the schedule manifest, the fan-out manifest, and marks the plan `applied`; the deploy
gate (`tests/deployment/test_pipeline_order.py`) then checks the repo declares every planned trigger at its new time.

**Revert**: every changed rule / schedule has its full previous definition under the backup prefix; the plan's
`before` expressions are the previous timetable. A revert ops is the same apply with `before` and `after` swapped.

## Dry run (ops 6584, 2026-10-21 02:39 UTC)

`aws/ops/reports/latest/ops_6584_pipeline_order_dry_run.md`: 1,028 trigger records (588 scheduled rules on the
default bus, 434 schedules), **81 would apply, 0 already, 0 drift, 0 missing, 0 failed**; late tick creatable
(parent `jhk-tick-daily-morn` ENABLED at 11:00, target the scheduler; role allowed); both S3 manifests computed.

## Applied (ops 6585, 2026-10-21 02:59 UTC)

`aws/ops/reports/latest/ops_6585_pipeline_order_apply.md`: 82 retimes applied (canary first, then 81 in parallel,
each precondition-checked, 0 drift, 0 failed), `jhk-tick-daily-morn-late` created (Scheduler, ENABLED, target the
scheduler with `{"tick": "daily-morn-late"}`, role `justhodl-scheduler-role`), both S3 manifests uploaded and equal
to the committed repo files (previous copies backed up), after-snapshot: 83 planned triggers live as planned, 0 not,
0 enabled triggers able to fire more than once a day. Backups:
`s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T025804Z/` (every rule / schedule definition
as it was, `manifest/` the two S3 manifests, `after-snapshot.json`). Repo side committed first (396cad5e): 45 engine
config.json crons, schedule + fan-out manifests, plan marked applied; deploy gate 1,156 tests green.

**First light**: ops 6586 (read-only) checks, from CloudWatch Invocations at 60 s resolution, that every retimed
trigger due so far fired at its new minute and not at the old one, that the late tick invoked its three members after
11:15 UTC (scheduler log `tick=daily-morn-late ok=3`), and that no retimed function errored in its new window.
Scheduled for 12:05 and 23:35 UTC on 2026-10-21; results are appended here.

### First light - results (2026-10-21)

* **12:05 UTC (morning pass)**: 7 of 7 retimed triggers due by then fired at their new minute; late tick
  `daily-morn-late` `ok=3 err=0`, its three members ran after 11:15 UTC.
* **23:35 UTC (evening pass, ops 6586 run 39694431317)**: 75 retimed triggers due; **72 fired at their new minute**;
  3 did not - and those three are the only enabled schedules whose `ScheduleExpressionTimezone` is America/New_York
  (`justhodl-auction-desk-buyback`, `justhodl-bond-warroom-after-auction`, `justhodl-master-ranker-weekday-close`).
  The planner had read their crons as UTC: 12:10 New York (16:10 UTC) was taken for 12:10 UTC and "moved" after a
  13:46 UTC writer it already followed. Each fired once today at the moved New York time (17:51 / 17:56 / 21:05 UTC) -
  after its writers, so no data ran early - and **ops 6603 restored the three original expressions** (12:10, 13:35,
  16:15 New York; timezone, target, role, input, retry untouched; backups under `backups/pipeline-order/20261021T234220Z/`;
  manifest repo + S3 declare the restored times). The plans record the reversal (`reverted` in plan 1 and plan 2).
  **Fix**: `scripts/pipeline_order.py` now converts a schedule's cron through its zone (`tz_offset_min`, `utc_minute`),
  plans in UTC and writes the cron back in the schedule's own zone (`timezone` on every action); ops 6586 checks the
  same way; `tests/deployment/test_pipeline_order.py` pins it. Pass 3 was rebuilt with the fix (still pending the census).
* **Intraday lane**: 12 of 12 members invoked ~96-97 times through the session, 0 errors on 11 of them;
  `justhodl-polygon-futures-curves` invoked 196 times with 98 errors - one failed attempt plus one automatic retry per
  tick (`RuntimeError: Native futures research failed; inspect retained request evidence`, ~0.2 s, 134 MB) - the retry
  succeeds, so the feed is current but every tick costs two runs and 98 logged errors a day. To be read with the full
  traceback at the 2026-10-22 13:20 UTC alarm re-check (engine change = Khalid's go).

## Pass 2 - the Polygon intraday lane (applied by ops 6589, 2026-10-21 04:22 UTC)

Khalid, 2026-10-21 23:56 ET: "Polygon should keep refreshing my data so I get fresh data during market hours Monday
through Friday." `config/intraday-lane.json`:

* **One EventBridge Scheduler schedule** `jhk-tick-session-5min`, `cron(0/5 9-16 ? * MON-FRI *)` in
  **America/New_York** (so it follows the clock change), target `justhodl-scheduler` with `{"tick": "session-5min"}`:
  96 fan-outs a day, 09:00-16:55 ET, Monday-Friday.
* **12 members** - the light Polygon feeds on live snapshot / quote endpoints (0.2-12 s runs): `justhodl-intraday-pulse`
  (built for a 60-second cadence; its market-hours guard is 13:30-20:00 UTC, i.e. EDT - after the clock change it skips
  the last session hour until the engine is taught New York time), `trade-ticket-monitor` (alerts once per level per
  day), `options-gamma` (index dealer gamma), `options-analytics` (per-name live greeks), `opex-calendar` (0DTE
  pinning; alerts only on state transitions), `market-map` (heat map), `risk-regime` (alerts only on a regime shift),
  `tail-risk`, `implied-prob`, `polygon-fx-regime`, `edge-engine`, `polygon-futures-curves`. Each member keeps its
  own daily trigger (the pre-market / post-close consolidation run). Not in the lane, with reasons in the config:
  dealer-gex, market-internals, polygon-options-flow (heavy end-of-day research jobs), tape-reader, options-flow-scanner,
  money-flow-state, crypto-etf-flows, portfolio-snapshot (end-of-day data), vol-regime (FRED).
* **Guardrails**: exempt from the once-a-day floor by name in `config/cadence-floor.json` (deploy gate) and in the
  governor's `control.json` `cadence_exempt` (the daily cadence sentinel; previous copy backed up); the $50/engine cap
  and the anomaly sentinel (200 invocations or $1 per hourly window; the lane is 12 invocations and ~$0.005 per
  window per engine) apply unchanged. Cost ~$1-2 of Lambda a month for the whole lane. Latency: the stocks tape is
  15-minute delayed on the current Massive/Polygon plan; real-time needs the Advanced tier ($199/month).
* The pass also carried 16 second-iteration order moves (readers that pass 1 could not bring all the way within an
  hour of their original time; 14-60 min): `industry-rotation-daily` 22:21 -> 22:50, `justhodl-attention-confluence-daily` 16:05 -> 17:01, `justhodl-best-setups-hourly` 22:53 -> 23:35, `justhodl-confluence-meta-daily` 12:50 -> 13:35, `justhodl-flow-confluence-daily` 14:20 -> 14:36, `justhodl-forced-selling-bounce-sched` 22:05 -> 23:05, `justhodl-nowcast-desk-daily` 14:21 -> 14:35, `justhodl-prepump-alerts-router-30min` 13:25 -> 14:19, `justhodl-stock-xray-daily` 22:58 -> 23:40, `justhodl-auction-desk-buyback` 13:06 -> 13:51, `justhodl-bond-warroom-after-auction` 13:35 -> 13:56, `justhodl-corr-break-trades-2h` 22:06 -> 22:50, `justhodl-industry-boom-daily` 11:35 -> 12:05, `justhodl-katlin-permission-refresh` 22:10 -> 23:05, `justhodl-spx-beaters-weekly` 13:41 -> 14:36, `justhodl-symdir-titles` 21:05 -> 22:05.
* Apply report `aws/ops/reports/latest/ops_6589_pipeline_order_pass2_apply.md`: 16 retimed, lane schedule created and
  ENABLED, control.json exemption added and read back, both S3 manifests equal the repo, 18 planned triggers live as
  planned, 0 cadence violators. Backups `s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261021T042127Z/`.
  First light: ops 6586 at 23:35 UTC checks every member was invoked ~96 times through the session with no errors.

## Pass 3 - providers as pinned writers + no bursts (census 2026-10-22 01:36 UTC; dry run ops 6604 clean 01:44 UTC; APPLIED by ops 6605 at 01:51 UTC)

Khalid, 2026-10-21: "the biggest freshness win costs nothing ... treat each provider as a pinned writer at its
publication time - dry run first, after a one-off census confirms the arithmetic" and "be careful not to pull all
FRED and ECB data at once and other gov so that you don't get blocked by API rate limit."

`config/provider-publication.json` holds the publication times (winter UTC, so they hold all year) and the per-provider
slot caps. The FRED pullers are split by what they pull (`scripts/provider_map.py` reads every engine's source):
**162 engines pull at least one daily H.15 series** (rates, yields, FX, commodities) - they are freshest in the US
morning, where yesterday's close is in, so only the evening print (21:15 UTC) aligns them; **21 engines pull only
macro releases** - they move after 08:30 ET (13:30 UTC + 30 min). Moving a mixed engine from 09:00 to 14:00 would have
made its rates a day older every morning to make its releases fresher a few days a month, so it is not done.

**The census (ops 6587, re-run 2026-10-22 01:36 UTC, after every daily engine had run once since the thaw)** read each
provider-fed output's own data date against its engine's trigger minute: files a day old or more at their engine's run -
FRED 43/101, FMP 46/146, Polygon 29/73, ECB 8/19, SEC 5/18, NY Fed 8/20, Cboe 5/17 (CFTC's weekly files a median 8 days,
as expected) - and every cluster sits before its provider's print, which is the arithmetic the plan rests on. One
correction came out of the per-file review: Cboe's VIX term-structure files were still on the previous day at 17:00 ET
(21:00 UTC), so the Cboe print is now taken as 17:30 ET (22:30 UTC) and `vix-curve-sched` moves to 23:01 UTC rather
than 22:01. The plan was then rebuilt timezone-aware (the first-light finding: the three America/New_York crons are
read in UTC and written back in their own zone, `scripts/pipeline_order.py` `utc_minute`); `--check` matches the configs.

Computed from the state after pass 2 (`config/pipeline-order-plan-3.json`):

| | before | after |
|---|---|---|
| pullers firing before the latest print inside their window | 51 | 2 (`justhodl-corp-bonds-daily` 20:15 and `justhodl-margin-lending-weekday-16` 16:00: session-window FINRA readers; FINRA prints at 23:00, outside their window, so they read the previous evening's files by design) |
| busiest minute, FMP / FRED / ECB / Polygon / SEC | 19 / 5 / 4 / 7 / 3 | 4 / 2 / 2 / 4 / 2 (= the caps) |
| minutes over a provider's cap | 36 | 0 |
| stale reader -> writer edges | 22 | 19 |
| triggers moved | | 135 (37 to catch a print, 55 to spread a burst, 34 readers following a writer, 7 order+print, 2 burst+order) |
| move size | | mean 33 min, max 303 (two macro-only FRED pullers from 09:00 to 14:03); 1 move crosses its window end (`justhodl-confluence-meta-daily` 13:35 -> 14:35, pre-market into session) |
| daily-morn | 23 members at 11:00, 3 at 11:15 (daily-morn-late, from pass 1) | 17 at 11:00, 6 at 11:15, 3 at 11:30 (daily-morn-late2, new); no late tick starts more than 4 FMP pullers |
| New York-zone crons (ET) | | `justhodl-auction-desk-buyback` 12:10 -> 14:30 (after Treasury's print), `justhodl-bond-warroom-after-auction` 13:35 -> 14:35, `justhodl-master-ranker-weekday-close` 16:15 -> 16:40 |
| left alone | | 12 triggers (would need a window change or more than the cap): `finviz-signals-sched` (finviz-universe would need 22:05); `intraday-arm-daily` (stock-valuations would need 14:55); `justhodl-alerts-digest-daily` (episode-reference would need 10:05); `justhodl-bond-vol-hourly` (daily-report-v3 would need 15:13); `justhodl-boom-stage-daily` (china-liquidity would need 14:35); `justhodl-canary-warroom-hourly` (factor-regime would need 21:33); `justhodl-history-snapshotter-daily` (insider-cluster-scanner would need 14:08); `justhodl-liquidity-inflection-daily` (vintage-fred would need 13:11); `justhodl-master-allocator-3h` (asset-compass would need 22:55, gold-equity-rotation would need 22:50); `justhodl-master-ranker-weekday-close` (sector-flow-state would need 21:20, squeeze-fuel would need 21:46, compound-aggregator would need 21:20); `justhodl-prepump-alerts-router-30min` (ticket-ai-rationale would need 15:10); `risk-gate-daily` (euro-fragmentation would need 13:10) |

**Dry run (ops 6604, 2026-10-22 01:44 UTC, `aws/ops/reports/latest/ops_6604_pipeline_order_pass3_dry_run.md`):** 588
rules on the default bus, 438 schedules, 1,032 records, 0 enabled triggers able to fire more than once a day; 135
retimes ready, 0 drift, 0 missing; late ticks: daily-morn-late present at 11:15, daily-morn-late2 creatable at 11:30
(parent tick present, invoke role proven); the lane schedule and its cadence exemption already in place; both S3
manifests would change (7 fan-out members move, 135 entries retimed, 1 schedule entry added). Applied at ~01:50 UTC,
exactly one trigger fires twice on the day of the apply (`justhodl-cusip-map-rebuild-hourly`: 01:28 had run, 03:30
runs again after the SEC print) and `justhodl-stock-xray-daily` skips its 23:40 run for the 00:35 one the next morning.

**Applied (ops 6605, 2026-10-22 01:51 UTC, `aws/ops/reports/latest/ops_6605_pipeline_order_pass3_apply.md`):** 135
triggers retimed (one canary first, then 134; every definition backed up to
`s3://justhodl-ops-private-857687956942/backups/pipeline-order/20261022T015100Z/` before the write, read back after);
`jhk-tick-daily-morn-late2` created ENABLED at 11:30 UTC on the proven invoke role (daily-morn-late already in place);
the lane schedule and its cadence exemption untouched; `config/fanout-manifest.json` and `config/schedule-manifest.json`
on S3 now equal the committed files (previous copies backed up). Verification snapshot: 588 rules, 439 schedules, 138
planned triggers live as planned (135 retimes + 2 late ticks + the lane), 0 not as planned, 0 enabled triggers able to
fire more than once a day. First light: the morning half (03:30 SEC-print moves, the 11:00-13:02 burst spreads, the two
late ticks) is read by the 13:20 UTC check with `ops_6586_pipeline_order_first_light.py` (it reads every applied plan);
the evening half (post-close moves, the three New York-zone schedules at 14:30 / 14:35 / 16:40 ET) at 23:58 UTC.

### Pass 3 moves

| window | trigger | move (UTC; ET where the cron is New York time) | min | why | after |
|---|---|---|---|---|---|
| overnight | `justhodl-cusip-map-rebuild-hourly` | 01:28 -> 03:30 | 122 | print | SEC EDGAR's print |
| overnight | `justhodl-sec-13f-daily` | 01:52 -> 03:31 | 99 | print | SEC EDGAR's print |
| pre-market | `justhodl-official-pulse-weekly` | 09:00 -> 14:03 | 303 | print | FRED macro's print |
| pre-market | `justhodl-provider-window-sentinel-weekly` (SUN) | 09:00 -> 14:03 | 303 | print | FRED macro's print |
| pre-market | `justhodl-bis-crossborder-weekly` (MON) | 09:40 -> 10:31 | 51 | print | BIS/IMF/WB/OECD's print |
| pre-market | `justhodl-asia-leads-daily` | 10:20 -> 14:01 | 221 | print | FRED macro's print |
| pre-market | `construction-housing-daily` | 11:00 -> 11:01 | 1 | burst | FMP (burst) |
| pre-market | `ecb-detail-daily` | 11:00 -> 11:01 | 1 | burst | FMP (burst) |
| pre-market | `forward-orders-daily` | 11:00 -> 11:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-econ-calendar-daily` | 11:00 -> 11:03 | 3 | burst | FMP (burst) |
| pre-market | `justhodl-edge-discovery-weekly` (SUN) | 11:00 -> 11:03 | 3 | burst | Polygon/Massive (burst) |
| pre-market | `justhodl-opex-calendar-daily` | 11:00 -> 11:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-political-intel-daily` | 11:00 -> 11:02 | 2 | burst | FMP (burst) |
| pre-market | `justhodl-research-backtest-daily` | 11:00 -> 11:02 | 2 | burst | FMP (burst) |
| pre-market | `macro-calendar-daily` | 11:00 -> 11:02 | 2 | burst | FMP (burst) |
| pre-market | `news-wire-15m` | 11:00 -> 11:02 | 2 | burst | FMP (burst) |
| pre-market | `smart-money-13f-weekly` (MON) | 11:00 -> 11:03 | 3 | burst | SEC EDGAR (burst) |
| pre-market | `justhodl-warm-bridge-hourly` | 11:27 -> 14:06 | 159 | order+print | usgov-direct, BEA's print, BLS's print, NY Fed's print |
| pre-market | `justhodl-blackout-daily` | 11:30 -> 11:31 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-earnings-tracker-6h` | 11:30 -> 11:31 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-crisis-canaries-daily` | 11:40 -> 13:32 | 112 | print | NY Fed's print |
| pre-market | `justhodl-freight-pulse-daily` | 11:50 -> 14:02 | 132 | print | FRED macro's print |
| pre-market | `symbol-resolver-daily` | 11:50 -> 14:06 | 136 | print | FRED macro's print |
| pre-market | `eurodollar-plumbing-daily` | 12:00 -> 12:02 | 2 | burst | FRED (burst) |
| pre-market | `justhodl-estimate-revisions-daily` | 12:00 -> 12:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-gbc-daily` | 12:00 -> 12:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-sec-filing-diff-daily` | 12:00 -> 12:01 | 1 | burst | SEC EDGAR (burst) |
| pre-market | `yen-carry-daily` | 12:00 -> 12:02 | 2 | burst | FMP (burst) |
| pre-market | `justhodl-industry-boom-daily` | 12:05 -> 12:06 | 1 | order | estimate-revisions |
| pre-market | `macro-attribution-daily` | 12:05 -> 14:04 | 119 | print | FRED macro's print |
| pre-market | `justhodl-liquidity-inflection-daily` | 12:06 -> 12:08 | 2 | order | eurodollar-plumbing |
| pre-market | `risk-gate-daily` | 12:06 -> 12:09 | 3 | burst+order | eurodollar-plumbing, FRED (burst) |
| pre-market | `sovereign-stress-daily` | 12:06 -> 12:08 | 2 | order | eurodollar-plumbing |
| pre-market | `systemic-stress-daily` | 12:06 -> 12:08 | 2 | order | eurodollar-plumbing |
| pre-market | `justhodl-kb-matcher-daily` | 12:11 -> 12:13 | 2 | order | liquidity-inflection |
| pre-market | `indicator-bus-daily` | 12:15 -> 14:01 | 106 | print | FRED macro's print |
| pre-market | `justhodl-boom-stage-daily` | 12:30 -> 14:07 | 97 | order+print | asia-leads, freight-pulse, FRED macro's print |
| pre-market | `fed-collateral-daily` | 12:40 -> 13:31 | 51 | print | NY Fed's print |
| pre-market | `justhodl-usgov-direct-daily` | 12:40 -> 14:01 | 81 | print | BEA's print, BLS's print |
| pre-market | `justhodl-bottleneck-boom-daily` | 12:45 -> 13:31 | 46 | print | NY Fed's print |
| pre-market | `justhodl-trade-nowcast-daily` | 12:50 -> 14:04 | 74 | print | FRED macro's print |
| pre-market | `fundamentals-engine-sched` (MON-FRI) | 13:00 -> 13:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-index-inclusion-weekly` (MON) | 13:00 -> 13:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-peer-comparison-daily` | 13:00 -> 13:01 | 1 | burst | FMP (burst) |
| pre-market | `justhodl-vintage-fred-daily` | 13:00 -> 13:02 | 2 | burst | FRED (burst) |
| pre-market | `leadlag-graph-daily` | 13:00 -> 13:01 | 1 | burst | FMP (burst) |
| pre-market | `fx-intelligence-sched` | 13:30 -> 13:31 | 1 | burst | FRED (burst) |
| pre-market | `justhodl-confluence-meta-daily` | 13:35 -> 14:35 | 60, crosses | order | ai-brief-router, bottleneck-boom |
| pre-market | `intraday-arm-daily` (MON-FRI) | 13:40 -> 14:06 | 26 | order | indicator-bus |
| pre-market | `justhodl-import-canary-daily` | 13:40 -> 14:01 | 21 | print | Census's print |
| pre-market | `justhodl-symdir-codelists` (MON-FRI) | 13:46 -> 14:11 | 25 | order+print | symbol-resolver, BLS's print |
| pre-market | `justhodl-symdir-fredupdates` (MON-FRI) | 13:46 -> 14:12 | 26 | order+print | symbol-resolver, BLS's print |
| pre-market | `justhodl-symdir-warm` (MON-FRI) | 13:46 -> 14:13 | 27 | order+print | symbol-resolver, BLS's print |
| pre-market | `earnings-nlp-sched` (MON-FRI) | 14:00 -> 14:05 | 5 | burst | FMP (burst) |
| pre-market | `opportunity-engine-daily` | 14:00 -> 14:03 | 3 | burst | FMP (burst) |
| pre-market | `sellside-views-mwf` (MON,WED,FRI) | 14:00 -> 14:04 | 4 | burst | FMP (burst) |
| pre-market | `theme-second-wave-daily` | 14:00 -> 14:05 | 5 | burst | FMP (burst) |
| session | `justhodl-rv-iv-scanner-daily` | 14:30 -> 14:31 | 1 | burst | FRED (burst) |
| session | `justhodl-smart-money-holdings-daily` | 14:30 -> 14:31 | 1 | burst | FMP (burst) |
| session | `justhodl-ecb-derived-daily` | 14:40 -> 15:30 | 50 | print | ECB's print |
| session | `justhodl-supply-chain-linkage-daily` | 15:00 -> 15:01 | 1 | burst | FMP (burst) |
| session | `justhodl-theme-classifier-6h` | 15:00 -> 15:01 | 1 | burst | FMP (burst) |
| session | `justhodl-report-research-hourly` | 15:02 -> 15:31 | 29 | print | ECB's print |
| session | `justhodl-crisis-plumbing-refresh` | 15:13 -> 15:42 | 29 | order | daily-report-v3 |
| session | `brief-compiler-plumbing-6h` | 15:20 -> 15:43 | 23 | order | daily-report-v3 |
| session | `stress-scenarios-sched` | 15:25 -> 15:42 | 17 | order | daily-report-v3 |
| session | `crisis-composite-hourly` | 15:36 -> 15:47 | 11 | order | crisis-plumbing, daily-report-v3 |
| session | `capitulation-3h` | 15:45 -> 15:52 | 7 | order | crisis-composite, crisis-composite |
| session | `auction-grader-sched` (MON-FRI) | 16:00 -> 16:15 | 15 | order | auction-desk |
| session | `justhodl-signal-backtest-daily` | 16:00 -> 16:01 | 1 | burst | FMP (burst) |
| session | `justhodl-auction-desk-buyback` (MON-FRI) | 12:10 -> 14:30 ET | 140 | print | Treasury's print |
| session | `divergence-v2-2hourly` | 17:00 -> 17:01 | 1 | burst | FRED (burst) |
| session | `justhodl-bond-warroom-after-auction` (MON-FRI) | 13:35 -> 14:35 ET | 60 | order+print | auction-desk, Treasury's print |
| session | `justhodl-master-ranker-weekday-close` (MON-FRI) | 16:15 -> 16:40 ET | 25 | order | cross-asset-regime |
| post-close | `bond-trace-daily` (MON-FRI) | 21:00 -> 23:31 | 151 | print | FINRA's print, FRED's print |
| post-close | `cds-proxy-tri-daily` (MON-FRI) | 21:00 -> 21:46 | 46 | print | FRED's print |
| post-close | `commodity-curves-sched` (MON-FRI) | 21:00 -> 21:48 | 48 | print | FRED's print |
| post-close | `vix-curve-sched` (MON-FRI) | 21:00 -> 23:01 | 121 | print | Cboe's print |
| post-close | `justhodl-market-tape-5min` | 21:11 -> 21:51 | 40 | print | FRED's print |
| post-close | `justhodl-put-call-extreme-daily` | 21:15 -> 21:52 | 37 | print | FRED's print |
| post-close | `justhodl-eurodollar-stress-2h` | 21:16 -> 21:48 | 32 | print | FRED's print |
| post-close | `justhodl-spx-ma-daily` (MON-FRI) | 21:16 -> 21:56 | 40 | order | market-tape |
| post-close | `justhodl-fifx-vol-daily` (MON-FRI) | 21:20 -> 21:51 | 31 | print | FRED's print |
| post-close | `justhodl-master-allocator-3h` | 21:21 -> 21:53 | 32 | order | eurodollar-stress |
| post-close | `justhodl-regime-conditional-router-2h` | 21:26 -> 21:58 | 32 | order | eurodollar-stress, master-allocator |
| post-close | `justhodl-breadth-thrust-sched` | 21:30 -> 21:47 | 17 | print | FRED's print |
| post-close | `justhodl-vix9d-vix-inversion-daily` | 21:30 -> 21:31 | 1 | burst | FMP (burst) |
| post-close | `fomc-reaction-daily` | 21:35 -> 21:46 | 11 | print | FRED's print |
| post-close | `justhodl-repo-market-daily` | 21:35 -> 21:52 | 17 | print | FRED's print |
| post-close | `justhodl-dark-pool-daily` (MON-FRI) | 21:40 -> 23:31 | 111 | print | FINRA's print |
| post-close | `justhodl-risk-ratios-daily` | 21:40 -> 21:53 | 13 | print | FRED's print |
| post-close | `justhodl-edge-6h` | 21:41 -> 21:47 | 6 | print | FRED's print |
| post-close | `justhodl-credit-composite-daily` | 21:42 -> 21:49 | 7 | print | FRED's print |
| post-close | `treasury-rehypo-daily` (MON-FRI) | 21:42 -> 23:32 | 110 | print | FRED's print, OFR's print |
| post-close | `justhodl-sp500-daily` (MON-FRI) | 21:45 -> 22:03 | 18 | order | spx-ma |
| post-close | `buzz-velocity-4x-daily` | 22:00 -> 22:01 | 1 | burst | FMP (burst) |
| post-close | `justhodl-52wk-quality-breakout-daily` | 22:00 -> 22:02 | 2 | burst | FMP (burst) |
| post-close | `justhodl-backtest-harness-daily` | 22:00 -> 22:01 | 1 | burst | FMP (burst) |
| post-close | `justhodl-breadth-divergence-daily` | 22:00 -> 22:02 | 2 | burst | FMP (burst) |
| post-close | `justhodl-cot-tracker-weekly` (FRI) | 22:00 -> 22:01 | 1 | burst | FMP (burst) |
| post-close | `justhodl-earnings-iv-crush-daily` | 22:00 -> 22:02 | 2 | burst | FMP (burst) |
| post-close | `justhodl-gf-value-daily` | 22:00 -> 22:02 | 2 | burst | FMP (burst) |
| post-close | `justhodl-lockup-expiration-daily` | 22:00 -> 22:03 | 3 | burst | FMP (burst) |
| post-close | `justhodl-vvix-vov-regime-daily` | 22:00 -> 22:03 | 3 | burst | FMP (burst) |
| post-close | `liquidity-profile-fri` (FRI) | 22:00 -> 22:03 | 3 | burst | Polygon/Massive (burst) |
| post-close | `justhodl-symdir-titles` | 22:05 -> 23:32 | 87 | order+print | provider-catalog, OFR's print |
| post-close | `risk-regime-sched` | 22:06 -> 22:08 | 2 | order | breadth-divergence, vvix-vov-regime |
| post-close | `rotation-dashboard-sched` | 22:11 -> 22:13 | 2 | order | risk-regime |
| post-close | `justhodl-crisis-knowledge-base-cadence` (MON-FRI) | 22:20 -> 23:06 | 46 | order | vix-curve |
| post-close | `justhodl-symdir-ustbank` (MON-FRI) | 22:29 -> 23:33 | 64 | print | OFR's print |
| post-close | `justhodl-cef-discount-daily` | 22:30 -> 22:31 | 1 | burst | FMP (burst) |
| post-close | `justhodl-event-study-cadence` (MON-FRI) | 22:30 -> 22:32 | 2 | burst | FRED (burst) |
| post-close | `justhodl-failed-pattern-reversal-daily` | 22:30 -> 22:31 | 1 | burst | FMP (burst) |
| post-close | `justhodl-flows-ai-analysis-daily` | 22:30 -> 23:11 | 41 | order | crisis-knowledge-base |
| post-close | `justhodl-ipo-pipeline-daily` | 22:30 -> 22:31 | 1 | burst | FMP (burst) |
| post-close | `justhodl-skew-tail-hedging-daily` | 22:30 -> 22:31 | 1 | burst | FMP (burst) |
| post-close | `justhodl-vrp-daily` | 22:30 -> 23:06 | 36 | burst+order | vix-curve, FMP (burst) |
| post-close | `justhodl-symdir-fredfresh` | 22:44 -> 23:31 | 47 | print | OFR's print |
| post-close | `justhodl-resilience-daily` (MON-FRI) | 22:45 -> 23:37 | 52 | order | dark-pool |
| post-close | `industry-rotation-daily` | 22:50 -> 23:42 | 52 | order | dark-pool, resilience |
| post-close | `justhodl-phase-detector-daily` | 22:50 -> 23:42 | 52 | order | dark-pool, resilience |
| post-close | `justhodl-market-extremes-daily` | 23:00 -> 23:11 | 11 | order | vrp |
| post-close | `justhodl-forced-selling-bounce-sched` (MON-FRI) | 23:05 -> 23:56 | 51 | order | predictability |
| post-close | `justhodl-katlin-permission-refresh` | 23:05 -> 23:51 | 46 | order | dark-pool, industry-rotation, sympathetic-momentum, sympathetic-momentum |
| post-close | `justhodl-pairs-arb-daily` | 23:30 -> 23:31 | 1 | burst | FMP (burst) |
| post-close | `justhodl-rating-change-cluster-daily` | 23:30 -> 23:32 | 2 | burst | FMP (burst) |
| post-close | `justhodl-squeeze-pretrigger-daily` | 23:30 -> 23:32 | 2 | burst | FMP (burst) |
| post-close | `justhodl-starmine-daily` | 23:30 -> 23:33 | 3 | burst | FMP (burst) |
| post-close | `justhodl-sympathetic-momentum-daily` | 23:30 -> 23:33 | 3 | burst | FMP (burst) |
| post-close | `justhodl-best-setups-hourly` | 23:35 -> 23:42 | 7 | order | resilience |
| post-close | `justhodl-global-flow-desk-daily` (MON-FRI) | 23:35 -> 23:37 | 2 | order | dark-pool |
| post-close | `justhodl-sector-capital-fusion-sched` (MON-FRI) | 23:35 -> 23:37 | 2 | order | dark-pool |
| post-close | `justhodl-stock-xray-daily` (MON-FRI) | 23:40 -> 00:35 | 55 | order | equity-confluence, resilience |
| post-close | `conviction-engine-3h` | 23:45 -> 23:47 | 2 | order | best-setups |
| post-close | `alpha-compass-3h` | 23:50 -> 23:52 | 2 | order | conviction-engine |

Late ticks after pass 3: daily-morn-late 11:15 = justhodl-khalid-metrics, justhodl-momentum-breakout, justhodl-morning-brief-tg, justhodl-nobrainer-rationale, justhodl-pre-pump-detector, justhodl-revenue-acceleration; daily-morn-late2 11:30 = justhodl-sector-earnings-diffusion, justhodl-theme-rotation-engine, justhodl-volatility-squeeze-hunter.

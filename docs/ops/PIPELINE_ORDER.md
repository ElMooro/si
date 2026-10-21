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

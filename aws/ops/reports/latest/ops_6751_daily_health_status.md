# ops 6751 - daily health status 2026-11-05 (governor status + caps, event records since ops 6727, relay lag, output freshness, FMP bandwidth hold, follow-up probe of the silent engines, source-refresh control; read-only)

**Status:** success  
**Duration:** 30.9s  
**Finished:** 2026-11-05T11:22:28+00:00  

## Data

| 13f-book_age_h | _health/cadence-manifest.json | _health/fleet.json | anomaly_held | baseline_usd_day | budget_actual | budget_actual_now | budget_forecast | budget_last_updated | budget_limit | cap | cap_source | capital_structure_control | capped | data/_freshness-monitor.json | engines_fresh | engines_missed_two_slots | engines_not_back_since_restore | events_since_6727 | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | leaders_age_h | meter_top | mode | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | probe_FAILING | probe_ON_TIME | probe_PARTLY_FAILING | probe_RAN | probe_RAN_NO_WRITE | probe_broken_count | projected_eom | recovered_since_yesterday | relay_alert_records_unrelayed | stage | stage_since | storage_day | storage_last_gb | storage_last_level | strength_age_h | version | versioning |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 13.8 | 3.2 | 8.9 | 0 | 9.68 | 50.703 | 50.703 | 251.416 | 2026-11-05 10:53:19.884000+00:00 | 600.0 | 600.0 | budget | failed since 2026-10-08T22:00:40.571580+00:00 (phase part-2) | 3 | 3.4 | 663 | 23 | 19 | {} | held until 2026-11-06T07:09+00:00 | clear | 3 | 2.8 | symdir $0.61; polygon-bank $0.51; eia-bank $0.32; bank-histories $0.23; research-backtest $0.17 | enforce | 50.7 | 2 | 212 | 1055 | 25 | 48 | 1340 | chart-patterns, earnings-blackout, fmp-ratios, momentum-breakout, predictability, revenue-acceleration, volatility-squeeze-hunter | base-rates, beaters-grader, cftc-deep-view, liquidity-profile, methodology-scout, naaim, spx-beaters | ecb-derived, eps-revision-velocity, systemic-stress | crypto-emergence | cross-asset-regime, earnings-iv-crush, earnings-quality, forward-orders, gdelt-sentiment, gf-value, gold-equity-rotation, leading-markets, portwatch, russell-recon-frontrun | 20 | 307.39 | crypto-emergence | 0 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 2026-11-04 | 1165.295 | None | 7.7 | 1.6.1 | None |

## Log
## A. governor status

- `11:21:59` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:21:59` cap $600.0 (budget) | budget limit $600.0 actual $50.703 forecast $251.416
- `11:21:59` thresholds: {"baseline_usd_day": 9.68, "brake_at": 530.43, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 25.53, "freeze_at": 570.43, "freeze_line_floored": false, "frozen_eom": 70.27, "lag_buffer": 9.68, "mtd": 50.7, "projected_eom": 307.39, "reserve": 29.57, "residual_usd_day": 0.39, "sources": {"budget_actual": 50.703, "ce_mtd": 48.3462}}
- `11:21:59` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:21:59` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:21:59` anomaly held (ANOM): none
- `11:21:59` fuse: {"checked_at": "2026-11-05T11:07:37.965323+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 2.87, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-bank-histories", 0.94], ["justhodl-fmp-walker", 0.54], ["justhodl-polygon-bank", 0.45], ["justhodl-eia-bank", 0.32], ["justhodl-fmp-bank", 0.25], ["justhodl-provider-bank", 0.1], ["justhodl-cycle-features", 0.06], ["justhodl-cq-feed", 0.05]]}
- `11:21:59` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 27 functions ran, fleet $2.87/day; month-to-date top: justhodl-symdir $0.61, justhodl-polygon-bank $0.51, justhodl-eia-bank $0.31"], "mode": "enforce", "mtd": 50.7, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 9.68, "brake_at": 530.43, "budget_forecast": 251.416, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 25.54, "freeze_at": 570.43, "freeze_line_floored": false, "frozen_eom": 70.27, "lag_buffer": 9.68, "mtd": 50.7, "projected_eom": 307.49, "reserve": 29.57, "residual_source": "min7", "residual_usd_day": 0.39, "sources": {"budget_actua
- `11:21:59` cadence sentinel: {"checked": 841, "checked_at": "2026-11-05T00:07:37.805036+00:00", "day": "2026-11-05", "errors": [], "violators": []} | fixed entries: 0
- `11:21:59` anomalies: {"errors_notified": {"day": "2026-10-22", "fns": ["justhodl-polygon-futures-curves"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:21:59` meter: {"active": 27, "daily": {"2026-11-02": {"alphavantage-market-agent": [2.0, 2e-05], "alphavantage-technical-analysis": [1.0, 1e-06], "bea-economic-agent": [3.0, 1.3e-05], "benzinga-news-agent": [4.0, 3.4e-05], "bls-employment-api-v2": [1.0, 3.7e-05], "bls-labor-agent": [1.0, 5.3e-05], "bond-indices-agent": [2.0, 6.1e-05], "census-economic-agent": [1.0, 2.6e-05], "cftc-futures-positioning-agent": [4.0, 0.001412], "chatgpt-agent-api": [1.0, 0.0], "coinmarketcap-agent": [1.0, 7e-06], "dollar-strength-agent": [3.0, 0.000141], "enhanced-repo-agent": [2.0, 9e-06], "fedliquidityapi": [6.0, 0.000112], "fmp-fundamentals-agent": [6.0, 4.9e-05], "fmp-stock-picks-agent": [1.0, 0.000285], "fred-ice-bofa-a
- `11:21:59` storage: day 2026-11-04 | versioning None | warn_days None | trip_days None
- `11:21:59` storage last assessment: {"day": "2026-11-03", "delta_gb": 7.11, "delta_objects": 486651, "delta_pct": 0.61, "gb": 1165.295, "level": null, "objects": 7661545, "prev": "2026-11-02", "prev_gb": 1158.181, "reason": "growth +7.1 GB (+0.6%), +486651 objects over 2026-11-03"}
- `11:21:59` S3 $/day (storage.s3_usd): {"2026-10-21": 18.9137, "2026-10-22": 29.4721, "2026-10-23": 6.7537, "2026-10-24": 2.7799, "2026-10-25": 3.452, "2026-10-26": 9.2012, "2026-10-27": 5.2497, "2026-10-28": 7.394, "2026-10-29": 7.2171, "2026-10-30": 7.6549, "2026-10-31": 7.0416, "2026-11-01": 1.6878, "2026-11-02": 2.3964, "2026-11-03": 7.0606}
- `11:21:59` service spend 2026-10-31: total $9.56 | Amazon Simple Storage Service $7.04, AWS Lambda $1.63, AmazonCloudWatch $0.47, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.07
- `11:21:59` service spend 2026-11-01: total $32.61 | AWS Business Support+ $29.00, Amazon Simple Storage Service $1.69, Amazon Route 53 $1.50, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.06
- `11:21:59` service spend 2026-11-02: total $2.92 | Amazon Simple Storage Service $2.40, AWS Secrets Manager $0.12, AmazonCloudWatch $0.10, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.08, Amazon DynamoDB $0.05
- `11:21:59` service spend 2026-11-03: total $7.80 | Amazon Simple Storage Service $7.06, AmazonCloudWatch $0.30, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.07, AWS Cost Explorer $0.07
## B. meter: month-to-date top engines

- `11:21:59` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:21:59` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:21:59` month-to-date top 25: symdir $0.61, polygon-bank $0.51, eia-bank $0.32, bank-histories $0.23, research-backtest $0.17, fmp-walker $0.12, causality-scanner $0.12, fmp-bank $0.11, polygon-options-flow $0.09, feed-catalog $0.08, thesis-engine $0.08, data-census $0.08, predictability $0.07, signal-genealogy $0.07, bottom $0.07, flow-lookthrough $0.07, wl-engines $0.07, katlin $0.06, etf-true-flows $0.06, etf-constituents $0.06
## C. governor event records since ops 6727 (2026-11-04 11:19 UTC), the raw budget read, the relay's marker

- `11:21:59` 0 event records on Nov 4 + Nov 5; 0 since ops 6727 (newest -)
- `11:21:59` no alert-class event records since yesterday's run (only bookkeeping kinds, if any)
- `11:21:59` relay marker: {"last_key": "spend-governor/events/20261103T063334.823234Z-release-cap.json", "updated": "2026-11-03T06:34:08.398286Z", "issues_opened": 3}
- `11:21:59` relay is behind the newest record by 0 records (0 alert-class): 
- `11:22:00` describe_budget now: limit $600.0 actual $50.703 forecast $251.416 | LastUpdatedTime 2026-11-05 10:53:19.884000+00:00
- `11:22:00` MTD sources: {"budget_actual": 50.703, "ce_mtd": 48.3462}
- `11:22:00` #138 - 'AWS Business Support+' by day in the governor's 14-day service record: {"2026-11-01": 29.0}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live) - 68 h after the Nov 2 15:44 UTC restore

- `11:22:00` _health/fleet.json: 8.9 h old (12026 B) -> fresh
- `11:22:00` _health/cadence-manifest.json: 3.2 h old (111119 B) -> fresh
- `11:22:00` data/_freshness-monitor.json: 3.4 h old (1124327 B) -> fresh
- `11:22:00` manifest 2026-11-05T07:39:34.867288+00:00: 781 engines, 1340 declared data/ keys
- `11:22:04` outputs by tier: {'fresh': 1055, 'dead': 212, 'stale': 48, 'missing': 25} (yesterday: fresh 1053, stale 49, dead 208, missing 25)
- `11:22:04` cadence manifest: 1 schedule entries readable
- `11:22:04` SILENT engines - ALL outputs >= 54 h and < 7 d old (two daily slots missed, not dead; yesterday 18 at the 88 h line): 23
- `11:22:04`   base-rates: 1 output(s) 117-117 h | schedule ?
- `11:22:04`   beaters-grader: 1 output(s) 116-116 h | schedule ?
- `11:22:04`   cftc-deep-view: 2 output(s) 110-110 h | schedule ?
- `11:22:04`   chart-patterns: 1 output(s) 60-60 h | schedule ?
- `11:22:04`   cross-asset-regime: 2 output(s) 63-63 h | schedule ?
- `11:22:04`   earnings-blackout: 2 output(s) 144-144 h | schedule ?
- `11:22:04`   earnings-iv-crush: 1 output(s) 61-61 h | schedule ?
- `11:22:04`   earnings-quality: 1 output(s) 141-141 h | schedule ?
- `11:22:04`   fmp-ratios: 1 output(s) 61-61 h | schedule ?
- `11:22:04`   forward-orders: 1 output(s) 144-144 h | schedule ?
- `11:22:04`   gdelt-sentiment: 1 output(s) 56-56 h | schedule ?
- `11:22:04`   gf-value: 1 output(s) 61-61 h | schedule ?
- `11:22:04`   gold-equity-rotation: 1 output(s) 61-61 h | schedule ?
- `11:22:04`   leading-markets: 2 output(s) 61-61 h | schedule ?
- `11:22:04`   liquidity-profile: 1 output(s) 133-133 h | schedule ?
- `11:22:04`   methodology-scout: 3 output(s) 57-57 h | schedule ?
- `11:22:04`   momentum-breakout: 1 output(s) 144-144 h | schedule ?
- `11:22:04`   naaim: 1 output(s) 138-138 h | schedule ?
- `11:22:04`   predictability: 1 output(s) 60-60 h | schedule ?
- `11:22:04`   revenue-acceleration: 1 output(s) 144-144 h | schedule ?
- `11:22:04`   russell-recon-frontrun: 1 output(s) 143-143 h | schedule ?
- `11:22:04`   spx-beaters: 1 output(s) 117-117 h | schedule ?
- `11:22:04`   volatility-squeeze-hunter: 1 output(s) 144-144 h | schedule ?
- `11:22:04` ⚠ 23 engines have every declared output 54 h - 7 d old (held engines marked; nothing re-invoked; section F probes them)
- `11:22:04` engines with ALL outputs 60 h - 7 d old (yesterday 19): 19
- `11:22:04` engines with at least one output < 30 h: 663 of 781 (yesterday 665 of 779)
- `11:22:04` STALE 30h-7d (48), youngest 40: inventory-drawdown.json 36h, dealer-survey.json 43h, pump-positioning.json 44h, import-canary-history.json 45h, import-canary.json 45h, forensic-screen.json 46h, brain-compiler.json 46h, asset-discovery.json 46h, vol-target-unwind.json 46h, theme-momentum.json 48h, theme-rotation-state.json 48h, gdelt-news.json 56h, gap-est-snapshots.json 57h, source-intel.json 57h, methodology-gaps.json 57h, methodology-kb.json 57h, predictability.json 60h, chart-patterns.json 60h, fmp-ratios.json 61h, gold-equity-rotation.json 61h, insider-aggregate.json 61h, leading-markets-history.json 61h, leading-markets.json 61h, gf-value.json 61h, earnings-iv-crush.json 61h, cross-asset-regime-state.json 63h, cross-asset-regime.json 63h, velocity-acceleration.json 65h, cftc-deep-view.json 110h, cot-extremes.json 110h, beaters-learned-weights.json 116h, spx-beaters.json 117h, base-rates.json 117h, sec-cik-ticker-map.json 119h, _state/ishares-product-map.json 127h, history/rebalance-eventstudy.json 133h, liquidity-profile.json 133h, nyfed-primary-dealer.json 134h, naaim.json 138h, earnings-quality.json 141h
- `11:22:04` DEAD >=7d (212); crossed the line in the last day (2): providers/mof/weekly-securities.json 169h<-justhodl-global-flows, signal-genealogy-research/current.json 190h<-justhodl-signal-genealogy
- `11:22:04` DEAD oldest: feedback-summary.json 185d, user-watchlist.json 182d, user-trades-stats.json 182d, user-trades.json 182d, carry-surface/alert-history.json 154d, quiver-lobbying-cache.json 147d, _alerts/convergence-radar-alerted.json 147d, quiver-congress-cache.json 133d, _cache/chokepoint-irreplaceability.json 126d, ka-config.json 114d, khalid-config.json 114d, _state/fleet-integrity-baseline.json 96d, ecb-confidence.json 95d, audit/inventory.json 93d, calibration-snapshot.json 93d
- `11:22:04` MISSING objects (25): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ici-flows.json<-justhodl-ici-flows, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:22:04` FRESH (<30 h): 1055 of 1340
- `11:22:04` engines born this week (13f-book Nov 3, strength Nov 4, leaders Nov 5):
- `11:22:04`   data/13f-book.json: 13.8 h old (1821194 B) -> fresh
- `11:22:04`   data/strength.json: 7.7 h old (1361785 B) -> fresh
- `11:22:04`   data/leaders.json: 2.8 h old (3035397 B) -> fresh
## E. FMP bandwidth hold (walker + bank manifests)

- `11:22:05` data/warm/fmp/manifest.json (0.2 h old): version 1.3.0 phase BACKFILL key_status ok | hold HELD until 2026-11-06T07:09+00:00 | FMP 429 'Bandwidth Limit Reach' at 2026-11-05T11:09:56+00:00: the plan's 30-day bandwidth is spent; holding 20 h (schedules probe once a day)
- `11:22:05` data/warm/fmp-bank/manifest.json (0.3 h old): version 1.3.0 phase BACKFILL key_status ok | hold clear until None | 
## F. silent engines: triggers, invocations, errors, newest log tail (follow-up to ops 6728)

- `11:22:05` 454 Scheduler schedules listed, 408 distinct target functions
- `11:22:07` base-rates -> ON_TIME (yesterday -) | reserved None | outputs 117 h | triggers (1 enabled, daily-or-faster False): sched justhodl-base-rates-weekly ENABLED cron(30 14 ? * SAT *)
- `11:22:07`     metrics: no datapoints in 4 days
- `11:22:08` beaters-grader -> ON_TIME (yesterday -) | reserved None | outputs 116 h | triggers (1 enabled, daily-or-faster False): sched justhodl-beaters-grader-weekly ENABLED cron(0 15 ? * SAT *)
- `11:22:08`     metrics: no datapoints in 4 days
- `11:22:08` cftc-deep-view -> ON_TIME (yesterday -) | reserved None | outputs 110/110 h | triggers (1 enabled, daily-or-faster False): rule justhodl-cftc-deep-view-sched ENABLED cron(0 21 ? * FRI,SAT *); sched justhodl-cftc-deep-view-fri DISABLED cron(0 22 ? * FRI *)
- `11:22:08`     metrics: no datapoints in 4 days
- `11:22:09` chart-patterns -> FAILING (yesterday -) | reserved None | outputs 60 h | triggers (1 enabled, daily-or-faster True): sched justhodl-chart-patterns-daily ENABLED cron(30 23 * * ? *)
- `11:22:09`     metrics: 11-02 n1 e0 t0, 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:09`     log 11-04 23:58: [fmp-bankfirst] hits=1 stale=599 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=43.3s
- `11:22:09`     log 11-04 23:59: [fmp-bankfirst] hits=1 stale=649 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=46.2s
- `11:22:09`     log 11-05 00:00: [fmp-bankfirst] hits=1 stale=698 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=49.0s
- `11:22:09`     log 11-05 00:00: [fmp-bankfirst] hits=1 stale=699 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=49.1s
- `11:22:09`     log 11-05 00:00: [fmp-bankfirst] hits=1 stale=749 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=52.1s
- `11:22:09`     log 11-05 00:01: [fmp-bankfirst] hits=1 stale=799 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=55.5s
- `11:22:09`     log 11-05 00:02: [fmp-bankfirst] hits=1 stale=899 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=60.9s
- `11:22:09`     log 11-05 00:02: [fmp-bankfirst] hits=1 stale=949 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=63.4s
- `11:22:09`     log 11-05 00:02: [fmp-bankfirst] hits=1 stale=999 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=66.3s
- `11:22:09`     log 11-05 00:03: REPORT RequestId: 2e6aebbf-f8fd-4b05-bf38-cfac64d76fe7 Duration: 600000.00 ms Billed Duration: 600000 ms Memory Size: 1024 MB Max Memory Used: 168 MB Status: timeout XRAY TraceId: 1-6aebc00f-01f1ea3f0f8a687d03a6e1c7 SegmentId: b503c6fbd41e778a Sampled: true
- `11:22:10` cross-asset-regime -> RAN_NO_WRITE (yesterday -) | reserved None | outputs 63/63 h | triggers (1 enabled, daily-or-faster False): rule justhodl-cross-asset-regime-sched ENABLED cron(35 20 ? * MON-FRI *)
- `11:22:10`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:11`     log 11-04 20:35: [regime] starting v1.0
- `11:22:11`     log 11-04 20:35: [regime] fetched 0/8 histories
- `11:22:11`     log 11-04 20:35: REPORT RequestId: 162131a2-5c7e-4d08-831f-9b056bf9f6b5 Duration: 1294.43 ms Billed Duration: 1788 ms Memory Size: 512 MB Max Memory Used: 120 MB Init Duration: 493.02 ms XRAY TraceId: 1-6aeb9724-5d60c6d421366b1227ed8f02 SegmentId: c46b3d1b2acfdf47 Sampled: tru
- `11:22:11` crypto-emergence -> RAN (yesterday RAN_NO_WRITE) | reserved None | outputs 12 h | triggers (1 enabled, daily-or-faster True): rule justhodl-crypto-emergence-daily ENABLED cron(0 23 * * ? *)
- `11:22:11`     metrics: 11-02 n1 e0 t0, 11-03 n2 e0 t0, 11-04 n2 e0 t0
- `11:22:11` earnings-blackout -> FAILING (yesterday FAILING) | reserved None | outputs 144/144 h | triggers (1 enabled, daily-or-faster True): rule justhodl-blackout-daily ENABLED cron(31 11 * * ? *)
- `11:22:11`     metrics: 11-03 n6 e6 t0, 11-04 n6 e6 t0
- `11:22:12`     log 11-04 13:30: [ERROR] HTTPError: HTTP Error 429: Too Many Requests Traceback (most recent call last): File "/var/task/lambda_function.py", line 66, in lambda_handler j = _get("https://financialmodelingprep.com/stable/earnings-calendar?from=%s&to=%s&limit=3000&apikey=***" Fi
- `11:22:12`     log 11-04 13:30: REPORT RequestId: ba6ad7a4-485a-46d5-9a67-0e3ffb816780 Duration: 3902.29 ms Billed Duration: 4413 ms Memory Size: 256 MB Max Memory Used: 162 MB Init Duration: 509.73 ms XRAY TraceId: 1-6aeb337e-5ec2c89f4ade093f0579470d SegmentId: 83bf4fb29959b3a8 Sampled: tru
- `11:22:12`     log 11-04 13:32: [blackout] sp500 members: 503
- `11:22:12`     log 11-04 13:32: [blackout] caps matched: 503/503 total $72.5T
- `11:22:12`     log 11-04 13:32: [ERROR] HTTPError: HTTP Error 429: Too Many Requests Traceback (most recent call last): File "/var/task/lambda_function.py", line 66, in lambda_handler j = _get("https://financialmodelingprep.com/stable/earnings-calendar?from=%s&to=%s&limit=3000&apikey=***" Fi
- `11:22:12`     log 11-04 13:32: REPORT RequestId: ba6ad7a4-485a-46d5-9a67-0e3ffb816780 Duration: 3457.92 ms Billed Duration: 3458 ms Memory Size: 256 MB Max Memory Used: 220 MB XRAY TraceId: 1-6aeb337e-5ec2c89f4ade093f0579470d SegmentId: de65d8416cd3a5a9 Sampled: true
- `11:22:12`     log 11-04 13:34: [blackout] sp500 members: 503
- `11:22:12`     log 11-04 13:34: [blackout] caps matched: 503/503 total $72.5T
- `11:22:12`     log 11-04 13:34: [ERROR] HTTPError: HTTP Error 429: Too Many Requests Traceback (most recent call last): File "/var/task/lambda_function.py", line 66, in lambda_handler j = _get("https://financialmodelingprep.com/stable/earnings-calendar?from=%s&to=%s&limit=3000&apikey=***" Fi
- `11:22:12`     log 11-04 13:34: REPORT RequestId: ba6ad7a4-485a-46d5-9a67-0e3ffb816780 Duration: 3083.78 ms Billed Duration: 3084 ms Memory Size: 256 MB Max Memory Used: 234 MB XRAY TraceId: 1-6aeb337e-5ec2c89f4ade093f0579470d SegmentId: da0f98198ad180f4 Sampled: true
- `11:22:12` earnings-iv-crush -> RAN_NO_WRITE (yesterday -) | reserved None | outputs 61 h | triggers (1 enabled, daily-or-faster True): rule justhodl-earnings-iv-crush-daily DISABLED cron(50 21 * * ? *); sched justhodl-earnings-iv-crush-daily ENABLED cron(2 22 * * ? *)
- `11:22:12`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:13`     log 11-04 22:02: earnings calendar error: HTTP Error 429: Too Many Requests
- `11:22:13`     log 11-04 22:02: earnings calendar returned 0 events
- `11:22:13`     log 11-04 22:02: REPORT RequestId: 8b6aebab-5818-4d13-bee8-32cafe59e13f Duration: 410.06 ms Billed Duration: 748 ms Memory Size: 1024 MB Max Memory Used: 99 MB Init Duration: 337.05 ms XRAY TraceId: 1-6aebab76-28122695265e77e86275b860 SegmentId: e4b368d146a402a3 Sampled: true
- `11:22:13` earnings-quality -> RAN_NO_WRITE (yesterday RAN_NO_WRITE) | reserved None | outputs 141 h | triggers (1 enabled, daily-or-faster True): rule jh-earnings-quality-daily ENABLED cron(18 14 * * ? *); sched justhodl-earnings-quality-weekly DISABLED cron(30 13 ? * WED *)
- `11:22:13`     metrics: 11-03 n2 e0 t0, 11-04 n2 e0 t0
- `11:22:14`     log 11-04 14:18: REPORT RequestId: 4d2f5774-8b7f-4898-a75b-ac4b02654d5f Duration: 1066.52 ms Billed Duration: 1516 ms Memory Size: 1536 MB Max Memory Used: 109 MB Init Duration: 449.36 ms XRAY TraceId: 1-6aeb3ebc-3c0aa27655adaf4149d1f3c9 SegmentId: 06e6d4a59aa80a9c Sampled: tr
- `11:22:14` ecb-derived -> PARTLY_FAILING (yesterday FAILING) | reserved None | outputs 20/22 h | triggers (1 enabled, daily-or-faster True): rule justhodl-ecb-derived-daily ENABLED cron(30 15 * * ? *)
- `11:22:14`     metrics: 11-03 n6 e6 t0, 11-04 n4 e3 t0
- `11:22:15`     log 11-04 15:43: [ecb-derived] ILM/W.U2.C.A050200.U2.EUR err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:44: [ecb-derived] ILM/W.U2.C.A050500.U2.EUR err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:44: [ecb-derived] EST/B.EU000A2X2A25.WT err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:45: [ecb-derived] multi YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M+SR_1Y+SR_2Y+SR_5Y+SR_10Y err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:45: [ecb-derived] EWT/Q.U2.N.WT.COVR._T.4F0._Z err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:46: [ecb-derived] EXR/D.USD.EUR.SP00.A err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:46: [ecb-derived] EXR/D.E02.EUR.EN00.A err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:47: [ecb-derived] IRS/M.DE.L.L40.CI.0000.EUR.N.Z err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:47: [ecb-derived] IRS/M.ES.L.L40.CI.0000.EUR.N.Z err: HTTP Error 504: Gateway Time-out
- `11:22:15`     log 11-04 15:48: REPORT RequestId: e722374a-4534-4d14-8e08-8ae85005e154 Duration: 300000.00 ms Billed Duration: 300000 ms Memory Size: 512 MB Max Memory Used: 98 MB Status: timeout XRAY TraceId: 1-6aeb4f8b-3a708cc95981f0c10d74aebe SegmentId: 7322d829b6f26687 Sampled: true
- `11:22:15` eps-revision-velocity -> PARTLY_FAILING (yesterday FAILING) | reserved None | outputs 0 h | triggers (0 enabled, daily-or-faster False): NONE
- `11:22:15`     metrics: 11-02 n3 e3 t0, 11-03 n3 e3 t0, 11-04 n1 e0 t0
- `11:22:15`     log 11-05 11:00: REPORT RequestId: b107692a-c8d2-4958-b142-fa0b312732bd Duration: 4053.23 ms Billed Duration: 4507 ms Memory Size: 1024 MB Max Memory Used: 191 MB Init Duration: 453.02 ms XRAY TraceId: 1-6aec61ba-3b7f46cc11aeab4b3eb1fa3e SegmentId: 55fc0e14ab16f4a3 Sampled: tr
- `11:22:16` fmp-ratios -> FAILING (yesterday -) | reserved None | outputs 61 h | triggers (1 enabled, daily-or-faster True): sched justhodl-fmp-ratios-daily ENABLED cron(38 22 * * ? *)
- `11:22:16`     metrics: 11-02 n1 e0 t0, 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:16`     log 11-04 22:42: [ERROR] RuntimeError: FMP key status {"status": "http_429", "path": "ratios"} - previous book kept Traceback (most recent call last): File "/var/task/lambda_function.py", line 184, in lambda_handler raise RuntimeError("FMP key status %s - previous book kept" %
- `11:22:16`     log 11-04 22:42: REPORT RequestId: c96aebb3-c8c0-439a-ace6-cabd135bb4a1 Duration: 252235.17 ms Billed Duration: 252663 ms Memory Size: 1024 MB Max Memory Used: 107 MB Init Duration: 427.74 ms XRAY TraceId: 1-6aebb3e7-611ca8ef0ff631e030406cec SegmentId: 64bb4ce7980a410a Sampled
- `11:22:16`     log 11-04 22:43: [fmp-ratios] universe {'core': 40, 'tv_universe': 292, 'sp500': 0, 'selected': 314, 'candidates': 314} -> 314 names; previous book 2026-11-02T22:38:32+00:00 with 697 rows
- `11:22:16`     log 11-04 22:47: [fmp-ratios] attempted 314 ok 0 errors 314 skipped(time) 0 key_status {"status": "http_429", "path": "ratios"} in 252s
- `11:22:16`     log 11-04 22:47: [ERROR] RuntimeError: FMP key status {"status": "http_429", "path": "ratios"} - previous book kept Traceback (most recent call last): File "/var/task/lambda_function.py", line 184, in lambda_handler raise RuntimeError("FMP key status %s - previous book kept" %
- `11:22:16`     log 11-04 22:47: REPORT RequestId: c96aebb3-c8c0-439a-ace6-cabd135bb4a1 Duration: 251997.70 ms Billed Duration: 251998 ms Memory Size: 1024 MB Max Memory Used: 118 MB XRAY TraceId: 1-6aebb3e7-611ca8ef0ff631e030406cec SegmentId: d2bec31f27cb2dce Sampled: true
- `11:22:16`     log 11-04 22:49: [fmp-ratios] universe {'core': 40, 'tv_universe': 292, 'sp500': 0, 'selected': 314, 'candidates': 314} -> 314 names; previous book 2026-11-02T22:38:32+00:00 with 697 rows
- `11:22:16`     log 11-04 22:54: [fmp-ratios] attempted 314 ok 0 errors 314 skipped(time) 0 key_status {"status": "http_429", "path": "ratios"} in 252s
- `11:22:16`     log 11-04 22:54: [ERROR] RuntimeError: FMP key status {"status": "http_429", "path": "ratios"} - previous book kept Traceback (most recent call last): File "/var/task/lambda_function.py", line 184, in lambda_handler raise RuntimeError("FMP key status %s - previous book kept" %
- `11:22:16`     log 11-04 22:54: REPORT RequestId: c96aebb3-c8c0-439a-ace6-cabd135bb4a1 Duration: 251971.11 ms Billed Duration: 251972 ms Memory Size: 1024 MB Max Memory Used: 119 MB XRAY TraceId: 1-6aebb3e7-611ca8ef0ff631e030406cec SegmentId: 535ac2c2155bf28a Sampled: true
- `11:22:16` forward-orders -> RAN_NO_WRITE (yesterday RAN_NO_WRITE) | reserved None | outputs 144 h | triggers (1 enabled, daily-or-faster True): rule forward-orders-daily ENABLED cron(1 11 * * ? *); sched justhodl-forward-orders-daily DISABLED cron(50 21 ? * MON-FRI *)
- `11:22:16`     metrics: 11-02 n1 e0 t0, 11-03 n2 e0 t0, 11-04 n2 e0 t0
- `11:22:17`     log 11-05 11:01: [fwd-orders] fetching universe…
- `11:22:17`     log 11-05 11:01: [fwd-orders] universe size: 0 tickers
- `11:22:17`     log 11-05 11:01: REPORT RequestId: 60cc2711-caa8-4516-9e96-46cf1ba0b735 Duration: 86.82 ms Billed Duration: 503 ms Memory Size: 1024 MB Max Memory Used: 93 MB Init Duration: 415.60 ms XRAY TraceId: 1-6aec61f3-096eb4506bd6baa14a7571ba SegmentId: 1c3c1554fa485c1e Sampled: true
- `11:22:17` gdelt-sentiment -> RAN_NO_WRITE (yesterday -) | reserved 2 | outputs 56 h | triggers (1 enabled, daily-or-faster True): rule justhodl-gdelt-sentiment-30min ENABLED cron(43 3 * * ? *)
- `11:22:17`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:18`     log 11-05 03:43: REPORT RequestId: ba4b31bb-0132-44d3-9258-ca132732dad5 Duration: 417.83 ms Billed Duration: 729 ms Memory Size: 1024 MB Max Memory Used: 92 MB Init Duration: 311.13 ms XRAY TraceId: 1-6aebfb4b-7d45f2e94d8d3a4f71f9c5de SegmentId: 3dbc038fe073777d Sampled: true
- `11:22:18` gf-value -> RAN_NO_WRITE (yesterday -) | reserved None | outputs 61 h | triggers (1 enabled, daily-or-faster True): sched justhodl-gf-value-daily ENABLED cron(2 22 * * ? *)
- `11:22:18`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:19`     log 11-04 22:02: [fmp-bankfirst] hits=85 stale=60 misses=0 unshaped=0 passthrough=66 bypassed=0 errors=0 bank_time=26.6s
- `11:22:19`     log 11-04 22:02: [fmp-bankfirst] hits=141 stale=98 misses=5 unshaped=0 passthrough=104 bypassed=0 errors=0 bank_time=78.0s
- `11:22:19`     log 11-04 22:02: [fmp-bankfirst] hits=141 stale=99 misses=5 unshaped=0 passthrough=104 bypassed=0 errors=0 bank_time=78.0s
- `11:22:19`     log 11-04 22:02: [fmp-bankfirst] hits=141 stale=100 misses=5 unshaped=0 passthrough=105 bypassed=0 errors=0 bank_time=78.3s
- `11:22:19`     log 11-04 22:02: [fmp-bankfirst] bank stepped aside for the rest of this invocation: bank time cap 90s (attempts=269 bank_time=106.5s)
- `11:22:19`     log 11-04 22:05: REPORT RequestId: 706aebab-5822-43c4-9f87-3d63d6d334e7 Duration: 181114.06 ms Billed Duration: 181536 ms Memory Size: 1536 MB Max Memory Used: 950 MB Init Duration: 421.27 ms XRAY TraceId: 1-6aebab66-32f65ae0489097dc17c809f8 SegmentId: 64184d79702c53c2 Sampled
- `11:22:19` gold-equity-rotation -> RAN_NO_WRITE (yesterday -) | reserved None | outputs 61 h | triggers (1 enabled, daily-or-faster True): rule justhodl-gold-equity-rotation-sched DISABLED cron(45 20 ? * MON-FRI *); sched justhodl-gold-equity-rotation-daily ENABLED cron(45 22 * * ? *)
- `11:22:19`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:19`     log 11-04 22:45: REPORT RequestId: 5a6aebb5-6c78-4e75-9266-218f85fd0e56 Duration: 9792.43 ms Billed Duration: 10176 ms Memory Size: 512 MB Max Memory Used: 156 MB Init Duration: 383.38 ms XRAY TraceId: 1-6aebb56f-6be16b62619c4a706e84b9a7 SegmentId: 55b14d9be717dc46 Sampled: tr
- `11:22:20` leading-markets -> RAN_NO_WRITE (yesterday -) | reserved None | outputs 61/61 h | triggers (1 enabled, daily-or-faster False): rule leading-markets-daily ENABLED cron(30 22 ? * MON-FRI *)
- `11:22:20`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:20`     log 11-04 22:31: [leading] EWP (Spain): insufficient data (0)
- `11:22:20`     log 11-04 22:31: [leading] EWQ (France): insufficient data (0)
- `11:22:20`     log 11-04 22:31: [leading] EFNL (Finland): insufficient data (0)
- `11:22:20`     log 11-04 22:31: [leading] EWD (Sweden): insufficient data (0)
- `11:22:20`     log 11-04 22:31: [leading] VGK (Europe): insufficient data (0)
- `11:22:20`     log 11-04 22:31: REPORT RequestId: a2f922b8-5a8d-4298-abb4-bf0e4783159a Duration: 84230.75 ms Billed Duration: 84811 ms Memory Size: 256 MB Max Memory Used: 116 MB Init Duration: 579.69 ms XRAY TraceId: 1-6aebb1eb-7c8deac3022f8f0b06b0454c SegmentId: c3a8564bc6806412 Sampled: t
- `11:22:21` liquidity-profile -> ON_TIME (yesterday -) | reserved None | outputs 133 h | triggers (1 enabled, daily-or-faster False): rule liquidity-profile-fri ENABLED cron(3 22 ? * FRI *)
- `11:22:21`     metrics: no datapoints in 4 days
- `11:22:21` methodology-scout -> ON_TIME (yesterday -) | reserved None | outputs 57/57/57 h | triggers (1 enabled, daily-or-faster False): rule justhodl-methodology-scout-cadence ENABLED rate(7 days)
- `11:22:21`     metrics: 11-02 n1 e0 t0
- `11:22:21` momentum-breakout -> FAILING (yesterday FAILING) | reserved None | outputs 144 h | triggers (0 enabled, daily-or-faster False): NONE
- `11:22:21`     metrics: 11-02 n3 e3 t0, 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:22`     log 11-05 11:15: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 476, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:22`     log 11-05 11:15: REPORT RequestId: 392da000-8d59-4ce3-a175-8727b4f781f1 Duration: 1048.59 ms Billed Duration: 1599 ms Memory Size: 1024 MB Max Memory Used: 123 MB Init Duration: 549.64 ms XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: cecac4766bed4a92 Sampled: tr
- `11:22:22`     log 11-05 11:16: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 476, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:22`     log 11-05 11:16: REPORT RequestId: 392da000-8d59-4ce3-a175-8727b4f781f1 Duration: 984.64 ms Billed Duration: 985 ms Memory Size: 1024 MB Max Memory Used: 131 MB XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: 85819504ec2870ab Sampled: true
- `11:22:22`     log 11-05 11:18: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 476, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:22`     log 11-05 11:18: REPORT RequestId: 392da000-8d59-4ce3-a175-8727b4f781f1 Duration: 1109.42 ms Billed Duration: 1110 ms Memory Size: 1024 MB Max Memory Used: 134 MB XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: 3511265fa5f1ffe9 Sampled: true
- `11:22:22` naaim -> ON_TIME (yesterday -) | reserved None | outputs 138 h | triggers (1 enabled, daily-or-faster False): rule justhodl-naaim-weekly ENABLED cron(30 17 ? * THU,FRI *)
- `11:22:22`     metrics: no datapoints in 4 days
- `11:22:22` portwatch -> RAN_NO_WRITE (yesterday RAN_NO_WRITE) | reserved None | outputs 192/192 h | triggers (1 enabled, daily-or-faster True): sched justhodl-portwatch-daily ENABLED cron(20 11 * * ? *)
- `11:22:22`     metrics: 11-02 n1 e0 t0, 11-03 n1 e0 t0, 11-04 n1 e0 t0
- `11:22:23`     log 11-05 11:21: PortWatch preserved publication failed: CaptureError
- `11:22:23`     log 11-05 11:21: REPORT RequestId: 5b6aec66-60fc-455a-86ca-c35c04a820ad Duration: 11868.29 ms Billed Duration: 12382 ms Memory Size: 1024 MB Max Memory Used: 241 MB Init Duration: 512.93 ms XRAY TraceId: 1-6aec6692-1b1a6af43e5c771d04e14836 SegmentId: 7c0dbfc8b8fe8956 Sampled: 
- `11:22:23` predictability -> FAILING (yesterday -) | reserved None | outputs 60 h | triggers (1 enabled, daily-or-faster True): sched justhodl-predictability-daily ENABLED cron(50 23 * * ? *)
- `11:22:23`     metrics: 11-02 n1 e0 t0, 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:24`     log 11-04 23:50: [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=0.1s
- `11:22:24`     log 11-05 00:00: REPORT RequestId: db6aebc4-a819-4f3c-b5bf-f84f3e72e045 Duration: 600000.00 ms Billed Duration: 600327 ms Memory Size: 1024 MB Max Memory Used: 95 MB Init Duration: 326.38 ms Status: timeout XRAY TraceId: 1-6aebc4b5-42f654195a7037b833b7c540 SegmentId: 6a84b61cd
- `11:22:24`     log 11-05 00:01: [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=0.1s
- `11:22:24`     log 11-05 00:11: REPORT RequestId: db6aebc4-a819-4f3c-b5bf-f84f3e72e045 Duration: 600000.00 ms Billed Duration: 600000 ms Memory Size: 1024 MB Max Memory Used: 95 MB Status: timeout XRAY TraceId: 1-6aebc4b5-42f654195a7037b833b7c540 SegmentId: 78b6fe115f71a4b8 Sampled: true
- `11:22:24`     log 11-05 00:12: [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=0.1s
- `11:22:24`     log 11-05 00:22: REPORT RequestId: db6aebc4-a819-4f3c-b5bf-f84f3e72e045 Duration: 600000.00 ms Billed Duration: 600000 ms Memory Size: 1024 MB Max Memory Used: 95 MB Status: timeout XRAY TraceId: 1-6aebc4b5-42f654195a7037b833b7c540 SegmentId: f19de0f775c275fb Sampled: true
- `11:22:24` revenue-acceleration -> FAILING (yesterday FAILING) | reserved None | outputs 144 h | triggers (0 enabled, daily-or-faster False): NONE
- `11:22:24`     metrics: 11-02 n3 e3 t0, 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:24`     log 11-05 11:15: [ERROR] ValueError: All statement populations unavailable; preserve previous publication Traceback (most recent call last): File "/var/task/lambda_function.py", line 584, in lambda_handler raise ValueError('All statement populations unavailable; preserve previ
- `11:22:24`     log 11-05 11:15: REPORT RequestId: 3efb321b-95a6-47ae-8644-b554c9a36294 Duration: 654.64 ms Billed Duration: 1160 ms Memory Size: 1024 MB Max Memory Used: 122 MB Init Duration: 505.26 ms XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: 7a42d67d5fc24b09 Sampled: tru
- `11:22:24`     log 11-05 11:16: [ERROR] ValueError: All statement populations unavailable; preserve previous publication Traceback (most recent call last): File "/var/task/lambda_function.py", line 584, in lambda_handler raise ValueError('All statement populations unavailable; preserve previ
- `11:22:24`     log 11-05 11:16: REPORT RequestId: 3efb321b-95a6-47ae-8644-b554c9a36294 Duration: 681.11 ms Billed Duration: 682 ms Memory Size: 1024 MB Max Memory Used: 136 MB XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: f21da5c0b31a0290 Sampled: true
- `11:22:24`     log 11-05 11:19: [ERROR] ValueError: All statement populations unavailable; preserve previous publication Traceback (most recent call last): File "/var/task/lambda_function.py", line 584, in lambda_handler raise ValueError('All statement populations unavailable; preserve previ
- `11:22:24`     log 11-05 11:19: REPORT RequestId: 3efb321b-95a6-47ae-8644-b554c9a36294 Duration: 568.01 ms Billed Duration: 569 ms Memory Size: 1024 MB Max Memory Used: 144 MB XRAY TraceId: 1-6aec6561-351023271d4249a71582c4d6 SegmentId: f7634169228054cc Sampled: true
- `11:22:25` russell-recon-frontrun -> RAN_NO_WRITE (yesterday RAN_NO_WRITE) | reserved None | outputs 143 h | triggers (1 enabled, daily-or-faster True): rule justhodl-russell-recon-daily ENABLED cron(30 12 * * ? *); rule justhodl-russell-recon-frontrun-sched DISABLED cron(30 12 * * ? *)
- `11:22:25`     metrics: 11-03 n2 e0 t0, 11-04 n2 e0 t0
- `11:22:25`     log 11-04 13:31: fmp_screener AMEX: 0 rows
- `11:22:25`     log 11-04 13:31: fmp_screener returned 0 rows; falling back to stock-list
- `11:22:25`     log 11-04 13:31: [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=3 bypassed=0 errors=0 bank_time=0.5s
- `11:22:25`     log 11-04 13:31: stock-list fallback: 0 US equity tickers
- `11:22:25`     log 11-04 13:31: screener rows: 0
- `11:22:25`     log 11-04 13:31: REPORT RequestId: 17726e6f-2a45-420d-8d3e-a3b2f015440c Duration: 926.66 ms Billed Duration: 1520 ms Memory Size: 1024 MB Max Memory Used: 149 MB Init Duration: 592.68 ms XRAY TraceId: 1-6aeb337e-5ec2c89f4ade093f0579470d SegmentId: a343d6a681b6f1af Sampled: tru
- `11:22:26` spx-beaters -> ON_TIME (yesterday -) | reserved None | outputs 117 h | triggers (1 enabled, daily-or-faster False): sched justhodl-spx-beaters-weekly ENABLED cron(36 14 ? * SAT *)
- `11:22:26`     metrics: no datapoints in 4 days
- `11:22:26` systemic-stress -> PARTLY_FAILING (yesterday FAILING) | reserved None | outputs 23 h | triggers (1 enabled, daily-or-faster True): rule systemic-stress-daily ENABLED cron(8 12 * * ? *)
- `11:22:26`     metrics: 11-03 n6 e6 t0, 11-04 n1 e0 t0
- `11:22:27`     log 11-04 12:08: [systemic-stress] composite 66.3/100 (ELEVATED) | euro CISS pct 11.2 | frag 100 (ACUTE FRAGMENTATION) | ciss=8 sov=11 errors=0
- `11:22:27`     log 11-04 12:08: REPORT RequestId: 8ae6fad2-d8f3-4b0c-8a66-5f1ad849fc4f Duration: 35160.54 ms Billed Duration: 35706 ms Memory Size: 256 MB Max Memory Used: 102 MB Init Duration: 544.85 ms XRAY TraceId: 1-6aeb2024-653142d14239493145960958 SegmentId: 1dcfe02a6ccb6f3a Sampled: t
- `11:22:27` volatility-squeeze-hunter -> FAILING (yesterday FAILING) | reserved None | outputs 144 h | triggers (0 enabled, daily-or-faster False): NONE
- `11:22:27`     metrics: 11-03 n3 e3 t0, 11-04 n3 e3 t0
- `11:22:27`     log 11-04 11:30: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 555, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:27`     log 11-04 11:30: REPORT RequestId: a144f4d5-3490-4b1f-9752-86afe8a5c707 Duration: 1176.32 ms Billed Duration: 2011 ms Memory Size: 1024 MB Max Memory Used: 131 MB Init Duration: 834.51 ms XRAY TraceId: 1-6aeb1764-59c20bb3002169a004092e93 SegmentId: 7f7f2d2d944a9f34 Sampled: tr
- `11:22:27`     log 11-04 11:31: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 555, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:27`     log 11-04 11:31: REPORT RequestId: a144f4d5-3490-4b1f-9752-86afe8a5c707 Duration: 1121.67 ms Billed Duration: 1122 ms Memory Size: 1024 MB Max Memory Used: 140 MB XRAY TraceId: 1-6aeb1764-59c20bb3002169a004092e93 SegmentId: 2a8a85490b8481b1 Sampled: true
- `11:22:27`     log 11-04 11:33: [ERROR] ValueError: No received market observations; preserve current Traceback (most recent call last): File "/var/task/lambda_function.py", line 555, in lambda_handler raise ValueError('No received market observations; preserve current')
- `11:22:27`     log 11-04 11:33: REPORT RequestId: a144f4d5-3490-4b1f-9752-86afe8a5c707 Duration: 1296.76 ms Billed Duration: 1297 ms Memory Size: 1024 MB Max Memory Used: 141 MB XRAY TraceId: 1-6aeb1764-59c20bb3002169a004092e93 SegmentId: 2abea2f9837f8b33 Sampled: true
- `11:22:27` FAILING (7): chart-patterns, earnings-blackout, fmp-ratios, momentum-breakout, predictability, revenue-acceleration, volatility-squeeze-hunter
- `11:22:27` ON_TIME (7): base-rates, beaters-grader, cftc-deep-view, liquidity-profile, methodology-scout, naaim, spx-beaters
- `11:22:27` PARTLY_FAILING (3): ecb-derived, eps-revision-velocity, systemic-stress
- `11:22:27` RAN (1): crypto-emergence
- `11:22:27` RAN_NO_WRITE (10): cross-asset-regime, earnings-iv-crush, earnings-quality, forward-orders, gdelt-sentiment, gf-value, gold-equity-rotation, leading-markets, portwatch, russell-recon-frontrun
- `11:22:27` of yesterday's twelve, recovered (fresh output or on time): crypto-emergence
- `11:22:27` ⚠ 20 probed engines are FAILING / PARTLY_FAILING / NEVER_STARTED / THROTTLED / RAN_NO_WRITE / NO_FUNCTION - nothing was changed here
## G. source-refresh control (the two red scheduled workflows; read-only, no release, no retry)

- `11:22:28` capital-structure refresh control (661 h since last write): {"contract": "capital-structure-recurring-refresh.v1", "status": "failed", "request_id": "scheduled-capital-structure:37839611323", "started_at": "2026-10-08T20:28:05.951329+00:00", "failed_at": "2026-10-08T22:00:40.571580+00:00", "error_type": "ValueError", "active_phase": "part-2", "phase_started_at": "2026-10-08T21:43:52.083415+00:00", "completed_phases": ["plan", "part-1"], "planned_sources": 11305, "parts": 1, "review": null}
- `11:22:28` ⚠ capital-structure refresh control is 'failed': every scheduled plan stops at 'explicit review is required' until release_failed_control is run with an ops number and a review note - a human decision, not this run's
- `11:22:28` ✅ read-only; nothing changed, nothing released

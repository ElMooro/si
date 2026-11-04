# ops 6727 - daily health status 2026-11-04 (governor status + caps, event records since ops 6719, relay lag, output freshness 2 days after the restore, 13F family, FMP bandwidth hold; read-only)

**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-11-04T11:19:55+00:00  

## Data

| 13f_book_age_h | _health/cadence-manifest.json | _health/fleet.json | anomaly_held | baseline_usd_day | budget_actual | budget_actual_now | budget_forecast | budget_last_updated | budget_limit | cap | cap_source | capped | data/_freshness-monitor.json | engines_fresh | engines_missed_two_slots | engines_not_back_since_restore | events_since_6719 | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | meter_top | mode | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | relay_alert_records_unrelayed | stage | stage_since | storage_day | storage_last_gb | storage_last_level | version | versioning |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 8.4 | 16.6 | 8.8 | 0 | 9.87 | 45.611 | 45.611 | 0.0 | 2026-11-04 10:24:44.587000+00:00 | 600.0 | 600.0 | budget | 3 | 3.3 | 665 | 18 | 19 | {} | held until 2026-11-05T06:50+00:00 | held until 2026-11-05T06:41+00:00 | 3 | symdir $0.35; polygon-bank $0.28; eia-bank $0.16; bank-histories $0.15; fmp-walker $0.10 | enforce | 45.61 | 2 | 208 | 1053 | 25 | 49 | 1335 | 317.22 | 0 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 2026-11-03 | 1158.181 | None | 1.6.1 | None |

## Log
## A. governor status

- `11:19:53` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:19:53` cap $600.0 (budget) | budget limit $600.0 actual $45.611 forecast $0.0
- `11:19:53` thresholds: {"baseline_usd_day": 9.87, "brake_at": 531.23, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 26.53, "freeze_at": 571.23, "freeze_line_floored": false, "frozen_eom": 64.38, "lag_buffer": 9.87, "mtd": 45.61, "projected_eom": 317.22, "reserve": 28.77, "residual_usd_day": 0.34, "sources": {"budget_actual": 45.611, "ce_mtd": 43.0881}}
- `11:19:53` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:19:53` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:19:53` anomaly held (ANOM): none
- `11:19:53` fuse: {"checked_at": "2026-11-04T10:07:38.117312+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.83, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-fmp-bank", 0.57], ["justhodl-stock-screener", 0.1], ["justhodl-signal-harvester", 0.06], ["justhodl-fundamental-graphs", 0.01], ["justhodl-backend-agent", 0.01], ["justhodl-a2a-bus", 0.01], ["justhodl-hkma-monitor", 0.01], ["justhodl-apac-flows", 0.01]]}
- `11:19:53` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 23 functions ran, fleet $2.50/day; month-to-date top: justhodl-symdir $0.35, justhodl-polygon-bank $0.28, justhodl-eia-bank $0.16"], "mode": "enforce", "mtd": 45.61, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 9.87, "brake_at": 531.22, "budget_forecast": 0.0, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 26.54, "freeze_at": 571.22, "freeze_line_floored": false, "frozen_eom": 64.39, "lag_buffer": 9.87, "mtd": 45.61, "projected_eom": 317.3, "reserve": 28.78, "residual_source": "min7", "residual_usd_day": 0.34, "sources": {"budget_actual":
- `11:19:53` cadence sentinel: {"checked": 840, "checked_at": "2026-11-04T00:07:38.035049+00:00", "day": "2026-11-04", "errors": [], "violators": []} | fixed entries: 0
- `11:19:53` anomalies: {"errors_notified": {"day": "2026-10-22", "fns": ["justhodl-polygon-futures-curves"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:19:53` meter: {"active": 23, "daily": {"2026-11-02": {"alphavantage-market-agent": [2.0, 2e-05], "alphavantage-technical-analysis": [1.0, 1e-06], "bea-economic-agent": [3.0, 1.3e-05], "benzinga-news-agent": [4.0, 3.4e-05], "bls-employment-api-v2": [1.0, 3.7e-05], "bls-labor-agent": [1.0, 5.3e-05], "bond-indices-agent": [2.0, 6.1e-05], "census-economic-agent": [1.0, 2.6e-05], "cftc-futures-positioning-agent": [4.0, 0.001412], "chatgpt-agent-api": [1.0, 0.0], "coinmarketcap-agent": [1.0, 7e-06], "dollar-strength-agent": [3.0, 0.000141], "enhanced-repo-agent": [2.0, 9e-06], "fedliquidityapi": [6.0, 0.000112], "fmp-fundamentals-agent": [6.0, 4.9e-05], "fmp-stock-picks-agent": [1.0, 0.000285], "fred-ice-bofa-a
- `11:19:53` storage: day 2026-11-03 | versioning None | warn_days None | trip_days None
- `11:19:53` storage last assessment: {"day": "2026-11-02", "delta_gb": 5.68, "delta_objects": 62283, "delta_pct": 0.49, "gb": 1158.181, "level": null, "objects": 7174894, "prev": "2026-11-01", "prev_gb": 1152.498, "reason": "growth +5.7 GB (+0.5%), +62283 objects over 2026-11-02"}
- `11:19:53` S3 $/day (storage.s3_usd): {"2026-10-20": 12.8342, "2026-10-21": 18.9137, "2026-10-22": 29.4721, "2026-10-23": 6.7537, "2026-10-24": 2.7799, "2026-10-25": 3.452, "2026-10-26": 9.2012, "2026-10-27": 5.2497, "2026-10-28": 7.394, "2026-10-29": 7.2171, "2026-10-30": 7.6549, "2026-10-31": 7.0416, "2026-11-01": 1.6878, "2026-11-02": 2.3964}
- `11:19:53` service spend 2026-10-30: total $10.31 | Amazon Simple Storage Service $7.65, AWS Lambda $1.75, AmazonCloudWatch $0.48, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.08
- `11:19:53` service spend 2026-10-31: total $9.56 | Amazon Simple Storage Service $7.04, AWS Lambda $1.63, AmazonCloudWatch $0.47, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.07
- `11:19:53` service spend 2026-11-01: total $32.61 | AWS Business Support+ $29.00, Amazon Simple Storage Service $1.69, Amazon Route 53 $1.50, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.06
- `11:19:53` service spend 2026-11-02: total $2.92 | Amazon Simple Storage Service $2.40, AWS Secrets Manager $0.12, AmazonCloudWatch $0.10, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.08, Amazon DynamoDB $0.05
## B. meter: month-to-date top engines

- `11:19:53` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:19:53` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:19:53` month-to-date top 25: symdir $0.35, polygon-bank $0.28, eia-bank $0.16, bank-histories $0.15, fmp-walker $0.10, research-backtest $0.09, fmp-bank $0.07, causality-scanner $0.06, polygon-options-flow $0.06, feed-catalog $0.05, bottom $0.04, signal-genealogy $0.04, predictability $0.04, etf-global-desk $0.04, data-census $0.04, thesis-engine $0.04, etf-constituents $0.04, bank-factors $0.04, katlin $0.04, etf-true-flows $0.03
## C. governor event records since ops 6719 (2026-11-03 11:19 UTC), the raw budget read, the relay's marker

- `11:19:53` 3 event records on Nov 3 + Nov 4; 0 since ops 6719 (newest 20261103T063334.823234Z-release-cap.json)
- `11:19:53` no alert-class event records since yesterday's run (only bookkeeping kinds, if any)
- `11:19:53` relay marker: {"last_key": "spend-governor/events/20261103T063334.823234Z-release-cap.json", "updated": "2026-11-03T06:34:08.398286Z", "issues_opened": 3}
- `11:19:53` relay is behind the newest record by 0 records (0 alert-class): 
- `11:19:53` describe_budget now: limit $600.0 actual $45.611 forecast $0.0 | LastUpdatedTime 2026-11-04 10:24:44.587000+00:00
- `11:19:53` MTD sources: {"budget_actual": 45.611, "ce_mtd": 43.0881}
- `11:19:53` #138 - 'AWS Business Support+' by day in the governor's 14-day service record: {"2026-11-01": 29.0}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live) - 44 h after the Nov 2 15:44 UTC restore

- `11:19:53` _health/fleet.json: 8.8 h old (10848 B) -> fresh
- `11:19:53` _health/cadence-manifest.json: 16.6 h old (110741 B) -> fresh
- `11:19:53` data/_freshness-monitor.json: 3.3 h old (1103793 B) -> fresh
- `11:19:53` manifest 2026-11-03T18:10:08.760692+00:00: 779 engines, 1335 declared data/ keys
- `11:19:55` outputs by tier: {'fresh': 1053, 'dead': 208, 'stale': 49, 'missing': 25} (yesterday: fresh 751, stale 328, dead 213, missing 25)
- `11:19:55` cadence manifest: 1 schedule entries readable
- `11:19:55` THE FINDING from yesterday's note - engines with ALL outputs >= 88 h and < 7 d old (pre-freeze writes, two daily slots missed since the restore, not dead): 18
- `11:19:55`   base-rates: 1 output(s) 93-93 h | schedule ?
- `11:19:55`   beaters-grader: 1 output(s) 92-92 h | schedule ?
- `11:19:55`   crypto-emergence: 1 output(s) 132-132 h | schedule ?
- `11:19:55`   earnings-blackout: 2 output(s) 120-120 h | schedule ?
- `11:19:55`   earnings-quality: 1 output(s) 117-117 h | schedule ?
- `11:19:55`   ecb-derived: 2 output(s) 92-92 h | schedule ?
- `11:19:55`   eps-revision-velocity: 1 output(s) 120-120 h | schedule ?
- `11:19:55`   forward-orders: 1 output(s) 120-120 h | schedule ?
- `11:19:55`   liquidity-profile: 1 output(s) 109-109 h | schedule ?
- `11:19:55`   momentum-breakout: 1 output(s) 120-120 h | schedule ?
- `11:19:55`   naaim: 1 output(s) 114-114 h | schedule ?
- `11:19:55`   portwatch: 2 output(s) 168-168 h | schedule ?
- `11:19:55`   revenue-acceleration: 1 output(s) 120-120 h | schedule ?
- `11:19:55`   russell-recon-frontrun: 1 output(s) 119-119 h | schedule ?
- `11:19:55`   sellside-views: 1 output(s) 117-117 h | schedule ?
- `11:19:55`   spx-beaters: 1 output(s) 93-93 h | schedule ?
- `11:19:55`   systemic-stress: 1 output(s) 95-95 h | schedule ?
- `11:19:55`   volatility-squeeze-hunter: 1 output(s) 120-120 h | schedule ?
- `11:19:55` ⚠ 18 engines have not written since before the Nov 1-2 freeze although two daily slots have passed since the restore - these are not the recovery curve any more (held engines marked; nothing re-invoked)
- `11:19:55` engines with ALL outputs 60 h - 7 d old (yesterday 216): 19
- `11:19:55` engines with at least one output < 30 h: 665 of 779 (yesterday 456)
- `11:19:55` STALE 30h-7d (49), youngest 40: gdelt-news.json 32h, gap-est-snapshots.json 33h, source-intel.json 33h, methodology-gaps.json 33h, methodology-kb.json 33h, predictability.json 35h, chart-patterns.json 36h, fmp-ratios.json 37h, gold-equity-rotation.json 37h, insider-aggregate.json 37h, leading-markets-history.json 37h, leading-markets.json 37h, gf-value.json 37h, earnings-iv-crush.json 37h, cross-asset-regime-state.json 39h, cross-asset-regime.json 39h, velocity-acceleration.json 41h, cftc-deep-view.json 86h, cot-extremes.json 86h, ecb-derived.json 92h, ecb-hist/esi.json 92h, beaters-learned-weights.json 92h, spx-beaters.json 93h, base-rates.json 93h, treasury-noise-history.json 94h, sec-cik-ticker-map.json 95h, systemic-stress.json 95h, _state/ishares-product-map.json 103h, history/rebalance-eventstudy.json 109h, liquidity-profile.json 109h, nyfed-primary-dealer.json 110h, naaim.json 114h, earnings-quality.json 117h, sellside-views.json 117h, carry-surface.json 118h, russell-recon-frontrun.json 119h, earnings-blackout.json 120h, history/blackout.json 120h, volatility-squeeze.json 120h, momentum-breakout.json 120h
- `11:19:55` DEAD >=7d (208); crossed the line in the last day (2): readthrough-state.json 181h<-justhodl-readthrough, warm/worldbank-full/_state/inflight.json 182h<-justhodl-worldbank-full
- `11:19:55` DEAD oldest: feedback-summary.json 184d, user-watchlist.json 181d, user-trades-stats.json 181d, user-trades.json 181d, carry-surface/alert-history.json 153d, quiver-lobbying-cache.json 146d, _alerts/convergence-radar-alerted.json 146d, quiver-congress-cache.json 132d, _cache/chokepoint-irreplaceability.json 125d, ka-config.json 113d, khalid-config.json 113d, _state/fleet-integrity-baseline.json 95d, ecb-confidence.json 94d, audit/inventory.json 92d, calibration-snapshot.json 92d
- `11:19:55` MISSING objects (25): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ici-flows.json<-justhodl-ici-flows, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:19:55` FRESH (<30 h): 1053 of 1335
- `11:19:55` 13F family after the book's birth (ops 6723-6726; justhodl-13f-book-daily cron(37 16 * * ? *) UTC):
- `11:19:55`   data/13f-book.json: 8.4 h old (335036 B) -> fresh
- `11:19:55`   data/13f-positions.json: 8.4 h old (57370 B) -> fresh
- `11:19:55`   data/holdings-research.json: 8.4 h old (20982 B) -> fresh
- `11:19:55`   data/13f-clone-alpha.json: 218.8 h old (26925 B) -> dead
- `11:19:55`   data/smart-money-clusters.json: 19.3 h old (26635 B) -> fresh
- `11:19:55`   data/sec-13f.json: An error occurred (404) when calling the HeadObject operation: Not Found
- `11:19:55`   data/capital-flow.json: 18.8 h old (338772 B) -> fresh
## E. FMP bandwidth hold (walker + bank manifests)

- `11:19:55` data/warm/fmp/manifest.json (0.5 h old): version 1.3.0 phase BACKFILL key_status ok | hold HELD until 2026-11-05T06:50+00:00 | FMP 429 'Bandwidth Limit Reach' at 2026-11-04T10:50:12+00:00: the plan's 30-day bandwidth is spent; holding 20 h (schedules probe once a day)
- `11:19:55` data/warm/fmp-bank/manifest.json (0.6 h old): version 1.3.0 phase BACKFILL key_status ok | hold HELD until 2026-11-05T06:41+00:00 | FMP 429 'Bandwidth Limit Reach' at 2026-11-04T10:41:59+00:00: the plan's 30-day bandwidth is spent; holding 20 h
- `11:19:55` ✅ read-only; nothing changed, nothing released

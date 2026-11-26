# ops 6968 - daily health status 2026-11-26 (governor status + caps + events since ops 6947, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Thursday; read-only)

**Status:** success  
**Duration:** 11.8s  
**Finished:** 2026-11-26T11:18:06+00:00  

## Data

| 10kq-filings_age_h | 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | capstruct_failed_at | capstruct_status | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corporate-actions__state_age_h | credit-before-equity_age_h | credit_credit-before-equity-history_age_h | delta_objects | engines_fresh | engines_missed_two_slots | etf-flows_age_h | etf-true-flows_age_h | events_since_6947 | feed-catalog_age_h | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | global-flow-map_age_h | held_total | ici-flows_age_h | insider-trades_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | regime-map-prev_age_h | regime-map_age_h | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 246.2 | 18.7 | 3.3 | 0 | 7.38 | 7.5 | 219.255 | 252.633 | 6.6 | 7.2 | 600.0 | 3 | 2026-10-08T22:00:40.571580+00:00 | failed | 4.0 | 4.1 | 4.0 | 4.1 | 4.1 | 4.6 | 12.7 | 12.7 | 124779 | 689 | 37 | 23.7 | 19.4 | 0 | 196.3 | 8.8 | clear | clear | 6.5 | 3 | 110.2 | 14.1 | 7.5 | symdir $5.85; polygon-bank $5.19; eia-bank $3.58; worldbank-full $3.35; research-backtest $1.97; feed-catalog $1.75 | enforce | 8.8 | None | 219.26 | 1 | 158 | 1105 | 24 | 70 | 1357 | 260.05 | 14.3 | 14.3 | 266.9 | 14.7 | 8.8 | 13.4 | 4.0 | 2026-11-21 6.17; 2026-11-22 3.49; 2026-11-23 3.45; 2026-11-24 3.84 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.1 | 2026-11-25 | 1289.257 | None | none | 7.6 | 7.6 | 1.6.1 | None | 8.3 | 8.0 | 8.1 |

## Log
## A. governor status

- `11:17:56` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:17:56` cap $600.0 (budget) | budget limit $600.0 actual $219.255 forecast $252.633
- `11:17:56` thresholds: {"baseline_usd_day": 7.38, "brake_at": 540.34, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 4.53, "freeze_at": 580.34, "freeze_line_floored": false, "frozen_eom": 228.92, "lag_buffer": 7.38, "mtd": 219.26, "projected_eom": 260.05, "reserve": 19.66, "residual_usd_day": 0.5, "sources": {"budget_actual": 219.255, "ce_mtd": 219.2551}}
- `11:17:56` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:17:56` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:17:56` anomaly held (ANOM): none
- `11:17:56` fuse: {"checked_at": "2026-11-26T10:07:37.727425+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.25, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-stock-screener", 0.1], ["justhodl-signal-harvester", 0.06], ["justhodl-fundamental-graphs", 0.02], ["justhodl-backend-agent", 0.01], ["justhodl-a2a-bus", 0.01], ["justhodl-apac-flows", 0.01], ["justhodl-cost-anomaly", 0.01], ["justhodl-analytics-snapshot", 0.01]]}
- `11:17:56` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 21 functions ran, fleet $0.33/day; month-to-date top: justhodl-symdir $5.85, justhodl-polygon-bank $5.19, justhodl-eia-bank $3.58"], "mode": "enforce", "mtd": 219.26, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 7.38, "brake_at": 540.34, "budget_forecast": 252.633, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 4.54, "freeze_at": 580.34, "freeze_line_floored": false, "frozen_eom": 228.92, "lag_buffer": 7.38, "mtd": 219.26, "projected_eom": 260.11, "reserve": 19.66, "residual_source": "min7", "residual_usd_day": 0.5, "sources": {"budget_ac
- `11:17:56` cadence sentinel: {"checked": 844, "checked_at": "2026-11-26T00:07:37.962659+00:00", "day": "2026-11-26", "errors": [], "violators": []} | fixed entries: 0
- `11:17:56` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:17:56` meter: {"active": 21, "daily": {"2026-11-12": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [12.0, 0.0001], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [9.0, 4.2e-05], "benzinga-news-agent": [3.0, 2.9e-05], "bls-employment-api-v2": [1.0, 3.6e-05], "bls-labor-agent": [7.0, 0.000339], "bond-indices-agent": [8.0, 0.000616], "census-economic-agent": [7.0, 0.00031], "cftc-futures-positioning-agent": [7.0, 0.002297], "chatgpt-agent-api": [6.0, 0.0], "coinmarketcap-agent": [2.0, 1.4e-05], "daily-liquidity-report": [1.0, 8.8e-05], "dollar-strength-agent": [8.0, 0.000996], "eia-energy-agent": [1.0, 5.5e-05], "enhanced-repo-agent": [9.0, 9.8e-05], "fedliquidityapi": [10.
- `11:17:56` storage: day 2026-11-25 | versioning None | warn_days None | trip_days None
- `11:17:56` storage last assessment: {"day": "2026-11-24", "delta_gb": 6.67, "delta_objects": 124779, "delta_pct": 0.52, "gb": 1289.257, "level": null, "objects": 10481739, "prev": "2026-11-23", "prev_gb": 1282.59, "reason": "growth +6.7 GB (+0.5%), +124779 objects over 2026-11-24"}
- `11:17:56` S3 $/day (storage.s3_usd): {"2026-11-11": 7.4233, "2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709, "2026-11-22": 3.4863, "2026-11-23": 3.4513, "2026-11-24": 3.8432}
- `11:17:56` S3 line, last 8 days: 2026-11-17 $7.22, 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17, 2026-11-22 $3.49, 2026-11-23 $3.45, 2026-11-24 $3.84  (Nov 14-23: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17 / 3.49 / 3.45; Nov 24 would have been a 'high' day by the alternating pattern, the third day with the history book on write-if-changed; Nov 23 came in at $3.45)
- `11:17:56` service spend 2026-11-21: total $9.55 | Amazon Simple Storage Service $6.17, AWS Lambda $2.48, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.12, Amazon EC2 Container Registry  $0.09
- `11:17:56` service spend 2026-11-22: total $6.25 | Amazon Simple Storage Service $3.49, AWS Lambda $1.87, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.11, Amazon EC2 Container Registry  $0.09
- `11:17:56` service spend 2026-11-23: total $6.37 | Amazon Simple Storage Service $3.45, AWS Lambda $2.05, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.09
- `11:17:56` service spend 2026-11-24: total $6.67 | Amazon Simple Storage Service $3.84, AWS Lambda $1.92, AmazonCloudWatch $0.41, AWS Cost Explorer $0.12, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09
- `11:17:56` storage fuse advanced: day 2026-11-25 (was 2026-11-24 at the Nov 25 run)
- `11:17:56` object delta for 2026-11-24: +124779 (Nov 17-20 ran +42-49k/day; Nov 21 +99,943; Nov 22 +81,712; Nov 23 +89,738 = ~50k keys written + ~40k noncurrent versions, ops 6948)
- `11:17:56` ⚠ object delta +124779 is above +100k - yesterday's watch: factor-book had written 27,403 keys and polygon-full 12,635 by 11:20 UTC Nov 25; if Nov 25 and Nov 26 both read above +100k the factor book rewrites everything daily and should go write-if-changed (the history book pattern)
- `11:17:56` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:17:56` storage_selftest live days: {"2026-11-20": {"gb": 1262.27, "objects": 10085567}, "2026-11-21": {"gb": 1267.64, "objects": 10185510}, "2026-11-22": {"gb": 1275.104, "objects": 10267222}, "2026-11-23": {"gb": 1282.59, "objects": 10356960}, "2026-11-24": {"gb": 1289.257, "objects": 10481739}}
- `11:17:56` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:17:57` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:17:57` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:17:57` month-to-date top 25: symdir $5.85, polygon-bank $5.19, eia-bank $3.58, worldbank-full $3.35, research-backtest $1.97, feed-catalog $1.75, bank-histories $1.57, causality-scanner $1.38, data-census $0.88, signal-genealogy $0.85, fmp-walker $0.80, thesis-engine $0.65, katlin $0.62, tradingview $0.62, predictability $0.62, stock-screener $0.58, chart-patterns $0.57, wl-engines $0.57, bottom $0.56, fmp-bank $0.54
## C. governor event records since ops 6947 (2026-11-25 11:16 UTC) and the relay marker

- `11:17:57` event records since the Nov 25 run: 0 | kinds {} | newest -
- `11:17:57` relay marker 266.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:17:57` manifest 2026-11-26T05:15:00.875842+00:00: 784 engines, 1357 declared data/ keys
- `11:18:02` outputs by tier: {'DEAD': 158, 'FRESH': 1105, 'STALE': 70, 'MISSING': 24} (Wed Nov 25, ops 6947: fresh 1101, stale 71, dead 157, missing 24 of 1,353; Tue Nov 24: 1107 / 65 / 157 / 24; Thu Nov 19 read 1096 / 51 / 155 / 25)
- `11:18:02` SILENT engines - ALL outputs 54 h - 7 d old (Wed: 26, the Sunday/Monday weekly slots past 54 h; Tue: 17; a Thursday should read near 20-35): 37
- `11:18:02`   naaim: 1 output(s) 138-138 h
- `11:18:02`   liquidity-profile: 1 output(s) 133-133 h
- `11:18:02`   base-rates: 1 output(s) 117-117 h
- `11:18:02`   spx-beaters: 1 output(s) 117-117 h
- `11:18:02`   beaters-grader: 1 output(s) 116-116 h
- `11:18:02`   cftc-deep-view: 2 output(s) 110-110 h
- `11:18:02`   ici-flows: 1 output(s) 110-110 h
- `11:18:02`   forward-returns: 1 output(s) 104-104 h
- `11:18:02`   symbol-dictionary: 1 output(s) 102-102 h
- `11:18:02`   xbrl-fundamentals: 2 output(s) 101-101 h
- `11:18:02`   spx-history: 1 output(s) 99-99 h
- `11:18:02`   alpha-calibrator: 2 output(s) 98-98 h
- `11:18:02`   gsi-calibrator: 1 output(s) 98-98 h
- `11:18:02`   jsi-calibrator: 1 output(s) 98-98 h
- `11:18:02`   gsi-horizons: 1 output(s) 98-98 h
- `11:18:02`   signal-orthogonality: 1 output(s) 98-98 h
- `11:18:02`   edge-discovery: 2 output(s) 96-96 h
- `11:18:02`   bagger-engine: 1 output(s) 95-95 h
- `11:18:02`   strategy-portfolio: 1 output(s) 95-95 h
- `11:18:02`   hiring-velocity: 1 output(s) 95-95 h
- `11:18:02`   provider-window-sentinel: 1 output(s) 93-93 h
- `11:18:02`   ma-target-predictor: 1 output(s) 93-93 h
- `11:18:02`   weekly-ai-review: 1 output(s) 93-93 h
- `11:18:02`   meta-improver: 1 output(s) 85-85 h
- `11:18:02`   signal-halflife: 2 output(s) 77-77 h
- `11:18:02`   fabrication-weekly: 1 output(s) 77-77 h
- `11:18:02`   playbook-engine: 2 output(s) 76-76 h
- `11:18:02`   13f-clone-alpha: 1 output(s) 75-75 h
- `11:18:02`   sec-bulk: 1 output(s) 74-74 h
- `11:18:02`   bis-crossborder: 1 output(s) 73-73 h
- `11:18:02`   seasonality: 1 output(s) 72-72 h
- `11:18:02`   smart-money-13f: 1 output(s) 72-72 h
- `11:18:02`   imf-full: 2 output(s) 72-72 h
- `11:18:02`   buyback-scanner: 1 output(s) 71-71 h
- `11:18:02`   index-inclusion: 1 output(s) 70-70 h
- `11:18:02`   whales: 1 output(s) 70-70 h
- `11:18:02`   methodology-scout: 3 output(s) 57-57 h
- `11:18:02` engines with at least one output < 30 h: 689 of 784 (Wed: 690 of 784; Tue: 695; Mon: 624)
- `11:18:02` STALE 30h-7d (70), youngest 40: cftc-all-cache.json 31h, inventory-drawdown.json 36h, config/cryptoquant-spec.json 38h, cryptoquant-onchain.json 38h, history/alt-px-cm.json 38h, cryptoquant-series.json 38h, history/cryptoquant.json 38h, dealer-survey.json 43h, brain-compiler.json 46h, asset-discovery.json 46h, sec-cik-ticker-map.json 47h, source-intel.json 57h, methodology-gaps.json 57h, methodology-kb.json 57h, readthrough-state.json 61h, whales.json 70h, index-inclusion.json 70h, buyback-scanner.json 71h, warm/imf-full/_state/state.json 72h, warm/imf-full/manifest.json 72h, smart-money-13f.json 72h, seasonality.json 72h, bis-crossborder.json 73h, fundgraph/symdir2.json 74h, _state/sec-bulk.json 74h, 13f-clone-alpha.json 75h, playbook-rules-public.json 76h, playbook-rules.json 76h, audit/fabrication-weekly.json 77h, history/signal-halflife-history.json 77h, signal-halflife.json 77h, meta-improver-state.json 85h, symdir/fred-titles.json 86h, weekly-review-latest.json 93h, ma-target-predictor.json 93h, provider-window-sentinel.json 93h, port-cargo.json 95h, hiring-velocity.json 95h, strategy-portfolio.json 95h, bagger-engine.json 95h
- `11:18:02` DEAD >=7d (158); crossed the line since the Nov 25 run (1): providers/mof/weekly-securities.json 169h<-justhodl-global-flows
- `11:18:02` ⚠ 1 declared output(s) crossed the 7-day line since yesterday: providers/mof/weekly-securities.json<-justhodl-global-flows
- `11:18:02` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:18:02` watched outputs (desks, banks, monitors):
- `11:18:02`   data/strength.json: 7.6 h old (4420438 B) -> FRESH
- `11:18:02`   data/strength-wide.json: 7.6 h old (2129845 B) -> FRESH
- `11:18:02`   data/leaders.json: 7.5 h old (3097776 B) -> FRESH
- `11:18:02`   data/bottom.json: 7.5 h old (2294156 B) -> FRESH
- `11:18:03`   data/stock-volatility.json: 7.1 h old (2791729 B) -> FRESH
- `11:18:03`   data/campaign-scan.json: 7.2 h old (2263386 B) -> FRESH
- `11:18:03`   data/etf-true-flows.json: 19.4 h old (1119494 B) -> FRESH
- `11:18:03`   data/ici-flows.json: 110.2 h old (32177 B) -> STALE
- `11:18:03`   data/13f-book.json: 18.7 h old (2439113 B) -> FRESH
- `11:18:03`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1567 B) -> FRESH
- `11:18:03`   data/warm/factor-book/manifest.json: 8.3 h old (20391 B) -> FRESH
- `11:18:03`   data/warm/history-book/manifest.json: 8.0 h old (11578 B) -> FRESH
- `11:18:03`   data/corporate-actions/_state.json: 4.6 h old (66 B) -> FRESH
- `11:18:03`   data/_freshness-monitor.json: 3.3 h old (1108894 B) -> FRESH
- `11:18:03`   _health/fleet.json: 8.8 h old (9878 B) -> FRESH
- `11:18:03`   _health/cadence-manifest.json: 6.6 h old (113386 B) -> FRESH
- `11:18:03`   data/ciss-stress.json: 4.1 h old (4619039 B) -> FRESH
- `11:18:04`   data/ciss-sovereign.json: 4.1 h old (820790 B) -> FRESH
- `11:18:04`   data/ciss-sovereign-world.json: 4.0 h old (743530 B) -> FRESH
- `11:18:04`   data/ciss-dollar-shortage.json: 4.0 h old (1020663 B) -> FRESH
- `11:18:04`   data/ciss-sovereign-history.json: 4.1 h old (2333634 B) -> FRESH
- `11:18:04`   data/10kq-filings.json: 246.2 h old (6573546 B) -> DEAD
- `11:18:04`   data/insider-trades.json: 14.1 h old (1878588 B) -> FRESH
- `11:18:04`   data/regime-map.json: 14.3 h old (225399 B) -> FRESH
- `11:18:04`   data/regime-map-prev.json: 14.3 h old (27327 B) -> FRESH
- `11:18:04`   data/credit-before-equity.json: 12.7 h old (2841324 B) -> FRESH
- `11:18:04`   credit/credit-before-equity-history.json: 12.7 h old (121579 B) -> FRESH
- `11:18:04`   data/global-flow-map.json: 6.5 h old (190542 B) -> FRESH
- `11:18:04`   data/etf-flows.json: 23.7 h old (108879 B) -> FRESH
- `11:18:04`   data/feed-catalog.json: 196.3 h old (20011493 B) -> DEAD
## E. FMP bandwidth hold (walker + bank manifests) - the window rolled Nov 24 (walker wrote 11.6k files that day)

- `11:18:05` data/warm/fmp/manifest.json (11.7 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:18:05` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:18:05` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent True telegram_sent False
- `11:18:05`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3213, "inflight": 0, "total": 3213}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:18:05` data/ciss-research/sovereign: 8 object(s) | newest world-state.json 4.0 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 91.7 h
- `11:18:05` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market-history.json 13.4 h
- `11:18:05` data/dealer: 60 object(s)+ | newest dealer-gex.json 14.7 h | oldest migration.json 1631.0 h
- `11:18:05` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2442.9 h
- `11:18:05` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2711.8 h
## H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')

- `11:18:06` control 48.6 d old: contract capital-structure-recurring-refresh.v1 | status failed | started 2026-10-08T20:28:05.951329+00:00 | failed_at 2026-10-08T22:00:40.571580+00:00 | error_type ValueError | active_phase part-2 | completed ['plan', 'part-1'] | review {}
- `11:18:06` ⚠ capital-structure control still 'failed' (since 2026-10-08T22:00:40.571580+00:00) - every scheduled plan stops at 04:46 UTC until release_failed_control runs with an ops number and a review note (Khalid's call; this run does not release it)
- `11:18:06` ✅ read-only; nothing changed, nothing released, nothing re-invoked

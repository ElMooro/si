# ops 6994 - daily health status 2026-11-27 (governor status + caps + events since ops 6968, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Friday; read-only)

**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-11-27T11:18:47+00:00  

## Data

| 10kq-filings_age_h | 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | breadth-thrust_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | capstruct_failed_at | capstruct_status | cds-desk_age_h | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corp-bonds_age_h | corporate-actions__state_age_h | credit-before-equity-funds_age_h | credit-before-equity_age_h | credit_credit-before-equity-history_age_h | delta_objects | engines_fresh | engines_missed_two_slots | etf-flows_age_h | etf-true-flows_age_h | events_since_6968 | feed-catalog_age_h | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | global-flow-map_age_h | held_total | ici-flows_age_h | insider-trades_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | providers_mof_weekly-securities_age_h | regime-map-prev_age_h | regime-map_age_h | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | sec-fund-holdings_age_h | sec-money-funds_age_h | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h | warm_sec-datasets_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 270.2 | 18.7 | 3.3 | 0 | 7.3 | 7.5 | 13.5 | 229.08 | 254.916 | 10.7 | 7.2 | 600.0 | 3 | 2026-10-08T22:00:40.571580+00:00 | failed | 7.3 | 4.1 | 4.1 | 4.1 | 4.1 | 4.1 | 15.0 | 4.6 | 8.2 | 8.2 | 8.2 | 82373 | 688 | 40 | 23.7 | 19.4 | 0 | 220.3 | 8.8 | clear | clear | 11.6 | 3 | 134.2 | 14.1 | 7.5 | symdir $6.23; polygon-bank $5.41; eia-bank $3.75; worldbank-full $3.35; research-backtest $2.05; feed-catalog $1.92 | enforce | 8.8 | None | 229.08 | 0 | 157 | 1091 | 24 | 89 | 1361 | 262.12 | 1.3 | 14.3 | 14.3 | 290.9 | 13.7 | 8.8 | 13.4 | 4.1 | 2026-11-22 3.49; 2026-11-23 3.45; 2026-11-24 3.84; 2026-11-25 4.07 | 10.1 | 10.1 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.2 | 2026-11-26 | 1296.207 | None | none | 7.6 | 7.6 | 1.6.1 | None | 8.3 | 8.1 | 8.1 | 10.1 |

## Log
## A. governor status

- `11:18:43` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:18:43` cap $600.0 (budget) | budget limit $600.0 actual $229.08 forecast $254.916
- `11:18:43` thresholds: {"baseline_usd_day": 7.3, "brake_at": 541.02, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 3.53, "freeze_at": 581.02, "freeze_line_floored": false, "frozen_eom": 238.06, "lag_buffer": 7.3, "mtd": 229.08, "projected_eom": 262.12, "reserve": 18.98, "residual_usd_day": 0.48, "sources": {"budget_actual": 229.08, "ce_mtd": 226.9486}}
- `11:18:43` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:18:43` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:18:43` anomaly held (ANOM): none
- `11:18:43` fuse: {"checked_at": "2026-11-27T11:07:38.199070+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.25, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-cycle-features", 0.05], ["justhodl-cq-feed", 0.05], ["justhodl-apac-leadlag", 0.04], ["justhodl-earnings-sentiment", 0.02], ["justhodl-signal-scorecard", 0.02], ["justhodl-sdmx-walker", 0.02], ["justhodl-episode-reference", 0.01], ["justhodl-regime-anomaly", 0.01]]}
- `11:18:43` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 18 functions ran, fleet $0.25/day; month-to-date top: justhodl-symdir $6.23, justhodl-polygon-bank $5.41, justhodl-eia-bank $3.75"], "mode": "enforce", "mtd": 226.95, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 7.3, "brake_at": 541.01, "budget_forecast": 254.916, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 3.54, "freeze_at": 581.01, "freeze_line_floored": false, "frozen_eom": 235.94, "lag_buffer": 7.3, "mtd": 226.95, "projected_eom": 260.05, "reserve": 18.99, "residual_source": "min7", "residual_usd_day": 0.48, "sources": {"budget_act
- `11:18:43` cadence sentinel: {"checked": 844, "checked_at": "2026-11-27T00:07:37.609679+00:00", "day": "2026-11-27", "errors": [], "violators": []} | fixed entries: 0
- `11:18:43` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:18:43` meter: {"active": 18, "daily": {"2026-11-13": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [14.0, 0.000147], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [11.0, 6.7e-05], "benzinga-news-agent": [3.0, 3.6e-05], "bls-employment-api-v2": [1.0, 3.6e-05], "bls-labor-agent": [9.0, 0.000468], "bond-indices-agent": [10.0, 0.000978], "census-economic-agent": [9.0, 0.000328], "cftc-futures-positioning-agent": [7.0, 0.002548], "chatgpt-agent-api": [8.0, 0.0], "coinmarketcap-agent": [2.0, 1.2e-05], "daily-liquidity-report": [1.0, 0.000105], "dollar-strength-agent": [10.0, 0.001068], "eia-energy-agent": [1.0, 0.000387], "enhanced-repo-agent": [11.0, 0.000119], "fedliquidity
- `11:18:43` storage: day 2026-11-26 | versioning None | warn_days None | trip_days None
- `11:18:43` storage last assessment: {"day": "2026-11-25", "delta_gb": 6.95, "delta_objects": 82373, "delta_pct": 0.54, "gb": 1296.207, "level": null, "objects": 10564112, "prev": "2026-11-24", "prev_gb": 1289.257, "reason": "growth +7.0 GB (+0.5%), +82373 objects over 2026-11-25"}
- `11:18:43` S3 $/day (storage.s3_usd): {"2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709, "2026-11-22": 3.4863, "2026-11-23": 3.4513, "2026-11-24": 3.8432, "2026-11-25": 4.0733}
- `11:18:43` S3 line, last 8 days: 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17, 2026-11-22 $3.49, 2026-11-23 $3.45, 2026-11-24 $3.84, 2026-11-25 $4.07  (Nov 14-24: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17 / 3.49 / 3.45 / 3.84; the $7 days are gone since the history book went write-if-changed; Nov 25 is the first day with the overnight sessions' new engines writing)
- `11:18:43` service spend 2026-11-22: total $6.25 | Amazon Simple Storage Service $3.49, AWS Lambda $1.87, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.11, Amazon EC2 Container Registry  $0.09
- `11:18:43` service spend 2026-11-23: total $6.37 | Amazon Simple Storage Service $3.45, AWS Lambda $2.05, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.09
- `11:18:43` service spend 2026-11-24: total $6.67 | Amazon Simple Storage Service $3.84, AWS Lambda $1.92, AmazonCloudWatch $0.41, AWS Cost Explorer $0.12, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09
- `11:18:43` service spend 2026-11-25: total $6.87 | Amazon Simple Storage Service $4.07, AWS Lambda $1.94, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:18:43` storage fuse advanced: day 2026-11-26 (was 2026-11-25 at the Nov 26 run)
- `11:18:43` object delta for 2026-11-25: +82373 (Nov 17-20 ran +42-49k/day; Nov 21 +99,943; Nov 22 +81,712; Nov 23 +89,738; Nov 24 +124,779 = 65k keys written + noncurrent versions, ops 6948)
- `11:18:44` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:18:44` storage_selftest live days: {"2026-11-21": {"gb": 1267.64, "objects": 10185510}, "2026-11-22": {"gb": 1275.104, "objects": 10267222}, "2026-11-23": {"gb": 1282.59, "objects": 10356960}, "2026-11-24": {"gb": 1289.257, "objects": 10481739}, "2026-11-25": {"gb": 1296.207, "objects": 10564112}}
- `11:18:44` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:18:44` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:18:44` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:18:44` month-to-date top 25: symdir $6.23, polygon-bank $5.41, eia-bank $3.75, worldbank-full $3.35, research-backtest $2.05, feed-catalog $1.92, bank-histories $1.58, causality-scanner $1.44, data-census $0.92, signal-genealogy $0.90, fmp-walker $0.84, thesis-engine $0.69, katlin $0.65, tradingview $0.65, predictability $0.62, wl-engines $0.60, stock-screener $0.58, bottom $0.58, chart-patterns $0.57, polygon-options-flow $0.57
## C. governor event records since ops 6968 (2026-11-26 11:17 UTC) and the relay marker

- `11:18:44` event records since the Nov 26 run: 0 | kinds {} | newest -
- `11:18:44` relay marker 290.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:18:44` manifest 2026-11-27T03:58:51.593836+00:00: 785 engines, 1361 declared data/ keys
- `11:18:46` outputs by tier: {'DEAD': 157, 'FRESH': 1091, 'STALE': 89, 'MISSING': 24} (Thu Nov 26, ops 6968: fresh 1105, stale 70, dead 158, missing 24 of 1,357; Wed Nov 25: 1101 / 71 / 157 / 24)
- `11:18:46` SILENT engines - ALL outputs 54 h - 7 d old (Thu: 37, the Sat/Sun + Monday weekly slots past 54 h; Wed: 26; a Friday should read near 30-45 as the Tuesday weekly slots join): 40
- `11:18:46`   liquidity-profile: 1 output(s) 157-157 h
- `11:18:46`   base-rates: 1 output(s) 141-141 h
- `11:18:46`   spx-beaters: 1 output(s) 141-141 h
- `11:18:46`   beaters-grader: 1 output(s) 140-140 h
- `11:18:46`   cftc-deep-view: 2 output(s) 134-134 h
- `11:18:46`   ici-flows: 1 output(s) 134-134 h
- `11:18:46`   forward-returns: 1 output(s) 128-128 h
- `11:18:46`   symbol-dictionary: 1 output(s) 126-126 h
- `11:18:46`   xbrl-fundamentals: 2 output(s) 125-125 h
- `11:18:46`   spx-history: 1 output(s) 123-123 h
- `11:18:46`   alpha-calibrator: 2 output(s) 122-122 h
- `11:18:46`   gsi-calibrator: 1 output(s) 122-122 h
- `11:18:46`   jsi-calibrator: 1 output(s) 122-122 h
- `11:18:46`   gsi-horizons: 1 output(s) 122-122 h
- `11:18:46`   signal-orthogonality: 1 output(s) 122-122 h
- `11:18:46`   edge-discovery: 2 output(s) 120-120 h
- `11:18:46`   bagger-engine: 1 output(s) 119-119 h
- `11:18:46`   strategy-portfolio: 1 output(s) 119-119 h
- `11:18:46`   hiring-velocity: 1 output(s) 119-119 h
- `11:18:46`   provider-window-sentinel: 1 output(s) 117-117 h
- `11:18:46`   ma-target-predictor: 1 output(s) 117-117 h
- `11:18:46`   weekly-ai-review: 1 output(s) 117-117 h
- `11:18:46`   meta-improver: 1 output(s) 109-109 h
- `11:18:46`   signal-halflife: 2 output(s) 101-101 h
- `11:18:46`   fabrication-weekly: 1 output(s) 101-101 h
- `11:18:46`   playbook-engine: 2 output(s) 100-100 h
- `11:18:46`   13f-clone-alpha: 1 output(s) 99-99 h
- `11:18:46`   sec-bulk: 1 output(s) 98-98 h
- `11:18:46`   bis-crossborder: 1 output(s) 97-97 h
- `11:18:46`   seasonality: 1 output(s) 96-96 h
- `11:18:46`   smart-money-13f: 1 output(s) 96-96 h
- `11:18:46`   imf-full: 2 output(s) 96-96 h
- `11:18:46`   buyback-scanner: 1 output(s) 95-95 h
- `11:18:46`   index-inclusion: 1 output(s) 94-94 h
- `11:18:46`   whales: 1 output(s) 94-94 h
- `11:18:46`   methodology-scout: 3 output(s) 81-81 h
- `11:18:46`   asset-discovery: 1 output(s) 70-70 h
- `11:18:46`   brain-compiler: 1 output(s) 70-70 h
- `11:18:46`   nyfed-dealer-survey: 1 output(s) 67-67 h
- `11:18:46`   inventory-drawdown: 1 output(s) 60-60 h
- `11:18:46` engines with at least one output < 30 h: 688 of 785 (Thu: 689 of 784; Wed: 690; Tue: 695)
- `11:18:46` STALE 30h-7d (89), youngest 40: warm/katlin/splits.json 31h, gdelt-news.json 32h, warm/real-economy/_summary.json 33h, warm/real-economy/acc_cab.json 33h, warm/real-economy/destatis_toll.json 33h, warm/real-economy/aar_rail.json 33h, warm/real-economy/copper.json 33h, warm/real-economy/noaa_degree_days.json 33h, warm/real-economy/aisi_steel.json 33h, warm/real-economy/fbx.json 33h, warm/real-economy/tsa.json 33h, _fleet-monitor-alert-history.json 33h, miss-summary.json 34h, crypto-emergence.json 36h, polygon-related-graph.json 37h, warm/polygon-full/today-manifest.json 37h, warm/polygon-full/today.json.gz 37h, credit-before-equity-evidence.json 42h, credit-before-equity-history.json 42h, sellside-views.json 45h, futures-research.json 46h, polygon-futures-curves.json 46h, warm/census-us/catalog.json.gz 52h, inventory-drawdown.json 60h, config/cryptoquant-spec.json 62h, cryptoquant-onchain.json 62h, history/alt-px-cm.json 62h, cryptoquant-series.json 62h, history/cryptoquant.json 62h, dealer-survey.json 67h, brain-compiler.json 70h, asset-discovery.json 70h, sec-cik-ticker-map.json 71h, source-intel.json 81h, methodology-gaps.json 81h, methodology-kb.json 81h, readthrough-state.json 85h, whales.json 94h, index-inclusion.json 94h, buyback-scanner.json 95h
- `11:18:46` DEAD >=7d (157); crossed the line since the Nov 26 run (0): 
- `11:18:46` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:18:46` watched outputs (desks, banks, monitors):
- `11:18:46`   data/strength.json: 7.6 h old (4423082 B) -> FRESH
- `11:18:46`   data/strength-wide.json: 7.6 h old (2129896 B) -> FRESH
- `11:18:46`   data/leaders.json: 7.5 h old (3098509 B) -> FRESH
- `11:18:46`   data/bottom.json: 7.5 h old (2293218 B) -> FRESH
- `11:18:46`   data/stock-volatility.json: 7.2 h old (2791736 B) -> FRESH
- `11:18:46`   data/campaign-scan.json: 7.2 h old (2263390 B) -> FRESH
- `11:18:46`   data/etf-true-flows.json: 19.4 h old (1107325 B) -> FRESH
- `11:18:46`   data/ici-flows.json: 134.2 h old (32177 B) -> STALE
- `11:18:46`   data/13f-book.json: 18.7 h old (2438685 B) -> FRESH
- `11:18:46`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1456 B) -> FRESH
- `11:18:46`   data/warm/factor-book/manifest.json: 8.3 h old (20391 B) -> FRESH
- `11:18:46`   data/warm/history-book/manifest.json: 8.1 h old (12023 B) -> FRESH
- `11:18:46`   data/corporate-actions/_state.json: 4.6 h old (66 B) -> FRESH
- `11:18:46`   data/_freshness-monitor.json: 3.3 h old (1116168 B) -> FRESH
- `11:18:46`   _health/fleet.json: 8.8 h old (9556 B) -> FRESH
- `11:18:46`   _health/cadence-manifest.json: 10.7 h old (113386 B) -> FRESH
- `11:18:46`   data/ciss-stress.json: 4.1 h old (4620578 B) -> FRESH
- `11:18:46`   data/ciss-sovereign.json: 4.1 h old (820774 B) -> FRESH
- `11:18:46`   data/ciss-sovereign-world.json: 4.1 h old (745691 B) -> FRESH
- `11:18:46`   data/ciss-dollar-shortage.json: 4.1 h old (1020683 B) -> FRESH
- `11:18:46`   data/ciss-sovereign-history.json: 4.1 h old (2333929 B) -> FRESH
- `11:18:46`   data/10kq-filings.json: 270.2 h old (6573546 B) -> DEAD
- `11:18:46`   data/insider-trades.json: 14.1 h old (2089880 B) -> FRESH
- `11:18:46`   data/regime-map.json: 14.3 h old (220371 B) -> FRESH
- `11:18:47`   data/regime-map-prev.json: 14.3 h old (27320 B) -> FRESH
- `11:18:47`   data/credit-before-equity.json: 8.2 h old (3214700 B) -> FRESH
- `11:18:47`   credit/credit-before-equity-history.json: 8.2 h old (124051 B) -> FRESH
- `11:18:47`   data/global-flow-map.json: 11.6 h old (188156 B) -> FRESH
- `11:18:47`   data/etf-flows.json: 23.7 h old (109955 B) -> FRESH
- `11:18:47`   data/feed-catalog.json: 220.3 h old (20011493 B) -> DEAD
- `11:18:47`   data/sec-fund-holdings.json: 10.1 h old (52920 B) -> FRESH
- `11:18:47`   data/sec-money-funds.json: 10.1 h old (27030 B) -> FRESH
- `11:18:47`   data/warm/sec-datasets/manifest.json: 10.1 h old (32260 B) -> FRESH
- `11:18:47`   data/credit-before-equity-funds.json: 8.2 h old (6377917 B) -> FRESH
- `11:18:47`   data/corp-bonds.json: 15.0 h old (8939426 B) -> FRESH
- `11:18:47`   data/breadth-thrust.json: 13.5 h old (9695 B) -> FRESH
- `11:18:47`   data/cds-desk.json: 7.3 h old (1451768 B) -> FRESH
- `11:18:47`   data/providers/mof/weekly-securities.json: 1.3 h old (187498 B) -> FRESH
## E. FMP bandwidth hold (walker + bank manifests) - both read clear Nov 25 and Nov 26

- `11:18:47` data/warm/fmp/manifest.json (11.7 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:18:47` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:18:47` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent False telegram_sent False
- `11:18:47`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3205, "inflight": 0, "total": 3205}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:18:47` data/ciss-research/sovereign: 9 object(s) | newest world-state.json 4.1 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 115.7 h
- `11:18:47` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market-history.json 13.4 h
- `11:18:47` data/dealer: 60 object(s)+ | newest d76c06647bcf234fcb3403ff243c3d96a63bde0337fd9b5277 13.7 h | oldest migration.json 1655.0 h
- `11:18:47` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2467.0 h
- `11:18:47` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2735.8 h
## H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')

- `11:18:47` control 49.6 d old: contract capital-structure-recurring-refresh.v1 | status failed | started 2026-10-08T20:28:05.951329+00:00 | failed_at 2026-10-08T22:00:40.571580+00:00 | error_type ValueError | active_phase part-2 | completed ['plan', 'part-1'] | review {}
- `11:18:47` ⚠ capital-structure control still 'failed' (since 2026-10-08T22:00:40.571580+00:00) - every scheduled plan stops at 04:46 UTC until release_failed_control runs with an ops number and a review note (Khalid's call; this run does not release it)
- `11:18:47` ✅ read-only; nothing changed, nothing released, nothing re-invoked

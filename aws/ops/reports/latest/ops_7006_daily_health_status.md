# ops 7006 - daily health status 2026-11-28 (governor status + caps + events since ops 6994, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Saturday; read-only)

**Status:** success  
**Duration:** 13.7s  
**Finished:** 2026-11-28T11:17:53+00:00  

## Data

| 10kq-filings_age_h | 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | breadth-thrust_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | canary-grid_age_h | canary-warroom_age_h | cap | capped | capstruct_failed_at | capstruct_status | cds-desk_age_h | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corp-bonds_age_h | corporate-actions__state_age_h | country-risk-premiums_age_h | credit-before-equity-funds_age_h | credit-before-equity_age_h | credit_credit-before-equity-history_age_h | crisis-canaries_age_h | delta_objects | engines_fresh | engines_missed_two_slots | equity-risk-premium_age_h | etf-flows_age_h | etf-true-flows_age_h | eva-spread_age_h | events_since_6994 | feed-catalog_age_h | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | global-flow-map_age_h | held_total | ici-flows_age_h | industry-benchmarks_age_h | insider-trades_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | providers_mof_weekly-securities_age_h | regime-map-prev_age_h | regime-map_age_h | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | sec-fund-holdings_age_h | sec-money-funds_age_h | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_damodaran_manifest_age_h | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h | warm_sec-datasets_manifest_age_h | warroom-history_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 294.2 | 18.7 | 3.3 | 0 | 7.02 | 7.5 | 13.5 | 236.358 | 255.529 | 7.2 | 7.2 | 18.1 | 14.5 | 600.0 | 3 | 2026-10-08T22:00:40.571580+00:00 | failed | 7.3 | 4.1 | 4.1 | 4.1 | 4.1 | 4.1 | 15.0 | 4.6 | 5.8 | 12.7 | 12.7 | 12.7 | 21.7 | 125163 | 697 | 37 | 5.8 | 23.7 | 19.4 | 5.8 | 0 | 244.3 | 8.8 | clear | clear | 11.6 | 3 | 158.2 | 5.8 | 14.1 | 7.5 | symdir $6.43; polygon-bank $5.66; eia-bank $3.92; worldbank-full $3.35; research-backtest $2.14; feed-catalog $2.09 | enforce | 8.8 | None | 236.36 | 1 | 156 | 1118 | 24 | 68 | 1366 | 261.13 | 25.3 | 14.3 | 14.3 | 314.9 | 13.7 | 8.8 | 13.4 | 4.1 | 2026-11-23 3.45; 2026-11-24 3.84; 2026-11-25 4.07; 2026-11-26 4.17 | 12.4 | 12.4 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.1 | 2026-11-27 | 1305.568 | None | none | 7.6 | 7.6 | 1.6.1 | None | 5.8 | 8.3 | 8.1 | 8.1 | 12.4 | 14.5 |

## Log
## A. governor status

- `11:17:41` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:17:41` cap $600.0 (budget) | budget limit $600.0 actual $236.358 forecast $255.529
- `11:17:41` thresholds: {"baseline_usd_day": 7.02, "brake_at": 541.28, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 2.53, "freeze_at": 581.28, "freeze_line_floored": false, "frozen_eom": 245.08, "lag_buffer": 7.02, "mtd": 236.36, "projected_eom": 261.13, "reserve": 18.72, "residual_usd_day": 0.67, "sources": {"budget_actual": 236.358, "ce_mtd": 234.0766}}
- `11:17:41` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:17:41` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:17:41` anomaly held (ANOM): none
- `11:17:41` fuse: {"checked_at": "2026-11-28T10:07:37.866575+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.24, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-stock-screener", 0.1], ["justhodl-signal-harvester", 0.06], ["justhodl-fundamental-graphs", 0.01], ["justhodl-backend-agent", 0.01], ["justhodl-a2a-bus", 0.01], ["justhodl-cost-anomaly", 0.01], ["justhodl-analytics-snapshot", 0.01], ["justhodl-hkma-monitor", 0.01]]}
- `11:17:41` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 19 functions ran, fleet $0.70/day; month-to-date top: justhodl-symdir $6.44, justhodl-polygon-bank $5.66, justhodl-eia-bank $3.92"], "mode": "enforce", "mtd": 234.08, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 7.02, "brake_at": 541.28, "budget_forecast": 255.529, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 2.54, "freeze_at": 581.28, "freeze_line_floored": false, "frozen_eom": 242.8, "lag_buffer": 7.02, "mtd": 234.08, "projected_eom": 258.9, "reserve": 18.72, "residual_source": "min7", "residual_usd_day": 0.67, "sources": {"budget_act
- `11:17:41` cadence sentinel: {"checked": 845, "checked_at": "2026-11-28T00:07:38.129399+00:00", "day": "2026-11-28", "errors": [], "violators": []} | fixed entries: 0
- `11:17:41` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:17:41` meter: {"active": 19, "daily": {"2026-11-14": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [16.0, 0.000123], "alphavantage-technical-analysis": [2.0, 4e-06], "bea-economic-agent": [11.0, 5.6e-05], "benzinga-news-agent": [3.0, 2.9e-05], "bls-employment-api-v2": [1.0, 3.6e-05], "bls-labor-agent": [9.0, 0.000504], "bond-indices-agent": [10.0, 0.001241], "census-economic-agent": [9.0, 0.000297], "cftc-futures-positioning-agent": [7.0, 0.002553], "chatgpt-agent-api": [8.0, 1e-06], "coinmarketcap-agent": [2.0, 1.3e-05], "daily-liquidity-report": [1.0, 6.1e-05], "dollar-strength-agent": [10.0, 0.0015], "eia-energy-agent": [1.0, 0.000399], "enhanced-repo-agent": [11.0, 8e-05], "fedliquidityapi"
- `11:17:41` storage: day 2026-11-27 | versioning None | warn_days None | trip_days None
- `11:17:41` storage last assessment: {"day": "2026-11-26", "delta_gb": 9.36, "delta_objects": 125163, "delta_pct": 0.72, "gb": 1305.568, "level": null, "objects": 10689275, "prev": "2026-11-25", "prev_gb": 1296.207, "reason": "growth +9.4 GB (+0.7%), +125163 objects over 2026-11-26"}
- `11:17:41` S3 $/day (storage.s3_usd): {"2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709, "2026-11-22": 3.4863, "2026-11-23": 3.4513, "2026-11-24": 3.8432, "2026-11-25": 4.0733, "2026-11-26": 4.1698}
- `11:17:41` S3 line, last 8 days: 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17, 2026-11-22 $3.49, 2026-11-23 $3.45, 2026-11-24 $3.84, 2026-11-25 $4.07, 2026-11-26 $4.17  (Nov 14-25: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17 / 3.49 / 3.45 / 3.84 / 4.07; the $7 days are gone since the history book went write-if-changed; Nov 26 is the first day with the Nov 26 build session's engines writing)
- `11:17:41` service spend 2026-11-23: total $6.37 | Amazon Simple Storage Service $3.45, AWS Lambda $2.05, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.09
- `11:17:41` service spend 2026-11-24: total $6.67 | Amazon Simple Storage Service $3.84, AWS Lambda $1.92, AmazonCloudWatch $0.41, AWS Cost Explorer $0.12, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09
- `11:17:41` service spend 2026-11-25: total $6.87 | Amazon Simple Storage Service $4.07, AWS Lambda $1.94, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:17:41` service spend 2026-11-26: total $7.11 | Amazon Simple Storage Service $4.17, AWS Lambda $2.06, AmazonCloudWatch $0.43, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:17:41` storage fuse advanced: day 2026-11-27 (was 2026-11-26 at the Nov 27 run)
- `11:17:41` object delta for 2026-11-26: +125163 (Nov 17-20 ran +42-49k/day; Nov 21 +99,943; Nov 22 +81,712; Nov 23 +89,738; Nov 24 +124,779; Nov 25 +82,373 - the factor-book watch closed)
- `11:17:41` ⚠ object delta +125163 is above +100k (Nov 24 read +124,779 was the only day above it; Nov 25 fell back to +82,373): if the Nov 26 read is above +100k again the Nov 26-27 build sessions' new engines (sec-datasets, corp-bonds, canaries record) are the first suspects - a build session's job, Khalid's call
- `11:17:42` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:17:42` storage_selftest live days: {"2026-11-22": {"gb": 1275.104, "objects": 10267222}, "2026-11-23": {"gb": 1282.59, "objects": 10356960}, "2026-11-24": {"gb": 1289.257, "objects": 10481739}, "2026-11-25": {"gb": 1296.207, "objects": 10564112}, "2026-11-26": {"gb": 1305.568, "objects": 10689275}}
- `11:17:42` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:17:42` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:17:42` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:17:42` month-to-date top 25: symdir $6.43, polygon-bank $5.66, eia-bank $3.92, worldbank-full $3.35, research-backtest $2.14, feed-catalog $2.09, bank-histories $1.61, causality-scanner $1.50, data-census $0.96, signal-genealogy $0.95, fmp-walker $0.89, thesis-engine $0.73, katlin $0.68, tradingview $0.68, wl-engines $0.63, predictability $0.62, bottom $0.61, stock-screener $0.60, polygon-options-flow $0.60, fmp-bank $0.59
## C. governor event records since ops 6994 (2026-11-27 11:18 UTC) and the relay marker

- `11:17:42` event records since the Nov 27 run: 0 | kinds {} | newest -
- `11:17:43` relay marker 314.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:17:43` manifest 2026-11-28T04:33:20.992631+00:00: 786 engines, 1366 declared data/ keys
- `11:17:48` outputs by tier: {'DEAD': 156, 'FRESH': 1118, 'STALE': 68, 'MISSING': 24} (Fri Nov 27, ops 6994: fresh 1091, stale 89, dead 157, missing 24 of 1,361; Thu Nov 26: 1105 / 70 / 158 / 24)
- `11:17:48` SILENT engines - ALL outputs 54 h - 7 d old (Fri: 40, Thu: 37, the weekly slots past 54 h; a Saturday should read near 35-50 as the Wednesday weekly slots join): 37
- `11:17:48`   base-rates: 1 output(s) 165-165 h
- `11:17:48`   spx-beaters: 1 output(s) 165-165 h
- `11:17:48`   beaters-grader: 1 output(s) 164-164 h
- `11:17:48`   ici-flows: 1 output(s) 158-158 h
- `11:17:48`   forward-returns: 1 output(s) 152-152 h
- `11:17:48`   symbol-dictionary: 1 output(s) 150-150 h
- `11:17:48`   xbrl-fundamentals: 2 output(s) 149-149 h
- `11:17:48`   spx-history: 1 output(s) 147-147 h
- `11:17:48`   alpha-calibrator: 2 output(s) 146-146 h
- `11:17:48`   gsi-calibrator: 1 output(s) 146-146 h
- `11:17:48`   jsi-calibrator: 1 output(s) 146-146 h
- `11:17:48`   gsi-horizons: 1 output(s) 146-146 h
- `11:17:48`   signal-orthogonality: 1 output(s) 146-146 h
- `11:17:48`   edge-discovery: 2 output(s) 144-144 h
- `11:17:48`   bagger-engine: 1 output(s) 143-143 h
- `11:17:48`   strategy-portfolio: 1 output(s) 143-143 h
- `11:17:48`   hiring-velocity: 1 output(s) 143-143 h
- `11:17:48`   provider-window-sentinel: 1 output(s) 141-141 h
- `11:17:48`   ma-target-predictor: 1 output(s) 141-141 h
- `11:17:48`   weekly-ai-review: 1 output(s) 141-141 h
- `11:17:48`   meta-improver: 1 output(s) 133-133 h
- `11:17:48`   signal-halflife: 2 output(s) 125-125 h
- `11:17:48`   fabrication-weekly: 1 output(s) 125-125 h
- `11:17:48`   playbook-engine: 2 output(s) 124-124 h
- `11:17:48`   13f-clone-alpha: 1 output(s) 123-123 h
- `11:17:48`   sec-bulk: 1 output(s) 122-122 h
- `11:17:48`   bis-crossborder: 1 output(s) 121-121 h
- `11:17:48`   seasonality: 1 output(s) 120-120 h
- `11:17:48`   smart-money-13f: 1 output(s) 120-120 h
- `11:17:48`   imf-full: 2 output(s) 120-120 h
- `11:17:48`   buyback-scanner: 1 output(s) 119-119 h
- `11:17:48`   index-inclusion: 1 output(s) 118-118 h
- `11:17:48`   whales: 1 output(s) 118-118 h
- `11:17:48`   methodology-scout: 3 output(s) 105-105 h
- `11:17:48`   nyfed-dealer-survey: 1 output(s) 91-91 h
- `11:17:48`   inventory-drawdown: 1 output(s) 84-84 h
- `11:17:48`   crypto-emergence: 1 output(s) 60-60 h
- `11:17:48` engines with at least one output < 30 h: 697 of 786 (Fri: 688 of 785; Thu: 689 of 784; Wed: 690)
- `11:17:48` STALE 30h-7d (68), youngest 40: stock-xray.json 35h, port-cargo.json 47h, warm/fred-scoped/Dollar_TradeWeighted/TWEXEMEGSMTH.json 53h, warm/fred-scoped/Dollar_TradeWeighted/TWEXAFEGSMTH.json 53h, warm/fred-scoped/Dollar_TradeWeighted/TWEXBGSMTH.json 53h, warm/fred-scoped/Dollar_TradeWeighted/RTWEXBGS.json 53h, crypto-emergence.json 60h, polygon-related-graph.json 61h, credit-before-equity-evidence.json 66h, credit-before-equity-history.json 66h, warm/census-us/catalog.json.gz 76h, inventory-drawdown.json 84h, config/cryptoquant-spec.json 86h, cryptoquant-onchain.json 86h, history/alt-px-cm.json 86h, cryptoquant-series.json 86h, history/cryptoquant.json 86h, dealer-survey.json 91h, sec-cik-ticker-map.json 95h, source-intel.json 105h, methodology-gaps.json 105h, methodology-kb.json 105h, whales.json 118h, index-inclusion.json 118h, buyback-scanner.json 119h, warm/imf-full/_state/state.json 120h, warm/imf-full/manifest.json 120h, smart-money-13f.json 120h, seasonality.json 120h, bis-crossborder.json 121h, fundgraph/symdir2.json 122h, _state/sec-bulk.json 122h, 13f-clone-alpha.json 123h, playbook-rules-public.json 124h, playbook-rules.json 124h, audit/fabrication-weekly.json 125h, history/signal-halflife-history.json 125h, signal-halflife.json 125h, meta-improver-state.json 133h, weekly-review-latest.json 141h
- `11:17:48` DEAD >=7d (156); crossed the line since the Nov 27 run (1): compound-history.json 182h<-justhodl-compound-aggregator
- `11:17:48` ⚠ 1 declared output(s) crossed the 7-day line since yesterday: compound-history.json<-justhodl-compound-aggregator
- `11:17:48` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:17:48` watched outputs (desks, banks, monitors):
- `11:17:48`   data/strength.json: 7.6 h old (4477327 B) -> FRESH
- `11:17:48`   data/strength-wide.json: 7.6 h old (2126297 B) -> FRESH
- `11:17:48`   data/leaders.json: 7.5 h old (3100221 B) -> FRESH
- `11:17:48`   data/bottom.json: 7.5 h old (2308059 B) -> FRESH
- `11:17:48`   data/stock-volatility.json: 7.1 h old (2784415 B) -> FRESH
- `11:17:48`   data/campaign-scan.json: 7.2 h old (2338673 B) -> FRESH
- `11:17:48`   data/etf-true-flows.json: 19.4 h old (1107901 B) -> FRESH
- `11:17:48`   data/ici-flows.json: 158.2 h old (32177 B) -> STALE
- `11:17:48`   data/13f-book.json: 18.7 h old (2438687 B) -> FRESH
- `11:17:48`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1580 B) -> FRESH
- `11:17:48`   data/warm/factor-book/manifest.json: 8.3 h old (20391 B) -> FRESH
- `11:17:49`   data/warm/history-book/manifest.json: 8.1 h old (11564 B) -> FRESH
- `11:17:49`   data/corporate-actions/_state.json: 4.6 h old (66 B) -> FRESH
- `11:17:49`   data/_freshness-monitor.json: 3.3 h old (1106896 B) -> FRESH
- `11:17:49`   _health/fleet.json: 8.8 h old (10073 B) -> FRESH
- `11:17:49`   _health/cadence-manifest.json: 7.2 h old (113968 B) -> FRESH
- `11:17:49`   data/ciss-stress.json: 4.1 h old (4620575 B) -> FRESH
- `11:17:49`   data/ciss-sovereign.json: 4.1 h old (820772 B) -> FRESH
- `11:17:49`   data/ciss-sovereign-world.json: 4.1 h old (747984 B) -> FRESH
- `11:17:49`   data/ciss-dollar-shortage.json: 4.1 h old (1020782 B) -> FRESH
- `11:17:49`   data/ciss-sovereign-history.json: 4.1 h old (2333929 B) -> FRESH
- `11:17:49`   data/10kq-filings.json: 294.2 h old (6573546 B) -> DEAD
- `11:17:50`   data/insider-trades.json: 14.1 h old (2099306 B) -> FRESH
- `11:17:50`   data/regime-map.json: 14.3 h old (226244 B) -> FRESH
- `11:17:50`   data/regime-map-prev.json: 14.3 h old (27295 B) -> FRESH
- `11:17:50`   data/credit-before-equity.json: 12.7 h old (3213371 B) -> FRESH
- `11:17:50`   credit/credit-before-equity-history.json: 12.7 h old (124051 B) -> FRESH
- `11:17:50`   data/global-flow-map.json: 11.6 h old (187947 B) -> FRESH
- `11:17:50`   data/etf-flows.json: 23.7 h old (109930 B) -> FRESH
- `11:17:50`   data/feed-catalog.json: 244.3 h old (20011493 B) -> DEAD
- `11:17:50`   data/sec-fund-holdings.json: 12.4 h old (53175 B) -> FRESH
- `11:17:50`   data/sec-money-funds.json: 12.4 h old (37715 B) -> FRESH
- `11:17:50`   data/warm/sec-datasets/manifest.json: 12.4 h old (59173 B) -> FRESH
- `11:17:50`   data/credit-before-equity-funds.json: 12.7 h old (6377917 B) -> FRESH
- `11:17:51`   data/corp-bonds.json: 15.0 h old (8940054 B) -> FRESH
- `11:17:51`   data/breadth-thrust.json: 13.5 h old (9686 B) -> FRESH
- `11:17:51`   data/cds-desk.json: 7.3 h old (1448925 B) -> FRESH
- `11:17:51`   data/providers/mof/weekly-securities.json: 25.3 h old (187498 B) -> FRESH
- `11:17:51`   data/canary-warroom.json: 14.5 h old (185403 B) -> FRESH
- `11:17:51`   data/warroom-history.json: 14.5 h old (77796 B) -> FRESH
- `11:17:51`   data/canary-grid.json: 18.1 h old (30288 B) -> FRESH
- `11:17:51`   data/crisis-canaries.json: 21.7 h old (12245 B) -> FRESH
- `11:17:51`   data/country-risk-premiums.json: 5.8 h old (15825 B) -> FRESH
- `11:17:51`   data/equity-risk-premium.json: 5.8 h old (78806 B) -> FRESH
- `11:17:51`   data/industry-benchmarks.json: 5.8 h old (213012 B) -> FRESH
- `11:17:52`   data/warm/damodaran/manifest.json: 5.8 h old (112943 B) -> FRESH
- `11:17:52`   data/eva-spread.json: 5.8 h old (118851 B) -> FRESH
## E. FMP bandwidth hold (walker + bank manifests) - both read clear Nov 25, 26 and 27

- `11:17:52` data/warm/fmp/manifest.json (11.7 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:17:52` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:17:52` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent True telegram_sent False
- `11:17:52`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3186, "inflight": 0, "total": 3186}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:17:52` data/ciss-research/sovereign: 10 object(s) | newest world-state.json 4.1 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 139.7 h
- `11:17:52` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market.json 13.4 h
- `11:17:52` data/dealer: 60 object(s)+ | newest b19f73d96d6812789e93b0a009c0c2ac59e5e4028e8f9cda5c 13.7 h | oldest migration.json 1679.0 h
- `11:17:53` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2490.9 h
- `11:17:53` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2759.8 h
## H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')

- `11:17:53` control 50.6 d old: contract capital-structure-recurring-refresh.v1 | status failed | started 2026-10-08T20:28:05.951329+00:00 | failed_at 2026-10-08T22:00:40.571580+00:00 | error_type ValueError | active_phase part-2 | completed ['plan', 'part-1'] | review {}
- `11:17:53` ⚠ capital-structure control still 'failed' (since 2026-10-08T22:00:40.571580+00:00) - every scheduled plan stops at 04:46 UTC until release_failed_control runs with an ops number and a review note (Khalid's call; this run does not release it)
- `11:17:53` ✅ read-only; nothing changed, nothing released, nothing re-invoked

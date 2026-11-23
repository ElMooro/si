# ops 6927 - daily health status 2026-11-23 (governor status + caps + events since ops 6910, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs; Monday; read-only)

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-11-23T11:17:02+00:00  

## Data

| 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corporate-actions__state_age_h | engines_fresh | engines_missed_two_slots | etf-true-flows_age_h | events_since_6910 | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | ici-flows_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 18.7 | 3.3 | 0 | 8.85 | 55.5 | 200.271 | 276.636 | 19.8 | 7.2 | 600.0 | 3 | 4.0 | 4.0 | 4.0 | 4.0 | 4.1 | 4.5 | 624 | 85 | 19.4 | 0 | 8.8 | clear | clear | 3 | 38.1 | 7.4 | symdir $5.00; polygon-bank $4.51; worldbank-full $3.35; eia-bank $3.08; research-backtest $1.71; bank-histories $1.49 | enforce | 8.8 | None | 200.27 | 5 | 156 | 979 | 24 | 194 | 1353 | 275.79 | 194.9 | 30.8 | 8.8 | 13.4 | 4.0 | 2026-11-18 4.07; 2026-11-19 7.36; 2026-11-20 4.45; 2026-11-21 6.17 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.1 | 2026-11-22 | 1267.64 | None | none | 7.6 | 7.6 | 1.6.1 | None | 8.3 | 8.1 | 8.1 |

## Log
## A. governor status

- `11:16:59` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:16:59` cap $600.0 (budget) | budget limit $600.0 actual $200.271 forecast $276.636
- `11:16:59` thresholds: {"baseline_usd_day": 8.85, "brake_at": 537.28, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 7.53, "freeze_at": 577.28, "freeze_line_floored": false, "frozen_eom": 212.99, "lag_buffer": 8.85, "mtd": 200.27, "projected_eom": 275.79, "reserve": 22.72, "residual_usd_day": 0.51, "sources": {"budget_actual": 200.271, "ce_mtd": 200.2712}}
- `11:16:59` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:16:59` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:16:59` anomaly held (ANOM): none
- `11:16:59` fuse: {"checked_at": "2026-11-23T11:07:37.884218+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.29, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-apac-leadlag", 0.1], ["justhodl-cq-feed", 0.05], ["justhodl-cycle-features", 0.05], ["justhodl-signal-scorecard", 0.02], ["justhodl-sdmx-walker", 0.02], ["justhodl-earnings-sentiment", 0.01], ["justhodl-episode-reference", 0.01], ["justhodl-ignition", 0.01]]}
- `11:16:59` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 21 functions ran, fleet $0.29/day; month-to-date top: justhodl-symdir $5.00, justhodl-polygon-bank $4.51, justhodl-worldbank-full $3.35"], "mode": "enforce", "mtd": 200.27, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 8.85, "brake_at": 537.27, "budget_forecast": 276.636, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 7.54, "freeze_at": 577.27, "freeze_line_floored": false, "frozen_eom": 213.0, "lag_buffer": 8.85, "mtd": 200.27, "projected_eom": 275.84, "reserve": 22.73, "residual_source": "min7", "residual_usd_day": 0.51, "sources": {"bud
- `11:16:59` cadence sentinel: {"checked": 844, "checked_at": "2026-11-23T00:07:38.100062+00:00", "day": "2026-11-23", "errors": [], "violators": []} | fixed entries: 0
- `11:16:59` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:16:59` meter: {"active": 21, "daily": {"2026-11-09": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [10.0, 0.000113], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [9.0, 2.8e-05], "benzinga-news-agent": [3.0, 3.1e-05], "bls-employment-api-v2": [1.0, 3.6e-05], "bls-labor-agent": [7.0, 0.000401], "bond-indices-agent": [8.0, 0.0008], "census-economic-agent": [7.0, 0.000222], "cftc-futures-positioning-agent": [7.0, 0.002169], "chatgpt-agent-api": [6.0, 0.0], "coinmarketcap-agent": [2.0, 1.4e-05], "daily-liquidity-report": [1.0, 0.000101], "dollar-strength-agent": [8.0, 0.001114], "eia-energy-agent": [1.0, 5.8e-05], "enhanced-repo-agent": [9.0, 8.9e-05], "fedliquidityapi": [1
- `11:16:59` storage: day 2026-11-22 | versioning None | warn_days None | trip_days None
- `11:16:59` storage last assessment: {"day": "2026-11-21", "delta_gb": 5.37, "delta_objects": 99943, "delta_pct": 0.43, "gb": 1267.64, "level": null, "objects": 10185510, "prev": "2026-11-20", "prev_gb": 1262.27, "reason": "growth +5.4 GB (+0.4%), +99943 objects over 2026-11-21"}
- `11:16:59` S3 $/day (storage.s3_usd): {"2026-11-08": 4.0928, "2026-11-09": 7.2844, "2026-11-10": 4.6142, "2026-11-11": 7.4233, "2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709}
- `11:16:59` S3 line, last 8 days: 2026-11-14 $3.93, 2026-11-15 $7.03, 2026-11-16 $3.97, 2026-11-17 $7.22, 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17  (Nov 14-20 alternated 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45; Nov 21 is the first full day with the history book on write-if-changed - a 'high' day by the pattern)
- `11:16:59` service spend 2026-11-18: total $7.66 | Amazon Simple Storage Service $4.07, AWS Lambda $2.75, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:16:59` service spend 2026-11-19: total $11.20 | Amazon Simple Storage Service $7.36, AWS Lambda $3.01, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.08
- `11:16:59` service spend 2026-11-20: total $8.15 | Amazon Simple Storage Service $4.45, AWS Lambda $2.80, AmazonCloudWatch $0.45, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:16:59` service spend 2026-11-21: total $9.55 | Amazon Simple Storage Service $6.17, AWS Lambda $2.48, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.12, Amazon EC2 Container Registry  $0.09
- `11:16:59` storage fuse advanced: day 2026-11-22 (was 2026-11-21 at the Nov 22 run)
- `11:16:59` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:16:59` storage_selftest live days: {"2026-11-17": {"gb": 1246.068, "objects": 9942425}, "2026-11-18": {"gb": 1251.208, "objects": 9984784}, "2026-11-19": {"gb": 1256.563, "objects": 10036646}, "2026-11-20": {"gb": 1262.27, "objects": 10085567}, "2026-11-21": {"gb": 1267.64, "objects": 10185510}}
- `11:16:59` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:16:59` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:16:59` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:16:59` month-to-date top 25: symdir $5.00, polygon-bank $4.51, worldbank-full $3.35, eia-bank $3.08, research-backtest $1.71, bank-histories $1.49, feed-catalog $1.25, causality-scanner $1.20, data-census $0.76, signal-genealogy $0.71, fmp-walker $0.67, predictability $0.61, thesis-engine $0.57, chart-patterns $0.57, stock-screener $0.57, katlin $0.56, tradingview $0.54, wl-engines $0.50, bottom $0.47, fmp-bank $0.47
## C. governor event records since ops 6910 (2026-11-22 11:23 UTC) and the relay marker

- `11:17:00` event records since the Nov 22 run: 0 | kinds {} | newest -
- `11:17:00` relay marker 194.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:17:00` manifest 2026-11-23T01:17:18.503616+00:00: 784 engines, 1353 declared data/ keys
- `11:17:02` outputs by tier: {'DEAD': 156, 'FRESH': 979, 'STALE': 194, 'MISSING': 24} (Sun Nov 22, ops 6910: fresh 979, stale 190, dead 154, missing 24; Sat Nov 21: 1066 / 70 / 187 / 24)
- `11:17:02` SILENT engines - ALL outputs 54 h - 7 d old (Sun: 25, all weekly/fortnightly slots; Sat: 39): 85
- `11:17:02`   buyback-scanner: 1 output(s) 167-167 h
- `11:17:02`   index-inclusion: 1 output(s) 166-166 h
- `11:17:02`   whales: 1 output(s) 166-166 h
- `11:17:02`   equity-ftd: 1 output(s) 164-164 h
- `11:17:02`   methodology-scout: 3 output(s) 153-153 h
- `11:17:02`   nyfed-dealer-survey: 1 output(s) 139-139 h
- `11:17:02`   inventory-drawdown: 1 output(s) 132-132 h
- `11:17:02`   feed-catalog: 1 output(s) 124-124 h
- `11:17:02`   analyst-consensus: 1 output(s) 72-72 h
- `11:17:02`   short-pressure: 1 output(s) 71-71 h
- `11:17:02`   self-improvement: 1 output(s) 71-71 h
- `11:17:02`   esi: 2 output(s) 71-71 h
- `11:17:02`   coffee-can: 2 output(s) 70-70 h
- `11:17:02`   digest-trends-ai: 2 output(s) 70-70 h
- `11:17:02`   price-redundancy: 2 output(s) 70-70 h
- `11:17:02`   fundamentals-engine: 1 output(s) 70-70 h
- `11:17:02`   political-trades: 1 output(s) 70-70 h
- `11:17:02`   term-premium: 1 output(s) 70-70 h
- `11:17:02`   pm-decision: 2 output(s) 69-69 h
- `11:17:02`   global-liquidity: 2 output(s) 69-69 h
- `11:17:02`   strategist: 1 output(s) 69-69 h
- `11:17:02`   sellside-views: 1 output(s) 69-69 h
- `11:17:02`   earnings-nlp: 1 output(s) 69-69 h
- `11:17:02`   premortem-engine: 2 output(s) 68-68 h
- `11:17:02`   convexity-scorer: 1 output(s) 68-68 h
- `11:17:02`   failure-library: 1 output(s) 68-68 h
- `11:17:02`   bank-stress: 2 output(s) 68-68 h
- `11:17:02`   repo-lending: 1 output(s) 67-67 h
- `11:17:02`   chart-vision: 1 output(s) 67-67 h
- `11:17:02`   auction-grader: 1 output(s) 67-67 h
- `11:17:02`   auction-tail: 1 output(s) 67-67 h
- `11:17:02`   naaim: 1 output(s) 66-66 h
- `11:17:02`   polygon-options-flow: 2 output(s) 64-64 h
- `11:17:02`   trade-tickets: 1 output(s) 64-64 h
- `11:17:02`   auction-desk: 7 output(s) 64-64 h
- `11:17:02`   bond-warroom: 3 output(s) 64-64 h
- `11:17:02`   powell-pivot: 1 output(s) 63-63 h
- `11:17:02`   corp-bonds: 1 output(s) 63-63 h
- `11:17:02`   streaming-fanout: 9 output(s) 63-63 h
- `11:17:02`   dealer-gex: 2 output(s) 63-63 h
- `11:17:02`   capex-pulse: 2 output(s) 63-63 h
- `11:17:02`   tiingo-news: 1 output(s) 63-63 h
- `11:17:02`   trend-reversal: 2 output(s) 62-62 h
- `11:17:02`   crypto-etf-arb: 1 output(s) 62-62 h
- `11:17:02`   prediction-snapshotter: 1 output(s) 62-62 h
- `11:17:02`   compound-aggregator: 5 output(s) 62-62 h
- `11:17:02`   13f-price-divergence: 1 output(s) 62-62 h
- `11:17:02`   consensus-bottom: 1 output(s) 62-62 h
- `11:17:02`   live-pulse: 2 output(s) 62-62 h
- `11:17:02`   pnl-tracker: 2 output(s) 62-62 h
- `11:17:02`   shadow-lab: 1 output(s) 62-62 h
- `11:17:02`   ma200-reclaim: 2 output(s) 62-62 h
- `11:17:02`   master-ranker: 1 output(s) 62-62 h
- `11:17:02`   squeeze-fuel: 1 output(s) 62-62 h
- `11:17:02`   cds-proxy: 1 output(s) 62-62 h
- `11:17:02`   commodity-curves: 1 output(s) 61-61 h
- `11:17:02`   factor-returns: 2 output(s) 61-61 h
- `11:17:02`   accumulation-radar: 2 output(s) 61-61 h
- `11:17:02`   options-gamma: 1 output(s) 61-61 h
- `11:17:02`   polygon-today: 2 output(s) 61-61 h
- `11:17:02`   spx-ma: 1 output(s) 61-61 h
- `11:17:02`   fifx-vol-migration: 2 output(s) 61-61 h
- `11:17:02`   dix: 2 output(s) 61-61 h
- `11:17:02`   liquidity-profile: 1 output(s) 61-61 h
- `11:17:02`   sp500: 2 output(s) 61-61 h
- `11:17:02`   global-macro: 1 output(s) 61-61 h
- `11:17:02`   usd-funding: 1 output(s) 61-61 h
- `11:17:02`   implied-prob: 1 output(s) 61-61 h
- `11:17:02`   earnings-confluence: 1 output(s) 61-61 h
- `11:17:02`   event-study: 1 output(s) 61-61 h
- `11:17:02`   historical-analogs: 1 output(s) 61-61 h
- `11:17:02`   cross-asset-rv: 2 output(s) 61-61 h
- `11:17:02`   scarcity-radar: 1 output(s) 61-61 h
- `11:17:02`   vix-curve: 2 output(s) 60-60 h
- `11:17:02`   crisis-knowledge-base: 1 output(s) 60-60 h
- `11:17:02`   institutional-footprint: 2 output(s) 60-60 h
- `11:17:02`   desk-returns: 1 output(s) 60-60 h
- `11:17:02`   bond-trace: 3 output(s) 60-60 h
- `11:17:02`   treasury-rehypo: 3 output(s) 60-60 h
- `11:17:02`   resilience: 1 output(s) 60-60 h
- `11:17:02` engines with at least one output < 30 h: 624 of 784 (Sun: 626 of 784; Sat: 670 of 784)
- `11:17:02` STALE 30h-7d (194), youngest 40: symbol-dictionary.json 30h, nyfed-primary-dealer.json 31h, _state/ishares-product-map.json 31h, forward-returns.json 32h, thesis-engine.json 36h, thesis-state-v2.json.gz 36h, wl-predictions.json 37h, wl-engines.json 37h, ici-flows.json 38h, cftc-deep-view.json 38h, cot-extremes.json 38h, sector-flow-state.json 40h, bond-desk.json 40h, history/bond-desk.json 40h, 13f-etf-classes-learned.json 44h, beaters-learned-weights.json 44h, spx-beaters.json 45h, base-rates.json 45h, insider-aggregate.json 46h, leading-markets-history.json 46h, leading-markets.json 46h, brain-compiler.json 46h, cross-asset-regime-state.json 46h, cross-asset-regime.json 46h, asset-discovery.json 46h, treasury-noise.json 46h, options-analytics-iv-history.json 46h, options-analytics.json 46h, market-internals.json 47h, ark-holdings-base.json 53h, warm/katlin/splits.json 55h, cds-desk.json 55h, warm/cds/bank.json.gz 55h, cds-desk-history.json 55h, warm/ecb-sup/_last-check.json 55h, warm/ecb-sup/state.json 55h, bottom.json 55h, capitulation-desk.json 55h, fortress.json 56h, _fleet-monitor-alert-history.json 57h
- `11:17:02` DEAD >=7d (156); crossed the line since the Nov 22 run (5): fi-census.json 172h<-justhodl-fi-census, fi-census-matrix.json 172h<-justhodl-fi-census, etf-census-matrix.json 173h<-justhodl-etf-census, etf-census.json 173h<-justhodl-etf-census, 10kq-filings.json 174h<-justhodl-sec-10kq
- `11:17:02` ⚠ 5 declared output(s) crossed the 7-day line since yesterday: fi-census.json<-justhodl-fi-census, fi-census-matrix.json<-justhodl-fi-census, etf-census-matrix.json<-justhodl-etf-census, etf-census.json<-justhodl-etf-census, 10kq-filings.json<-justhodl-sec-10kq
- `11:17:02` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:17:02` watched outputs (desks, banks, monitors):
- `11:17:02`   data/strength.json: 7.6 h old (4370648 B) -> FRESH
- `11:17:02`   data/strength-wide.json: 7.6 h old (2121258 B) -> FRESH
- `11:17:02`   data/leaders.json: 7.4 h old (3083502 B) -> FRESH
- `11:17:02`   data/bottom.json: 55.5 h old (2269881 B) -> STALE
- `11:17:02`   data/stock-volatility.json: 7.1 h old (2768741 B) -> FRESH
- `11:17:02`   data/campaign-scan.json: 7.2 h old (2021693 B) -> FRESH
- `11:17:02`   data/etf-true-flows.json: 19.4 h old (1109582 B) -> FRESH
- `11:17:02`   data/ici-flows.json: 38.1 h old (32177 B) -> STALE
- `11:17:02`   data/13f-book.json: 18.7 h old (2439119 B) -> FRESH
- `11:17:02`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1443 B) -> FRESH
- `11:17:02`   data/warm/factor-book/manifest.json: 8.3 h old (20485 B) -> FRESH
- `11:17:02`   data/warm/history-book/manifest.json: 8.1 h old (11958 B) -> FRESH
- `11:17:02`   data/corporate-actions/_state.json: 4.5 h old (66 B) -> FRESH
- `11:17:02`   data/_freshness-monitor.json: 3.3 h old (1182140 B) -> FRESH
- `11:17:02`   _health/fleet.json: 8.8 h old (10985 B) -> FRESH
- `11:17:02`   _health/cadence-manifest.json: 19.8 h old (111967 B) -> FRESH
- `11:17:02`   data/ciss-stress.json: 4.1 h old (4619320 B) -> FRESH
- `11:17:02`   data/ciss-sovereign.json: 4.0 h old (820803 B) -> FRESH
- `11:17:02`   data/ciss-sovereign-world.json: 4.0 h old (726086 B) -> FRESH
- `11:17:02`   data/ciss-dollar-shortage.json: 4.0 h old (1020309 B) -> FRESH
- `11:17:02`   data/ciss-sovereign-history.json: 4.0 h old (2333035 B) -> FRESH
## E. FMP bandwidth hold (walker + bank manifests) - the 30-day window rolls ~Nov 24-27

- `11:17:02` data/warm/fmp/manifest.json (11.7 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:17:02` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:17:02` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent False telegram_sent False
- `11:17:02`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3250, "inflight": 0, "total": 3250}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:17:02` data/ciss-research/sovereign: 5 object(s) | newest world-state.json 4.0 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 19.7 h
- `11:17:02` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market-history.json 13.4 h
- `11:17:02` data/dealer: 60 object(s)+ | newest a8141b08b806e151bde6e2431ffc621f8ad842ff1386107fd2 30.8 h | oldest migration.json 1559.0 h
- `11:17:02` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2370.9 h
- `11:17:02` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2639.8 h
- `11:17:02` ✅ read-only; nothing changed, nothing released, nothing re-invoked

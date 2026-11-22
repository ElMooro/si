# ops 6910 - daily health status 2026-11-22 (governor status + caps + events since ops 6878, output freshness, FMP hold, fleet-error-monitor, the overnight Repo desk outputs; Sunday; read-only)

**Status:** success  
**Duration:** 7.5s  
**Finished:** 2026-11-22T11:23:15+00:00  

## Data

| 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | corporate-actions__state_age_h | engines_fresh | engines_missed_two_slots | etf-true-flows_age_h | events_since_6878 | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | ici-flows_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | s3_usd_recent | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 18.8 | 3.4 | 0 | 8.85 | 31.6 | 195.124 | 283.151 | 41.3 | 7.3 | 600.0 | 3 | 4.6 | 626 | 25 | 19.5 | 0 | 8.9 | clear | clear | 3 | 14.2 | 7.5 | symdir $4.71; polygon-bank $4.31; worldbank-full $3.35; eia-bank $2.94; research-backtest $1.62; bank-histories $1.47 | enforce | 8.9 | None | 195.12 | 4 | 154 | 979 | 24 | 190 | 1347 | 279.41 | 171.0 | 6.9 | 8.9 | 13.5 | 2026-11-17 7.22; 2026-11-18 4.07; 2026-11-19 7.36; 2026-11-20 4.45 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.2 | 2026-11-21 | 1262.27 | None | none | 7.7 | 7.7 | 1.6.1 | None | 8.4 | 8.1 | 8.2 |

## Log
## A. governor status

- `11:23:09` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:23:09` cap $600.0 (budget) | budget limit $600.0 actual $195.124 forecast $283.151
- `11:23:09` thresholds: {"baseline_usd_day": 8.85, "brake_at": 537.23, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 8.53, "freeze_at": 577.23, "freeze_line_floored": false, "frozen_eom": 207.89, "lag_buffer": 8.85, "mtd": 195.12, "projected_eom": 279.41, "reserve": 22.77, "residual_usd_day": 0.46, "sources": {"budget_actual": 195.124, "ce_mtd": 194.0222}}
- `11:23:09` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:23:09` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:23:09` anomaly held (ANOM): none
- `11:23:09` fuse: {"checked_at": "2026-11-22T10:07:37.777943+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 2.62, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-katlin", 1.67], ["justhodl-fortress", 0.73], ["justhodl-stock-screener", 0.1], ["justhodl-signal-harvester", 0.06], ["justhodl-fundamental-graphs", 0.02], ["justhodl-backend-agent", 0.01], ["justhodl-cost-anomaly", 0.01], ["justhodl-analytics-snapshot", 0.01]]}
- `11:23:09` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 20 functions ran, fleet $1.75/day; month-to-date top: justhodl-symdir $4.71, justhodl-polygon-bank $4.31, justhodl-worldbank-full $3.35"], "mode": "enforce", "mtd": 195.12, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 8.85, "brake_at": 537.23, "budget_forecast": 283.151, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 8.54, "freeze_at": 577.23, "freeze_line_floored": false, "frozen_eom": 207.9, "lag_buffer": 8.85, "mtd": 195.12, "projected_eom": 279.51, "reserve": 22.77, "residual_source": "min7", "residual_usd_day": 0.46, "sources": {"bud
- `11:23:09` cadence sentinel: {"checked": 844, "checked_at": "2026-11-22T00:07:37.945570+00:00", "day": "2026-11-22", "errors": [], "violators": []} | fixed entries: 0
- `11:23:09` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:23:09` meter: {"active": 20, "daily": {"2026-11-08": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [16.0, 0.000169], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [11.0, 3.7e-05], "benzinga-news-agent": [2.0, 2e-05], "bls-employment-api-v2": [1.0, 3.7e-05], "bls-labor-agent": [9.0, 0.000441], "bond-indices-agent": [10.0, 0.0006], "census-economic-agent": [9.0, 0.000219], "cftc-futures-positioning-agent": [7.0, 0.001294], "chatgpt-agent-api": [8.0, 0.0], "coinmarketcap-agent": [2.0, 1.3e-05], "daily-liquidity-report": [1.0, 6.1e-05], "dollar-strength-agent": [10.0, 0.000746], "eia-energy-agent": [1.0, 6.1e-05], "enhanced-repo-agent": [11.0, 7.2e-05], "fedliquidityapi": [
- `11:23:09` storage: day 2026-11-21 | versioning None | warn_days None | trip_days None
- `11:23:09` storage last assessment: {"day": "2026-11-20", "delta_gb": 5.71, "delta_objects": 48921, "delta_pct": 0.45, "gb": 1262.27, "level": null, "objects": 10085567, "prev": "2026-11-19", "prev_gb": 1256.563, "reason": "growth +5.7 GB (+0.5%), +48921 objects over 2026-11-20"}
- `11:23:09` S3 $/day (storage.s3_usd): {"2026-11-07": 6.9638, "2026-11-08": 4.0928, "2026-11-09": 7.2844, "2026-11-10": 4.6142, "2026-11-11": 7.4233, "2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508}
- `11:23:09` S3 line, last 8 days: 2026-11-13 $7.35, 2026-11-14 $3.93, 2026-11-15 $7.03, 2026-11-16 $3.97, 2026-11-17 $7.22, 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45  (Nov 14-19 alternated 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36; Nov 21 is the first full day with the history book on write-if-changed)
- `11:23:09` service spend 2026-11-17: total $10.71 | Amazon Simple Storage Service $7.22, AWS Lambda $2.65, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.08
- `11:23:09` service spend 2026-11-18: total $7.66 | Amazon Simple Storage Service $4.07, AWS Lambda $2.75, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:23:09` service spend 2026-11-19: total $11.20 | Amazon Simple Storage Service $7.36, AWS Lambda $3.01, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.08
- `11:23:09` service spend 2026-11-20: total $8.15 | Amazon Simple Storage Service $4.45, AWS Lambda $2.80, AmazonCloudWatch $0.45, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:23:09` storage fuse advanced: day 2026-11-21 (was 2026-11-20 at the Nov 21 run)
- `11:23:10` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:23:10` storage_selftest live days: {"2026-11-16": {"gb": 1241.386, "objects": 9897663}, "2026-11-17": {"gb": 1246.068, "objects": 9942425}, "2026-11-18": {"gb": 1251.208, "objects": 9984784}, "2026-11-19": {"gb": 1256.563, "objects": 10036646}, "2026-11-20": {"gb": 1262.27, "objects": 10085567}}
- `11:23:10` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:23:10` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:23:10` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:23:10` month-to-date top 25: symdir $4.71, polygon-bank $4.31, worldbank-full $3.35, eia-bank $2.94, research-backtest $1.62, bank-histories $1.47, causality-scanner $1.14, feed-catalog $1.08, data-census $0.72, signal-genealogy $0.67, fmp-walker $0.62, predictability $0.58, thesis-engine $0.57, chart-patterns $0.57, stock-screener $0.56, katlin $0.56, tradingview $0.51, wl-engines $0.50, bottom $0.47, polygon-options-flow $0.45
## C. governor event records since ops 6878 (2026-11-21 11:22 UTC) and the relay marker

- `11:23:10` event records since the Nov 21 run: 0 | kinds {} | newest -
- `11:23:10` relay marker 171.0 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:23:10` manifest 2026-11-22T06:56:20.931857+00:00: 784 engines, 1347 declared data/ keys
- `11:23:13` outputs by tier: {'STALE': 190, 'FRESH': 979, 'DEAD': 154, 'MISSING': 24} (Sat Nov 21, ops 6878: fresh 1066, stale 70, dead 187, missing 24; Fri Nov 20: 1064 / 71 / 187 / 24)
- `11:23:13` SILENT engines - ALL outputs 54 h - 7 d old (Sat: 39, 37 of them weekly/fortnightly slots): 25
- `11:23:13`   strategy-portfolio: 1 output(s) 167-167 h
- `11:23:13`   provider-window-sentinel: 1 output(s) 165-165 h
- `11:23:13`   ma-target-predictor: 1 output(s) 165-165 h
- `11:23:13`   weekly-ai-review: 1 output(s) 165-165 h
- `11:23:13`   meta-improver: 1 output(s) 157-157 h
- `11:23:13`   sec-10kq: 1 output(s) 150-150 h
- `11:23:13`   signal-halflife: 2 output(s) 149-149 h
- `11:23:13`   fabrication-weekly: 1 output(s) 149-149 h
- `11:23:13`   etf-census: 2 output(s) 149-149 h
- `11:23:13`   playbook-engine: 2 output(s) 148-148 h
- `11:23:13`   fi-census: 2 output(s) 148-148 h
- `11:23:13`   13f-clone-alpha: 1 output(s) 147-147 h
- `11:23:13`   sec-bulk: 1 output(s) 146-146 h
- `11:23:13`   bis-crossborder: 1 output(s) 145-145 h
- `11:23:13`   seasonality: 1 output(s) 144-144 h
- `11:23:13`   smart-money-13f: 1 output(s) 144-144 h
- `11:23:13`   buyback-scanner: 1 output(s) 143-143 h
- `11:23:13`   index-inclusion: 1 output(s) 142-142 h
- `11:23:13`   whales: 1 output(s) 142-142 h
- `11:23:13`   equity-ftd: 1 output(s) 140-140 h
- `11:23:13`   methodology-scout: 3 output(s) 129-129 h
- `11:23:13`   nyfed-dealer-survey: 1 output(s) 115-115 h
- `11:23:13`   inventory-drawdown: 1 output(s) 108-108 h
- `11:23:13`   feed-catalog: 1 output(s) 100-100 h
- `11:23:13`   stock-xray: 1 output(s) 59-59 h
- `11:23:13` engines with at least one output < 30 h: 626 of 784 (Sat: 670 of 784; Fri: 668 of 784)
- `11:23:13` STALE 30h-7d (190), youngest 40: warm/katlin/splits.json 31h, cds-desk.json 31h, warm/cds/bank.json.gz 31h, cds-desk-history.json 31h, warm/ecb-sup/_last-check.json 31h, warm/ecb-sup/state.json 31h, bottom.json 32h, capitulation-desk.json 32h, fortress.json 32h, _fleet-monitor-alert-history.json 33h, finra-short.json 34h, forced-selling-bounce.json 35h, global-flow-desk.json 36h, sector-capital-fusion.json 36h, resilience.json 36h, warm/treasury-par/curve.json.gz 36h, treasury-rehypo-history.json 36h, treasury-rehypo-long.json 36h, treasury-rehypo.json 36h, bond-trace.json 36h, finra-aggregate-history-v1.json 36h, trace-bond-prints.json 36h, desk-returns.json 36h, history/institutional-footprint.json 36h, institutional-footprint.json 36h, crisis-knowledge-base.json 36h, vix-curve-history.json 36h, vix-curve.json 36h, crypto-emergence.json 36h, scarcity-radar.json 37h, cross-asset-rv-history.json 37h, cross-asset-rv.json 37h, historical-analogs.json 37h, event-study.json 37h, earnings-confluence.json 37h, implied-prob.json 37h, usd-funding.json 37h, global-macro.json 37h, readthrough.json 37h, sp500.json 37h
- `11:23:13` DEAD >=7d (154); crossed the line since the Nov 21 run (4): fundamental-census-history.json 172h<-justhodl-fundamental-census, fundamental-census.json 172h<-justhodl-fundamental-census, fundamental-census-matrix.json 172h<-justhodl-fundamental-census, _state/fundamental-census-cursor.json 172h<-justhodl-fundamental-census
- `11:23:13` ⚠ 4 declared output(s) crossed the 7-day line since yesterday: fundamental-census-history.json<-justhodl-fundamental-census, fundamental-census.json<-justhodl-fundamental-census, fundamental-census-matrix.json<-justhodl-fundamental-census, _state/fundamental-census-cursor.json<-justhodl-fundamental-census
- `11:23:13` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:23:13` watched outputs (desks, banks, monitors):
- `11:23:13`   data/strength.json: 7.7 h old (4370244 B) -> FRESH
- `11:23:13`   data/strength-wide.json: 7.7 h old (2121256 B) -> FRESH
- `11:23:13`   data/leaders.json: 7.5 h old (3070639 B) -> FRESH
- `11:23:13`   data/bottom.json: 31.6 h old (2269881 B) -> STALE
- `11:23:14`   data/stock-volatility.json: 7.2 h old (2768739 B) -> FRESH
- `11:23:14`   data/campaign-scan.json: 7.3 h old (2021692 B) -> FRESH
- `11:23:14`   data/etf-true-flows.json: 19.5 h old (1109577 B) -> FRESH
- `11:23:14`   data/ici-flows.json: 14.2 h old (32177 B) -> FRESH
- `11:23:14`   data/13f-book.json: 18.8 h old (2439118 B) -> FRESH
- `11:23:14`   data/warm/polygon-full/eod/manifest.json: 8.2 h old (1443 B) -> FRESH
- `11:23:14`   data/warm/factor-book/manifest.json: 8.4 h old (20485 B) -> FRESH
- `11:23:14`   data/warm/history-book/manifest.json: 8.1 h old (11594 B) -> FRESH
- `11:23:14`   data/corporate-actions/_state.json: 4.6 h old (66 B) -> FRESH
- `11:23:14`   data/_freshness-monitor.json: 3.4 h old (1122494 B) -> FRESH
- `11:23:14`   _health/fleet.json: 8.9 h old (11018 B) -> FRESH
- `11:23:14`   _health/cadence-manifest.json: 41.3 h old (111689 B) -> STALE
## E. FMP bandwidth hold (walker + bank manifests) - first day with 0 conductor kicks for the two walkers

- `11:23:14` data/warm/fmp/manifest.json (11.8 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:23:14` data/warm/fmp-bank/manifest.json (12.0 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:23:14` data/_fleet-monitor.json (8.9 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent False telegram_sent False
- `11:23:14`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3255, "inflight": 0, "total": 3255}}
## G. the Repo desk + OFR bank outputs from the overnight session (ages only, first 60 keys per prefix)

- `11:23:15` data/repo-board: no objects
- `11:23:15` data/repo-market: 2 object(s) | newest repo-market-history.json 13.5 h | oldest repo-market-history.json 13.5 h
- `11:23:15` data/dealer: 60 object(s)+ | newest a8141b08b806e151bde6e2431ffc621f8ad842ff1386107fd2 6.9 h | oldest migration.json 1535.1 h
- `11:23:15` data/fr2004: no objects
- `11:23:15` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.9 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2347.0 h
- `11:23:15` data/ofr: 3 object(s) | newest ofr-fsi.json 8.9 h | oldest ofr-stfm.json 2615.9 h
- `11:23:15` ✅ read-only; nothing changed, nothing released, nothing re-invoked

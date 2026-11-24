# ops 6934 - daily health status 2026-11-24 (governor status + caps + events since ops 6927, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Tuesday; read-only)

**Status:** success  
**Duration:** 11.3s  
**Finished:** 2026-11-24T11:19:28+00:00  

## Data

| 10kq-filings_age_h | 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | capstruct_failed_at | capstruct_status | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corporate-actions__state_age_h | delta_objects | engines_fresh | engines_missed_two_slots | etf-true-flows_age_h | events_since_6927 | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | ici-flows_age_h | insider-trades_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 198.2 | 18.7 | 3.3 | 0 | 7.91 | 7.5 | 205.838 | 249.596 | 43.9 | 7.2 | 600.0 | 3 | 2026-10-08T22:00:40.571580+00:00 | failed | 4.1 | 4.1 | 4.1 | 4.1 | 4.1 | 4.6 | 81712 | 695 | 17 | 19.4 | 0 | 8.8 | clear | clear | 3 | 62.2 | 14.1 | 7.5 | symdir $5.41; polygon-bank $4.74; worldbank-full $3.35; eia-bank $3.25; research-backtest $1.80; bank-histories $1.52 | enforce | 8.8 | None | 205.84 | 5 | 157 | 1107 | 24 | 65 | 1353 | 265.38 | 218.9 | 14.7 | 8.8 | 13.4 | 4.1 | 2026-11-19 7.36; 2026-11-20 4.45; 2026-11-21 6.17; 2026-11-22 3.49 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.2 | 2026-11-23 | 1275.104 | None | none | 7.6 | 7.6 | 1.6.1 | None | 8.4 | 8.1 | 8.1 |

## Log
## A. governor status

- `11:19:19` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:19:19` cap $600.0 (budget) | budget limit $600.0 actual $205.838 forecast $249.596
- `11:19:19` thresholds: {"baseline_usd_day": 7.91, "brake_at": 538.88, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 6.53, "freeze_at": 578.88, "freeze_line_floored": false, "frozen_eom": 216.96, "lag_buffer": 7.91, "mtd": 205.84, "projected_eom": 265.38, "reserve": 21.12, "residual_usd_day": 0.49, "sources": {"budget_actual": 205.838, "ce_mtd": 205.8383}}
- `11:19:19` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:19:19` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:19:19` anomaly held (ANOM): none
- `11:19:19` fuse: {"checked_at": "2026-11-24T11:07:37.914334+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.3, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-apac-leadlag", 0.11], ["justhodl-cq-feed", 0.05], ["justhodl-cycle-features", 0.05], ["justhodl-signal-scorecard", 0.02], ["justhodl-sdmx-walker", 0.02], ["justhodl-earnings-sentiment", 0.02], ["justhodl-episode-reference", 0.01], ["justhodl-ignition", 0.01]]}
- `11:19:19` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 19 functions ran, fleet $0.30/day; month-to-date top: justhodl-symdir $5.41, justhodl-polygon-bank $4.74, justhodl-worldbank-full $3.35"], "mode": "enforce", "mtd": 205.84, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 7.91, "brake_at": 538.88, "budget_forecast": 249.596, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 6.54, "freeze_at": 578.88, "freeze_line_floored": false, "frozen_eom": 216.96, "lag_buffer": 7.91, "mtd": 205.84, "projected_eom": 265.45, "reserve": 21.12, "residual_source": "min7", "residual_usd_day": 0.49, "sources": {"bu
- `11:19:19` cadence sentinel: {"checked": 844, "checked_at": "2026-11-24T00:07:37.972855+00:00", "day": "2026-11-24", "errors": [], "violators": []} | fixed entries: 0
- `11:19:19` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:19:19` meter: {"active": 19, "daily": {"2026-11-10": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [16.0, 0.000142], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [11.0, 4.1e-05], "benzinga-news-agent": [3.0, 3e-05], "bls-employment-api-v2": [1.0, 3.7e-05], "bls-labor-agent": [9.0, 0.000506], "bond-indices-agent": [10.0, 0.000635], "census-economic-agent": [9.0, 0.000327], "cftc-futures-positioning-agent": [16.0, 0.001918], "chatgpt-agent-api": [8.0, 0.0], "coinmarketcap-agent": [2.0, 1.3e-05], "daily-liquidity-report": [1.0, 0.000104], "dollar-strength-agent": [10.0, 0.000881], "eia-energy-agent": [1.0, 6e-05], "enhanced-repo-agent": [11.0, 9.2e-05], "fedliquidityapi":
- `11:19:19` storage: day 2026-11-23 | versioning None | warn_days None | trip_days None
- `11:19:19` storage last assessment: {"day": "2026-11-22", "delta_gb": 7.46, "delta_objects": 81712, "delta_pct": 0.59, "gb": 1275.104, "level": null, "objects": 10267222, "prev": "2026-11-21", "prev_gb": 1267.64, "reason": "growth +7.5 GB (+0.6%), +81712 objects over 2026-11-22"}
- `11:19:19` S3 $/day (storage.s3_usd): {"2026-11-09": 7.2844, "2026-11-10": 4.6142, "2026-11-11": 7.4233, "2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709, "2026-11-22": 3.4863}
- `11:19:19` S3 line, last 8 days: 2026-11-15 $7.03, 2026-11-16 $3.97, 2026-11-17 $7.22, 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17, 2026-11-22 $3.49  (Nov 14-21: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17; Nov 22 is a 'low' day by the alternating pattern, the second day with the history book on write-if-changed)
- `11:19:19` service spend 2026-11-19: total $11.20 | Amazon Simple Storage Service $7.36, AWS Lambda $3.01, AmazonCloudWatch $0.40, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.08
- `11:19:19` service spend 2026-11-20: total $8.15 | Amazon Simple Storage Service $4.45, AWS Lambda $2.80, AmazonCloudWatch $0.45, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:19:19` service spend 2026-11-21: total $9.55 | Amazon Simple Storage Service $6.17, AWS Lambda $2.48, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.12, Amazon EC2 Container Registry  $0.09
- `11:19:19` service spend 2026-11-22: total $6.25 | Amazon Simple Storage Service $3.49, AWS Lambda $1.87, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.11, Amazon EC2 Container Registry  $0.09
- `11:19:19` storage fuse advanced: day 2026-11-23 (was 2026-11-22 at the Nov 23 run)
- `11:19:19` object delta for 2026-11-22: +81712 (Nov 17-20 ran +42-49k/day; Nov 21 +99,943 with the Saturday build)
- `11:19:19` ⚠ object delta +81712 is a second day near double the usual - a per-object writer is worth finding, not only the Nov 21 build
- `11:19:19` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:19:19` storage_selftest live days: {"2026-11-18": {"gb": 1251.208, "objects": 9984784}, "2026-11-19": {"gb": 1256.563, "objects": 10036646}, "2026-11-20": {"gb": 1262.27, "objects": 10085567}, "2026-11-21": {"gb": 1267.64, "objects": 10185510}, "2026-11-22": {"gb": 1275.104, "objects": 10267222}}
- `11:19:19` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:19:20` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:19:20` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:19:20` month-to-date top 25: symdir $5.41, polygon-bank $4.74, worldbank-full $3.35, eia-bank $3.25, research-backtest $1.80, bank-histories $1.52, feed-catalog $1.41, causality-scanner $1.26, data-census $0.80, signal-genealogy $0.76, fmp-walker $0.71, predictability $0.61, katlin $0.58, thesis-engine $0.57, chart-patterns $0.57, stock-screener $0.57, tradingview $0.57, wl-engines $0.50, bottom $0.50, fmp-bank $0.49
## C. governor event records since ops 6927 (2026-11-23 11:17 UTC) and the relay marker

- `11:19:20` event records since the Nov 23 run: 0 | kinds {} | newest -
- `11:19:20` relay marker 218.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:19:20` manifest 2026-11-23T22:54:27.839018+00:00: 784 engines, 1353 declared data/ keys
- `11:19:25` outputs by tier: {'DEAD': 157, 'FRESH': 1107, 'STALE': 65, 'MISSING': 24} (Mon Nov 23, ops 6927: fresh 979, stale 194, dead 156, missing 24; Sun Nov 22: 979 / 190 / 154 / 24; Tue Nov 17 read 1072 / 61 / 155 / 25)
- `11:19:25` SILENT engines - ALL outputs 54 h - 7 d old (Mon: 85 = Friday market-day writes + weekly slots; Sun: 25; a Tuesday should be back near 20-30): 17
- `11:19:25`   nyfed-dealer-survey: 1 output(s) 163-163 h
- `11:19:25`   inventory-drawdown: 1 output(s) 156-156 h
- `11:19:25`   feed-catalog: 1 output(s) 148-148 h
- `11:19:25`   naaim: 1 output(s) 90-90 h
- `11:19:25`   liquidity-profile: 1 output(s) 85-85 h
- `11:19:25`   treasury-noise: 2 output(s) 70-94 h
- `11:19:25`   asset-discovery: 1 output(s) 70-70 h
- `11:19:25`   brain-compiler: 1 output(s) 70-70 h
- `11:19:25`   base-rates: 1 output(s) 69-69 h
- `11:19:25`   spx-beaters: 1 output(s) 69-69 h
- `11:19:25`   beaters-grader: 1 output(s) 68-68 h
- `11:19:25`   cftc-deep-view: 2 output(s) 62-62 h
- `11:19:25`   ici-flows: 1 output(s) 62-62 h
- `11:19:25`   wl-engines: 3 output(s) 60-61 h
- `11:19:25`   thesis-engine: 2 output(s) 60-60 h
- `11:19:25`   forward-returns: 1 output(s) 56-56 h
- `11:19:25`   symbol-dictionary: 1 output(s) 54-54 h
- `11:19:25` engines with at least one output < 30 h: 695 of 784 (Mon: 624 of 784; Sun: 626; Sat: 670)
- `11:19:25` STALE 30h-7d (65), youngest 40: warm/statcan/cube-catalog.json.gz 31h, gdelt-news.json 32h, cycle-clock.json 36h, meta-improver-state.json 37h, symdir/fred-titles.json 38h, ecb-derived.json 44h, ecb-hist/esi.json 44h, weekly-review-latest.json 45h, ma-target-predictor.json 45h, provider-window-sentinel.json 45h, port-cargo.json 47h, hiring-velocity.json 47h, strategy-portfolio.json 47h, bagger-engine.json 47h, portwatch.json 48h, warm/portwatch/history/daily-rows.json.gz 48h, edge-discovery-graduated.json 48h, edge-discovery.json 48h, signal-orthogonality.json 50h, katlin-backtest.json 50h, gsi-horizons.json 50h, jsi-calibration.json 50h, fortress-backtest.json 50h, gsi-calibration.json 50h, calibration-history.json 50h, calibration-latest.json 50h, spx-history-deep.json 51h, warm/fred-scoped/Dollar_TradeWeighted/TWEXEMEGSMTH.json 53h, xbrl-fundamentals-index.json 53h, xbrl-fundamentals/_state.json 53h, warm/fred-scoped/Dollar_TradeWeighted/TWEXAFEGSMTH.json 53h, warm/fred-scoped/Dollar_TradeWeighted/TWEXBGSMTH.json 53h, warm/fred-scoped/Dollar_TradeWeighted/DTWEXB.json 53h, warm/fred-scoped/Dollar_TradeWeighted/DTWEXM.json 53h, warm/fred-scoped/Dollar_TradeWeighted/RTWEXBGS.json 53h, factor-decomposition.json 53h, symbol-dictionary.json 54h, nyfed-primary-dealer.json 55h, _state/ishares-product-map.json 55h, forward-returns.json 56h
- `11:19:25` DEAD >=7d (157); crossed the line since the Nov 23 run (5): 13f-security-master.json 176h<-justhodl-13f-book, history/bond-crisis-analogs.json 187h<-justhodl-bond-desk, equity-ftd.json 188h<-justhodl-equity-ftd, congress-party-map.json 189h<-justhodl-political-stocks, sec-cik-ticker-map.json 191h<-justhodl-sec-8k-enrich
- `11:19:25` ⚠ 5 declared output(s) crossed the 7-day line since yesterday: 13f-security-master.json<-justhodl-13f-book, history/bond-crisis-analogs.json<-justhodl-bond-desk, equity-ftd.json<-justhodl-equity-ftd, congress-party-map.json<-justhodl-political-stocks, sec-cik-ticker-map.json<-justhodl-sec-8k-enrich
- `11:19:25` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:19:25` watched outputs (desks, banks, monitors):
- `11:19:25`   data/strength.json: 7.6 h old (4396326 B) -> FRESH
- `11:19:25`   data/strength-wide.json: 7.6 h old (2130008 B) -> FRESH
- `11:19:25`   data/leaders.json: 7.5 h old (3104972 B) -> FRESH
- `11:19:25`   data/bottom.json: 7.5 h old (2284598 B) -> FRESH
- `11:19:25`   data/stock-volatility.json: 7.2 h old (2781525 B) -> FRESH
- `11:19:25`   data/campaign-scan.json: 7.2 h old (2175020 B) -> FRESH
- `11:19:25`   data/etf-true-flows.json: 19.4 h old (1116796 B) -> FRESH
- `11:19:25`   data/ici-flows.json: 62.2 h old (32177 B) -> STALE
- `11:19:25`   data/13f-book.json: 18.7 h old (2439109 B) -> FRESH
- `11:19:26`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1567 B) -> FRESH
- `11:19:26`   data/warm/factor-book/manifest.json: 8.4 h old (20391 B) -> FRESH
- `11:19:26`   data/warm/history-book/manifest.json: 8.1 h old (11619 B) -> FRESH
- `11:19:26`   data/corporate-actions/_state.json: 4.6 h old (66 B) -> FRESH
- `11:19:26`   data/_freshness-monitor.json: 3.3 h old (1099145 B) -> FRESH
- `11:19:26`   _health/fleet.json: 8.8 h old (11119 B) -> FRESH
- `11:19:26`   _health/cadence-manifest.json: 43.9 h old (111967 B) -> STALE
- `11:19:26`   data/ciss-stress.json: 4.1 h old (4620592 B) -> FRESH
- `11:19:26`   data/ciss-sovereign.json: 4.1 h old (820782 B) -> FRESH
- `11:19:26`   data/ciss-sovereign-world.json: 4.1 h old (728387 B) -> FRESH
- `11:19:26`   data/ciss-dollar-shortage.json: 4.1 h old (1020561 B) -> FRESH
- `11:19:26`   data/ciss-sovereign-history.json: 4.1 h old (2333076 B) -> FRESH
- `11:19:27`   data/10kq-filings.json: 198.2 h old (6573546 B) -> DEAD
- `11:19:27`   data/insider-trades.json: 14.1 h old (1755676 B) -> FRESH
## E. FMP bandwidth hold (walker + bank manifests) - the 30-day window rolls ~Nov 24-27; today is Nov 24

- `11:19:27` data/warm/fmp/manifest.json (11.7 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:19:27` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:19:27` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent True telegram_sent False
- `11:19:27`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3237, "inflight": 0, "total": 3237}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:19:27` data/ciss-research/sovereign: 6 object(s) | newest world-state.json 4.1 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 43.7 h
- `11:19:27` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market-history.json 13.4 h
- `11:19:27` data/dealer: 60 object(s)+ | newest dealer-gex.json 14.7 h | oldest migration.json 1583.0 h
- `11:19:27` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2395.0 h
- `11:19:28` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2663.8 h
## H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')

- `11:19:28` control 46.6 d old: contract capital-structure-recurring-refresh.v1 | status failed | started 2026-10-08T20:28:05.951329+00:00 | failed_at 2026-10-08T22:00:40.571580+00:00 | error_type ValueError | active_phase part-2 | completed ['plan', 'part-1'] | review {}
- `11:19:28` ⚠ capital-structure control still 'failed' (since 2026-10-08T22:00:40.571580+00:00) - every scheduled plan stops at 04:46 UTC until release_failed_control runs with an ops number and a review note (Khalid's call; this run does not release it)
- `11:19:28` ✅ read-only; nothing changed, nothing released, nothing re-invoked

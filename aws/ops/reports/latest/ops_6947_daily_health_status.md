# ops 6947 - daily health status 2026-11-25 (governor status + caps + events since ops 6934, output freshness, FMP hold, fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Wednesday; read-only)

**Status:** success  
**Duration:** 11.0s  
**Finished:** 2026-11-25T11:16:52+00:00  

## Data

| 10kq-filings_age_h | 13f-book_age_h | _freshness-monitor_age_h | anomaly_held | baseline_usd_day | bottom_age_h | budget_actual | budget_forecast | cadence-manifest_age_h | campaign-scan_age_h | cap | capped | capstruct_failed_at | capstruct_status | ciss-dollar-shortage_age_h | ciss-sovereign-history_age_h | ciss-sovereign-world_age_h | ciss-sovereign_age_h | ciss-stress_age_h | corporate-actions__state_age_h | credit-before-equity_age_h | credit_credit-before-equity-history_age_h | delta_objects | engines_fresh | engines_missed_two_slots | etf-true-flows_age_h | events_since_6934 | fleet_age_h | fmp_hold_fmp | fmp_hold_fmp-bank | held_total | ici-flows_age_h | insider-trades_age_h | leaders_age_h | meter_top | mode | monitor_age_h | monitor_dlq_visible | mtd | newly_dead | out_dead | out_fresh | out_missing | out_stale | outputs | projected_eom | regime-map-prev_age_h | regime-map_age_h | relay_marker_age_h | repo_dealer_newest_h | repo_ofr_newest_h | repo_repo-market_newest_h | repo_sovereign_newest_h | s3_usd_recent | stage | stage_since | stock-volatility_age_h | storage_day | storage_last_gb | storage_last_level | storage_live_partial_days | strength-wide_age_h | strength_age_h | version | versioning | warm_factor-book_manifest_age_h | warm_history-book_manifest_age_h | warm_polygon-full_eod_manifest_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 222.1 | 18.6 | 3.3 | 0 | 7.91 | 7.5 | 213.456 | 254.027 | 11.6 | 7.2 | 600.0 | 3 | 2026-10-08T22:00:40.571580+00:00 | failed | 4.0 | 4.0 | 4.0 | 4.0 | 4.1 | 4.5 | 11.4 | 11.4 | 89738 | 690 | 26 | 19.4 | 0 | 8.8 | clear | clear | 3 | 86.1 | 14.1 | 7.4 | symdir $5.61; polygon-bank $4.97; eia-bank $3.41; worldbank-full $3.35; research-backtest $1.88; feed-catalog $1.58 | enforce | 8.8 | None | 213.46 | 1 | 157 | 1101 | 24 | 71 | 1353 | 265.11 | 14.3 | 14.3 | 242.9 | 14.6 | 8.8 | 13.4 | 4.0 | 2026-11-20 4.45; 2026-11-21 6.17; 2026-11-22 3.49; 2026-11-23 3.45 | NORMAL | 2026-11-02T15:44:47.324976+00:00 | 7.1 | 2026-11-24 | 1282.59 | None | none | 7.6 | 7.6 | 1.6.1 | None | 8.3 | 8.1 | 8.1 |

## Log
## A. governor status

- `11:16:43` version 1.6.1 mode enforce stage NORMAL since 2026-11-02T15:44:47.324976+00:00
- `11:16:43` cap $600.0 (budget) | budget limit $600.0 actual $213.456 forecast $254.027
- `11:16:43` thresholds: {"baseline_usd_day": 7.91, "brake_at": 539.38, "cap": 600.0, "cap_at_risk_even_frozen": false, "days_left": 5.53, "freeze_at": 579.38, "freeze_line_floored": false, "frozen_eom": 224.08, "lag_buffer": 7.91, "mtd": 213.46, "projected_eom": 265.11, "reserve": 20.62, "residual_usd_day": 0.49, "sources": {"budget_actual": 213.456, "ce_mtd": 213.4563}}
- `11:16:43` held (stage/lever -> count): {"CAP/lambda": 3}
- `11:16:43` capped (CAP, manual release only): ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'ofrapi']
- `11:16:43` anomaly held (ANOM): none
- `11:16:43` fuse: {"checked_at": "2026-11-25T10:07:37.778399+00:00", "diffuse": false, "firing": [], "fleet_usd_day": 0.25, "monitor_notified": {"day": "2026-10-16", "fns": ["justhodl-event-coordinator", "justhodl-miss-calibrator"]}, "suspects": {}, "top": [["justhodl-stock-screener", 0.1], ["justhodl-signal-harvester", 0.06], ["justhodl-fundamental-graphs", 0.01], ["justhodl-backend-agent", 0.01], ["justhodl-a2a-bus", 0.01], ["justhodl-apac-flows", 0.01], ["justhodl-cost-anomaly", 0.01], ["justhodl-analytics-snapshot", 0.01]]}
- `11:16:43` suppress_until: None | last tick: {"decision": "NORMAL", "logs": ["meter: 1.0 h window, 18 functions ran, fleet $0.73/day; month-to-date top: justhodl-symdir $5.61, justhodl-polygon-bank $4.97, justhodl-eia-bank $3.41"], "mode": "enforce", "mtd": 213.46, "reason": "steady", "thresholds": {"baseline_source": "median14", "baseline_usd_day": 7.91, "brake_at": 539.37, "budget_forecast": 254.027, "budget_limit": 600.0, "cap": 600.0, "cap_at_risk_even_frozen": false, "cap_source": "budget", "days_left": 5.54, "freeze_at": 579.37, "freeze_line_floored": false, "frozen_eom": 224.08, "lag_buffer": 7.91, "mtd": 213.46, "projected_eom": 265.16, "reserve": 20.63, "residual_source": "min7", "residual_usd_day": 0.49, "sources": {"budget_a
- `11:16:43` cadence sentinel: {"checked": 844, "checked_at": "2026-11-25T00:07:37.621083+00:00", "day": "2026-11-25", "errors": [], "violators": []} | fixed entries: 0
- `11:16:43` anomalies: {"errors_notified": {"day": "2026-11-15", "fns": ["justhodl-fundamental-graphs"]}, "holds": {}, "month": "2026-11", "suspects": {}}
- `11:16:43` meter: {"active": 18, "daily": {"2026-11-11": {"MLPredictor": [1.0, 0.0], "alphavantage-market-agent": [8.0, 6e-05], "alphavantage-technical-analysis": [2.0, 2e-06], "bea-economic-agent": [7.0, 3.3e-05], "benzinga-news-agent": [3.0, 3.4e-05], "bls-employment-api-v2": [1.0, 3.8e-05], "bls-labor-agent": [5.0, 0.000232], "bond-indices-agent": [6.0, 0.000164], "census-economic-agent": [5.0, 0.000231], "cftc-futures-positioning-agent": [11.0, 0.001893], "chatgpt-agent-api": [4.0, 0.0], "coinmarketcap-agent": [2.0, 1.2e-05], "daily-liquidity-report": [1.0, 0.000101], "dollar-strength-agent": [6.0, 0.000523], "eia-energy-agent": [1.0, 6.8e-05], "enhanced-repo-agent": [7.0, 6.1e-05], "fedliquidityapi": [8.
- `11:16:43` storage: day 2026-11-24 | versioning None | warn_days None | trip_days None
- `11:16:43` storage last assessment: {"day": "2026-11-23", "delta_gb": 7.49, "delta_objects": 89738, "delta_pct": 0.59, "gb": 1282.59, "level": null, "objects": 10356960, "prev": "2026-11-22", "prev_gb": 1275.104, "reason": "growth +7.5 GB (+0.6%), +89738 objects over 2026-11-23"}
- `11:16:43` S3 $/day (storage.s3_usd): {"2026-11-10": 4.6142, "2026-11-11": 7.4233, "2026-11-12": 4.4004, "2026-11-13": 7.3488, "2026-11-14": 3.9307, "2026-11-15": 7.0286, "2026-11-16": 3.9717, "2026-11-17": 7.2245, "2026-11-18": 4.0734, "2026-11-19": 7.3582, "2026-11-20": 4.4508, "2026-11-21": 6.1709, "2026-11-22": 3.4863, "2026-11-23": 3.4513}
- `11:16:43` S3 line, last 8 days: 2026-11-16 $3.97, 2026-11-17 $7.22, 2026-11-18 $4.07, 2026-11-19 $7.36, 2026-11-20 $4.45, 2026-11-21 $6.17, 2026-11-22 $3.49, 2026-11-23 $3.45  (Nov 14-22: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17 / 3.49; Nov 23 is a 'high' day by the alternating pattern, the third day with the history book on write-if-changed)
- `11:16:43` service spend 2026-11-20: total $8.15 | Amazon Simple Storage Service $4.45, AWS Lambda $2.80, AmazonCloudWatch $0.45, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.09
- `11:16:43` service spend 2026-11-21: total $9.55 | Amazon Simple Storage Service $6.17, AWS Lambda $2.48, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.12, Amazon EC2 Container Registry  $0.09
- `11:16:43` service spend 2026-11-22: total $6.25 | Amazon Simple Storage Service $3.49, AWS Lambda $1.87, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, AWS Cost Explorer $0.11, Amazon EC2 Container Registry  $0.09
- `11:16:43` service spend 2026-11-23: total $6.37 | Amazon Simple Storage Service $3.45, AWS Lambda $2.05, AmazonCloudWatch $0.41, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.09
- `11:16:43` storage fuse advanced: day 2026-11-24 (was 2026-11-23 at the Nov 24 run)
- `11:16:43` object delta for 2026-11-23: +89738 (Nov 17-20 ran +42-49k/day; Nov 21 +99,943 Repo desk build; Nov 22 +81,712 Sovereign desk build)
- `11:16:43` ⚠ object delta +89738 is a THIRD day near double the usual, and Nov 23 had no desk build behind it - a per-object writer needs finding (list the day's new keys by prefix next)
- `11:16:43` storage_selftest ok True | scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} | live versioning Enabled | live error None
- `11:16:43` storage_selftest live days: {"2026-11-19": {"gb": 1256.563, "objects": 10036646}, "2026-11-20": {"gb": 1262.27, "objects": 10085567}, "2026-11-21": {"gb": 1267.64, "objects": 10185510}, "2026-11-22": {"gb": 1275.104, "objects": 10267222}, "2026-11-23": {"gb": 1282.59, "objects": 10356960}}
- `11:16:43` storage_selftest live partial: {}
## B. meter: month-to-date top engines against the $50 per-engine cap

- `11:16:44` engine cap $50.0/month | overrides {} | held [['justhodl-event-coordinator', 431.7289, '2026-10-20T19:42:48.856487+00:00'], ['justhodl-miss-calibrator', 1551.2815, '2026-10-20T19:42:48.856487+00:00'], ['ofrapi', None, '2026-10-20T20:55:54.358869+00:00']]
- `11:16:44` released: {"justhodl-spend-governor-canary": {"at": "2026-11-03T06:33:34.823234+00:00", "mtd_at_release": 0.0, "reason": "ops 6718: trigger-cap live proof complete"}, "macro-financial-intelligence": {"at": "2026-11-02T16:14:44.475751+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}, "treasury-api": {"at": "2026-11-02T16:14:45.773612+00:00", "mtd_at_release": 0.0, "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"}}
- `11:16:44` month-to-date top 25: symdir $5.61, polygon-bank $4.97, eia-bank $3.41, worldbank-full $3.35, research-backtest $1.88, feed-catalog $1.58, bank-histories $1.54, causality-scanner $1.32, data-census $0.84, signal-genealogy $0.80, fmp-walker $0.76, predictability $0.61, thesis-engine $0.61, katlin $0.60, tradingview $0.59, stock-screener $0.57, chart-patterns $0.57, wl-engines $0.53, bottom $0.53, fmp-bank $0.52
## C. governor event records since ops 6934 (2026-11-24 11:19 UTC) and the relay marker

- `11:16:44` event records since the Nov 24 run: 0 | kinds {} | newest -
- `11:16:44` relay marker 242.9 h old: 3 relayed keys | {"last_key": "***.868478Z-error-storm.json", "updated": "2026-11-15T08:24:57.422562Z", "issues_opened": 1}
## D. freshness of declared outputs (HEAD, bucket justhodl-dashboard-live)

- `11:16:44` manifest 2026-11-24T23:29:39.141355+00:00: 784 engines, 1353 declared data/ keys
- `11:16:49` outputs by tier: {'DEAD': 157, 'FRESH': 1101, 'STALE': 71, 'MISSING': 24} (Tue Nov 24, ops 6934: fresh 1107, stale 65, dead 157, missing 24 of 1,353; Mon Nov 23: 979 / 194 / 156 / 24; Wed Nov 18 read 1081 / 58 / 155 / 25)
- `11:16:49` SILENT engines - ALL outputs 54 h - 7 d old (Tue: 17, all weekly or event-driven slots; Mon: 85; a Wednesday should read near 15-30): 26
- `11:16:49`   naaim: 1 output(s) 114-114 h
- `11:16:49`   liquidity-profile: 1 output(s) 109-109 h
- `11:16:49`   base-rates: 1 output(s) 93-93 h
- `11:16:49`   spx-beaters: 1 output(s) 93-93 h
- `11:16:49`   beaters-grader: 1 output(s) 92-92 h
- `11:16:49`   cftc-deep-view: 2 output(s) 86-86 h
- `11:16:49`   ici-flows: 1 output(s) 86-86 h
- `11:16:49`   forward-returns: 1 output(s) 80-80 h
- `11:16:49`   symbol-dictionary: 1 output(s) 78-78 h
- `11:16:49`   xbrl-fundamentals: 2 output(s) 77-77 h
- `11:16:49`   spx-history: 1 output(s) 75-75 h
- `11:16:49`   alpha-calibrator: 2 output(s) 74-74 h
- `11:16:49`   gsi-calibrator: 1 output(s) 74-74 h
- `11:16:49`   jsi-calibrator: 1 output(s) 74-74 h
- `11:16:49`   gsi-horizons: 1 output(s) 74-74 h
- `11:16:49`   signal-orthogonality: 1 output(s) 74-74 h
- `11:16:49`   edge-discovery: 2 output(s) 72-72 h
- `11:16:49`   portwatch: 2 output(s) 72-72 h
- `11:16:49`   bagger-engine: 1 output(s) 71-71 h
- `11:16:49`   strategy-portfolio: 1 output(s) 71-71 h
- `11:16:49`   hiring-velocity: 1 output(s) 71-71 h
- `11:16:49`   provider-window-sentinel: 1 output(s) 69-69 h
- `11:16:49`   ma-target-predictor: 1 output(s) 69-69 h
- `11:16:49`   weekly-ai-review: 1 output(s) 69-69 h
- `11:16:49`   meta-improver: 1 output(s) 61-61 h
- `11:16:49`   gdelt-sentiment: 1 output(s) 56-56 h
- `11:16:49` engines with at least one output < 30 h: 690 of 784 (Tue: 695 of 784; Mon: 624; Sun: 626)
- `11:16:49` STALE 30h-7d (71), youngest 40: _fleet-monitor-alert-history.json 33h, source-intel.json 33h, methodology-gaps.json 33h, methodology-kb.json 33h, readthrough-state.json 37h, sellside-views.json 45h, whales.json 46h, index-inclusion.json 46h, buyback-scanner.json 47h, warm/imf-full/_state/state.json 48h, warm/imf-full/manifest.json 48h, smart-money-13f.json 48h, seasonality.json 48h, bis-crossborder.json 49h, fundgraph/symdir2.json 50h, _state/sec-bulk.json 50h, 13f-clone-alpha.json 51h, playbook-rules-public.json 52h, playbook-rules.json 52h, audit/fabrication-weekly.json 53h, history/signal-halflife-history.json 53h, signal-halflife.json 53h, gdelt-news.json 56h, cycle-clock.json 60h, meta-improver-state.json 61h, symdir/fred-titles.json 62h, weekly-review-latest.json 69h, ma-target-predictor.json 69h, provider-window-sentinel.json 69h, port-cargo.json 71h, hiring-velocity.json 71h, strategy-portfolio.json 71h, bagger-engine.json 71h, portwatch.json 72h, warm/portwatch/history/daily-rows.json.gz 72h, edge-discovery-graduated.json 72h, edge-discovery.json 72h, signal-orthogonality.json 74h, katlin-backtest.json 74h, gsi-horizons.json 74h
- `11:16:49` DEAD >=7d (157); crossed the line since the Nov 24 run (1): feed-catalog.json 172h<-justhodl-feed-catalog
- `11:16:49` ⚠ 1 declared output(s) crossed the 7-day line since yesterday: feed-catalog.json<-justhodl-feed-catalog
- `11:16:49` MISSING objects (24): _state/signal-alpha-map.json<-justhodl-signal-scorecard, _telegram-chat.json<-justhodl-alpha-compass, _tmp/trend-reversal-partial.json<-justhodl-trend-reversal, activist-radar.json<-justhodl-activist-radar, biotech-catalysts.json<-justhodl-biotech-catalysts, earnings-transcripts.json<-justhodl-earnings-transcripts, engine-robustness.json<-justhodl-engine-robustness, ipo-calendar.json<-justhodl-ipo-calendar, kill-switch-state.json<-justhodl-kill-switch, prediction-markets.json<-justhodl-prediction-markets, regsho-threshold.json<-justhodl-regsho, retail-attention.json<-justhodl-retail-attention, sentiment-surveys.json<-justhodl-sentiment-surveys, signal-genealogy-research/cache/current.json<-justhodl-signal-genealogy, symbology/bond-cusips.json<-justhodl-symbology-master, symdir/wb-indicators.json.gz<-justhodl-symdir, transcripts-index.json<-justhodl-transcript-indexer, tv-descriptions.json<-justhodl-tv-notes-ingest, usaspending-contracts.json<-justhodl-usaspending, uspto-patents.json<-justhodl-uspto-patents, warm/asia-trade/kr/403Y001__exports-imports.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y009__industrial-production.json.gz<-justhodl-asia-trade-full, warm/asia-trade/kr/901Y011__manufacturing-shipments.json.gz<-justhodl-asia-trade-full, warm/blackswan/vn_VNINDEX.json<-justhodl-blackswan-watch,justhodl-bottom
- `11:16:49` watched outputs (desks, banks, monitors):
- `11:16:49`   data/strength.json: 7.6 h old (4416409 B) -> FRESH
- `11:16:49`   data/strength-wide.json: 7.6 h old (2130058 B) -> FRESH
- `11:16:49`   data/leaders.json: 7.4 h old (3091270 B) -> FRESH
- `11:16:49`   data/bottom.json: 7.5 h old (2284095 B) -> FRESH
- `11:16:49`   data/stock-volatility.json: 7.1 h old (2788641 B) -> FRESH
- `11:16:49`   data/campaign-scan.json: 7.2 h old (2198706 B) -> FRESH
- `11:16:49`   data/etf-true-flows.json: 19.4 h old (1119806 B) -> FRESH
- `11:16:50`   data/ici-flows.json: 86.1 h old (32177 B) -> STALE
- `11:16:50`   data/13f-book.json: 18.6 h old (2439688 B) -> FRESH
- `11:16:50`   data/warm/polygon-full/eod/manifest.json: 8.1 h old (1567 B) -> FRESH
- `11:16:50`   data/warm/factor-book/manifest.json: 8.3 h old (20391 B) -> FRESH
- `11:16:50`   data/warm/history-book/manifest.json: 8.1 h old (11585 B) -> FRESH
- `11:16:50`   data/corporate-actions/_state.json: 4.5 h old (66 B) -> FRESH
- `11:16:50`   data/_freshness-monitor.json: 3.3 h old (1105663 B) -> FRESH
- `11:16:50`   _health/fleet.json: 8.8 h old (9730 B) -> FRESH
- `11:16:50`   _health/cadence-manifest.json: 11.6 h old (112521 B) -> FRESH
- `11:16:50`   data/ciss-stress.json: 4.1 h old (4619512 B) -> FRESH
- `11:16:50`   data/ciss-sovereign.json: 4.0 h old (820784 B) -> FRESH
- `11:16:50`   data/ciss-sovereign-world.json: 4.0 h old (730666 B) -> FRESH
- `11:16:50`   data/ciss-dollar-shortage.json: 4.0 h old (1020678 B) -> FRESH
- `11:16:50`   data/ciss-sovereign-history.json: 4.0 h old (2333634 B) -> FRESH
- `11:16:50`   data/10kq-filings.json: 222.1 h old (6573546 B) -> DEAD
- `11:16:51`   data/insider-trades.json: 14.1 h old (1905672 B) -> FRESH
- `11:16:51`   data/regime-map.json: 14.3 h old (222757 B) -> FRESH
- `11:16:51`   data/regime-map-prev.json: 14.3 h old (13660 B) -> FRESH
- `11:16:51`   data/credit-before-equity.json: 11.4 h old (1547950 B) -> FRESH
- `11:16:51`   credit/credit-before-equity-history.json: 11.4 h old (120342 B) -> FRESH
## E. FMP bandwidth hold (walker + bank manifests) - the 30-day window rolls ~Nov 24-27; today is Nov 25, day two

- `11:16:51` data/warm/fmp/manifest.json (11.6 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
- `11:16:51` data/warm/fmp-bank/manifest.json (11.9 h old): held False until None | {"version": "1.3.0", "key_status": "ok"}
## F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)

- `11:16:51` data/_fleet-monitor.json (8.8 h old): version 1.3.0 | scanned None | detected None | raised 1 | dlq {} | sns_sent False telegram_sent False
- `11:16:51`   alert: {"lambda": "<DLQ>", "severity": "WARNING", "error_category": "DLQ_BACKLOG", "diagnostic_text_private": true, "dlq_depth": {"available": true, "visible": 3231, "inflight": 0, "total": 3231}}
## G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)

- `11:16:51` data/ciss-research/sovereign: 7 object(s) | newest world-state.json 4.0 h | oldest b5f96afde2fe6bf541a1fae8277fb38abf4977bb623de3fab0 67.7 h
- `11:16:52` data/repo-market: 2 object(s) | newest repo-market-history.json 13.4 h | oldest repo-market-history.json 13.4 h
- `11:16:52` data/dealer: 60 object(s)+ | newest dealer-gex.json 14.6 h | oldest migration.json 1607.0 h
- `11:16:52` data/warm/ofr: 60 object(s)+ | newest _last-check.json 8.8 h | oldest FICC-SPONSORED_REPO_VOL.json.gz 2418.9 h
- `11:16:52` data/ofr: 3 object(s) | newest ofr-fsi.json 8.8 h | oldest ofr-stfm.json 2687.8 h
## H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')

- `11:16:52` control 47.6 d old: contract capital-structure-recurring-refresh.v1 | status failed | started 2026-10-08T20:28:05.951329+00:00 | failed_at 2026-10-08T22:00:40.571580+00:00 | error_type ValueError | active_phase part-2 | completed ['plan', 'part-1'] | review {}
- `11:16:52` ⚠ capital-structure control still 'failed' (since 2026-10-08T22:00:40.571580+00:00) - every scheduled plan stops at 04:46 UTC until release_failed_control runs with an ops number and a review note (Khalid's call; this run does not release it)
- `11:16:52` ✅ read-only; nothing changed, nothing released, nothing re-invoked

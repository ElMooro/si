# Sample unsurfaced S3 data

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-04-26T01:07:30+00:00  

## Log
## A. Direct file samples

## 📄 _health/dashboard.json

- `01:07:28`   size=30586B  mod=2026-04-26 01:00
- `01:07:28`   generated_at: "2026-04-26T01:00:08.048782+00:00"
- `01:07:28`   checked_at_unix: 1777165217
- `01:07:28`   duration_sec: 9.359875
- `01:07:28`   system_status: "red"
- `01:07:28`   counts: dict(5 keys)
- `01:07:28`     green: 58
- `01:07:28`     yellow: 0
- `01:07:28`     red: 18
- `01:07:28`     info: 2
- `01:07:28`     unknown: 0
- `01:07:28`   total_components: 78
- `01:07:28`   components: list[78 dicts]
- `01:07:28`     [0]:
- `01:07:28`       id: "lambda:justhodl-intelligence"
- `01:07:28`       type: "lambda"
- `01:07:28`       name: "justhodl-intelligence"
- `01:07:28`       note: "Cross-system synthesis. FIXED 2026-04-25 (adapter pattern)."
- `01:07:28`       severity: "critical"
- `01:07:28`       invocations_24h: 4
- `01:07:28`       errors_24h: 0
- `01:07:28`       error_rate_24h: 0.0
- `01:07:28`       status: "red"
- `01:07:28`       reason: "only 4 invocations in 24h (expected \u226510)"
## 📄 _health/last_alerted.json

- `01:07:28`   size=10084B  mod=2026-04-26 01:00
- `01:07:28`   components: dict(78 keys)
- `01:07:28`     lambda:justhodl-intelligence: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "critical"
- `01:07:28`       last_alerted_at: 1777151716
- `01:07:28`     lambda:justhodl-repo-monitor: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "critical"
- `01:07:28`       last_alerted_at: 1777146314
- `01:07:28`     s3:intelligence-report.json: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "critical"
- `01:07:28`       last_alerted_at: 1777083316
- `01:07:28`     s3:repo-data.json: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "critical"
- `01:07:28`       last_alerted_at: 1777119317
- `01:07:28`     lambda:ecb-data-daily-updater: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "important"
- `01:07:28`       last_alerted_at: 0
- `01:07:28`     lambda:fmp-stock-picks-agent: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "important"
- `01:07:28`       last_alerted_at: 0
- `01:07:28`     lambda:global-liquidity-agent-v2: dict(3 keys)
- `01:07:28`       status: "red"
- `01:07:28`       severity: "important"
- `01:07:28`       last_alerted_at: 0
- `01:07:28`     lambda:justhodl-data-collector: dict(3 keys)
## 📄 data/khalid-metrics.json

- `01:07:28`   size=6522B  mod=2026-04-25 11:01
- `01:07:28`   metrics: dict(37 keys)
- `01:07:28`     RESPPALGUOXAWXCH52NWW: dict(6 keys)
- `01:07:28`       current: 195355.0
- `01:07:28`       1w: 3.89
- `01:07:28`       1m: 50.64
- `01:07:28`       3m: 742.21
- `01:07:28`       6m: 221.19
- `01:07:28`       1y: 160.67
- `01:07:28`     RESPPALGUONNWW: dict(6 keys)
- `01:07:28`       current: 3995053.0
- `01:07:28`       1w: 0.01
- `01:07:28`       1m: 0.04
- `01:07:28`       3m: 0.05
- `01:07:28`       6m: -0.15
- `01:07:28`       1y: -0.68
- `01:07:28`     BOGMBASE: dict(6 keys)
- `01:07:28`       current: 5388.0
- `01:07:28`       1w: null
- `01:07:28`       1m: -0.27
- `01:07:28`       3m: 1.62
- `01:07:28`       6m: -5.24
- `01:07:28`       1y: -6.7
- `01:07:28`     WFCDA: dict(6 keys)
- `01:07:28`       current: 19302.0
- `01:07:28`       1w: -0.55
- `01:07:28`       1m: 0.94
- `01:07:28`       3m: 0.2
- `01:07:28`       6m: -0.17
- `01:07:28`       1y: 0.06
- `01:07:28`     SWP1690: dict(6 keys)
## 📄 data/khalid-config.json

- `01:07:28`   size=19557B  mod=2026-02-27 06:44
- `01:07:28`   metrics: list[84 dicts]
- `01:07:28`     [0]:
- `01:07:28`       id: "SOFR"
- `01:07:28`       name: "SOFR Rate"
- `01:07:28`       source: "fred"
- `01:07:28`       category: "Liquidity & Repos"
- `01:07:28`       weight: 9
- `01:07:28`       flash: "red_up"
- `01:07:28`       enabled: true
- `01:07:28`   categories: list[11] ["Fed Balance Sheet", "Liquidity & Repos", "Money Markets"]
- `01:07:28`   version: 13
## 📄 data/khalid-analysis.json

- `01:07:28`   size=11319B  mod=2026-04-25 11:02
- `01:07:28`   plumbing_health: dict(6 keys)
- `01:07:28`     score: 76
- `01:07:28`     grade: "B"
- `01:07:28`     summary: "Financial plumbing shows moderate health with Fed balance sheet stable and ECB 
- `01:07:28`     key_signals: list[6] ["ECB deposit rate cut 46.67% to 2.0% vs Fed stability signals divergence", "Eur
- `01:07:28`     stress_points: list[4] ["Major data gaps in critical overnight rates (SOFR, EFFR) limiting visibility",
- `01:07:28`     positive_signs: list[4] ["European monetary easing with ECB cutting across all facilities", "Strong Euro
- `01:07:28`   crisis_comparison: dict(6 keys)
- `01:07:28`     current_vs_2008: dict(3 keys)
- `01:07:28`       similarity_pct: 15
- `01:07:28`       summary: "Current environment shows none of the systemic credit freeze or dealer stress t
- `01:07:28`       key_differences: list[3] ["No credit freeze - commercial lending growing 5.69% vs collapse in 2008", "Cen
- `01:07:28`     current_vs_2020: dict(3 keys)
- `01:07:28`       similarity_pct: 25
- `01:07:28`       summary: "Unlike 2020's liquidity crisis requiring emergency facilities, current period s
- `01:07:28`       key_differences: list[3] ["No emergency lending facilities active vs massive programs in 2020", "Orderly 
- `01:07:28`     current_vs_2022: dict(3 keys)
- `01:07:28`       similarity_pct: 65
- `01:07:28`       summary: "Closer parallel to 2022 with central bank policy divergence and currency volati
- `01:07:28`       key_differences: list[3] ["ECB cutting rates vs aggressive hiking cycle in 2022", "European credit expans
- `01:07:28`     closest_historical_analog: "Mid-2019 Fed pivot"
- `01:07:28`     crisis_probability_6mo: 20
- `01:07:28`     crisis_type_if_occurs: "Dollar funding stress from policy divergence"
- `01:07:28`   risk_regime: dict(8 keys)
- `01:07:28`     stance: "TRANSITIONING"
- `01:07:28`     confidence: 72
- `01:07:28`     summary: "Markets transitioning from risk-off to cautiously risk-on as ECB easing acceler
- `01:07:28`     risk_on_signals: list[4] ["ECB aggressive easing with deposit rate cut to 2.0%", "European bank lending a
- `01:07:28`     risk_off_signals: list[4] ["Elevated Fed discount window usage up 16.73% YoY", "Rising European sovereign 
- `01:07:28`     regime_duration_estimate: "3-6 months"
## 📄 reports/scorecard.json

- `01:07:28`   size=50661B  mod=2026-04-26 00:23
- `01:07:28`   meta: dict(9 keys)
- `01:07:28`     generated_at: "2026-04-26T00:23:51.612651+00:00"
- `01:07:28`     signals_total: 4879
- `01:07:28`     outcomes_total: 4410
- `01:07:28`     scored_outcomes: 0
- `01:07:28`     has_calibration: false
- `01:07:28`     calibration_summary: dict(3 keys)
- `01:07:28`       weights_count: 25
- `01:07:28`       accuracy_count: 0
- `01:07:28`       report_keys: list[10] ["generated_at", "total_outcomes", "signal_types_tracked"]
- `01:07:28`     is_meaningful: false
- `01:07:28`     n_calibrated_signals: 0
- `01:07:28`     n_signals_with_outcomes: 0
- `01:07:28`   signal_scorecard: list[15 dicts]
- `01:07:28`     [0]:
- `01:07:28`       signal_type: "screener_top_pick"
- `01:07:28`       total: 955
- `01:07:28`       scored: 0
- `01:07:28`       correct: 0
- `01:07:28`       hit_rate: null
- `01:07:28`       avg_magnitude_error_pct: null
- `01:07:28`       by_horizon: dict(0 keys)
- `01:07:28`       trend_30d: null
- `01:07:28`       trend_60d: null
- `01:07:28`       trend_90d: null
- `01:07:28`       calibrator_weight: 0.85
- `01:07:28`   khalid_timeline: list[250 dicts]
- `01:07:28`     [0]:
- `01:07:28`       ts: "2026-04-24T23:42:13.103532+00:00"
- `01:07:28`       date: "2026-04-24"
## 📄 sentiment/news.json

- `01:07:28` ⚠   ⚠ does not exist
## 📄 secretary/findings.json

- `01:07:28` ⚠   ⚠ does not exist
## 📄 learning/prompt_templates.json

- `01:07:28`   size=877B  mod=2026-04-25 14:59
- `01:07:28`   _version: 1
- `01:07:28`   _initialized_at: "2026-04-25T14:59:20.299590+00:00"
- `01:07:28`   _note: "Loop 3 template store. Iterator at justhodl-prompt-iterator updates this weekly
- `01:07:28`   morning_brief: "You are JustHodlAI, institutional-grade autonomous financial intelligence. Gene
- `01:07:28`   improvement_writer: "You are a quant analyst reviewing JustHodlAI prediction failures. Produce conci
## 📄 learning/improvement_log.json

- `01:07:29`   size=151B  mod=2026-04-25 14:59
## B. Prefix contents

## 📁 investor-analysis/

- `01:07:29`   2 objects
- `01:07:29`     investor-analysis/AAPL.json                                 7074B    73.6h
- `01:07:29`     investor-analysis/NVDA.json                                 5758B   677.3h
- `01:07:29` 
  Sample: investor-analysis/AAPL.json
- `01:07:29`     ticker: "AAPL"
- `01:07:29`     name: "Apple Inc."
- `01:07:29`     sector: "Technology"
- `01:07:29`     price: 273.17
- `01:07:29`     metrics: dict(33 keys)
- `01:07:29`       ticker: "AAPL"
- `01:07:29`       name: "Apple Inc."
- `01:07:29`       sector: "Technology"
- `01:07:29`       industry: "Consumer Electronics"
- `01:07:29`       price: 273.17
- `01:07:29`       mktCap: 0.0
- `01:07:29`       pe: 0
- `01:07:29`       pb: 45.68
- `01:07:29`       priceToSales: 9.22
- `01:07:29`       pfcf: 0
## 📁 calibration/

- `01:07:29`   8 objects
- `01:07:29`     calibration/history/2026-04-25.json                         1058B     4.2h
- `01:07:29`     calibration/latest.json                                     1058B     4.2h
- `01:07:29`     calibration/history/2026-04-19.json                         3899B   160.1h
- `01:07:29`     calibration/history/2026-04-12.json                         3895B   328.1h
- `01:07:29`     calibration/history/2026-04-05.json                         3891B   496.1h
- `01:07:29`     calibration/history/2026-03-29.json                         3891B   664.1h
- `01:07:29`     calibration/history/2026-03-22.json                         3242B   832.1h
- `01:07:29`     calibration/history/2026-03-15.json                         2031B  1000.1h
- `01:07:29` 
  Sample: calibration/history/2026-04-25.json
- `01:07:29`     generated_at: "2026-04-25T20:53:58.639012+00:00"
- `01:07:29`     total_outcomes: 0
- `01:07:29`     signal_types_tracked: 0
- `01:07:29`     weights: dict(25 keys)
- `01:07:29`       khalid_index: 1.0
- `01:07:29`       screener_top_pick: 0.85
- `01:07:29`       valuation_composite: 0.8
- `01:07:29`       cftc_gold: 0.8
- `01:07:29`       cftc_spx: 0.8
- `01:07:29`       cftc_bitcoin: 0.75
- `01:07:29`       cftc_crude: 0.7
- `01:07:29`       edge_regime: 0.75
- `01:07:29`       edge_composite: 0.7
- `01:07:29`       market_phase: 0.75
- `01:07:29`       crypto_btc_signal: 0.7
## 📁 _audit/

- `01:07:29`   7 objects
- `01:07:29`     _audit/plan_vs_usage_2026-04-25.md                          4499B    14.9h
- `01:07:29`     _audit/feature_audit_2026-04-25.md                          9625B    22.9h
- `01:07:29`     _audit/ddb_pre_delete_20260425_015658.json                 22645B    23.2h
- `01:07:29`     _audit/broken_lambdas_2026-04-25.md                        10479B    23.3h
- `01:07:29`     _audit/cost_audit_2026-04-25.md                             4988B    23.4h
- `01:07:29`     _audit/system_architecture_2026-04-25.md                   35376B    24.4h
- `01:07:29`     _audit/inventory_2026-04-25.json                          783313B    24.6h
- `01:07:29` 
  Sample: _audit/ddb_pre_delete_20260425_015658.json
- `01:07:29`     snapshot_at: "2026-04-25T01:56:58.788219+00:00"
- `01:07:29`     purpose: "Pre-deletion record of empty DDB tables. Can be used to recreate if needed."
- `01:07:29`     tables_to_delete: list[18 dicts]
- `01:07:29`       [0]:
- `01:07:29`         name: "APIKeys"
- `01:07:29`         size_bytes: 0
- `01:07:29`         items: 0
- `01:07:29`         billing: "PAY_PER_REQUEST"
- `01:07:29`         key_schema: list[1 dicts]
- `01:07:29`         attribute_definitions: list[1 dicts]
- `01:07:29`         creation_date: "2025-06-15 23:33:05.773000+00:00"
- `01:07:29`         status: "ACTIVE"
- `01:07:29`     all_tables_at_snapshot_time: list[25 dicts]
- `01:07:29`       [0]:
- `01:07:29`         name: "APIKeys"
## 📁 deploy/

- `01:07:29`   3 objects
- `01:07:29`     deploy/lambda_function.py                                  18855B  1314.7h
- `01:07:29`     deploy/secretary-lambda.zip                                10854B  1319.7h
- `01:07:29`     deploy/crypto-intel-v2.zip                                  8821B  1366.9h
## 📁 stock-ai/

- `01:07:29`   2 objects
- `01:07:29`     stock-ai/TSM.json                                           4231B     1.3h
- `01:07:29`     stock-ai/AAPL.json                                          3960B     1.5h
- `01:07:29` 
  Sample: stock-ai/TSM.json
- `01:07:29`     ticker: "TSM"
- `01:07:29`     company: dict(9 keys)
- `01:07:29`       name: "Taiwan Semiconductor Manufacturing Company Limited"
- `01:07:29`       sector: "Technology"
- `01:07:29`       industry: "Semiconductors"
- `01:07:29`       ceo: "C. C. Wei"
- `01:07:29`       employees: 65152.0
- `01:07:29`       ipo_date: "1997-10-09"
- `01:07:29`       website: "https://www.tsmc.com"
- `01:07:29`       country: "TW"
- `01:07:29`       exchange: "NYSE"
- `01:07:29`     snapshot: dict(14 keys)
- `01:07:29`       price: 402.46
- `01:07:29`       market_cap: 2087362534890.0
- `01:07:29`       pe: 29.4
## 📁 stock-analysis/

- `01:07:29`   2 objects
- `01:07:29`     stock-analysis/TSM.json                                   107551B     2.2h
- `01:07:29`     stock-analysis/AAPL.json                                  112491B  1100.0h
- `01:07:29` 
  Sample: stock-analysis/TSM.json
- `01:07:29`     ticker: "TSM"
- `01:07:29`     name: "Taiwan Semiconductor Manufacturing Company Limited"
- `01:07:29`     generated_at: "2026-04-25T22:55:36.277559+00:00"
- `01:07:29`     technicals: dict(25 keys)
- `01:07:29`       price: 402.46
- `01:07:29`       sma20: 185.93
- `01:07:29`       sma50: 203.99
- `01:07:29`       sma200: 264.49
- `01:07:29`       ema12: 176.76
- `01:07:29`       ema26: 184.13
- `01:07:29`       rsi: 19.2
- `01:07:29`       macd: -7.37
- `01:07:29`       stoch_rsi: 4.3
- `01:07:29`       atr: 5.99
- `01:07:29`       bb_upper: 205.99
## 📁 khalid/

- `01:07:29`   1 objects
- `01:07:29`     khalid/index.html                                          69260B  1386.1h
## 📁 secretary/

- `01:07:29`   1 objects
- `01:07:29`     secretary/index.html                                       17322B  1319.5h
## 📁 sentiment/

- `01:07:29`   1 objects
- `01:07:29`     sentiment/data.json                                        80176B    18.8h
- `01:07:29` 
  Sample: sentiment/data.json
- `01:07:29`     generated_at: "2026-04-25T06:17:50.043229+00:00"
- `01:07:29`     generated_at_unix: 1777097870
- `01:07:29`     elapsed_seconds: 148.9
- `01:07:29`     count: 503
- `01:07:29`     bullish_count: 0
- `01:07:29`     bearish_count: 0
- `01:07:29`     neutral_count: 503
- `01:07:29`     sentiment: list[503 dicts]
- `01:07:29`       [0]:
- `01:07:29`         symbol: "CASY"
- `01:07:29`         name: "Casey's General Stores, Inc."
- `01:07:29`         sentimentScore: 0.0
- `01:07:29`         sentimentSignal: "neutral"
- `01:07:29`         sentimentReason: "No recent news"
- `01:07:29`         headlines: list[0] []
## 📁 telegram/

- `01:07:29`   1 objects
- `01:07:29`     telegram/alert_state.json                                    167B     0.1h
- `01:07:30` 
  Sample: telegram/alert_state.json
- `01:07:30`     subscribers: list[1] [8678089260]
- `01:07:30`     last_regime: "BEAR"
- `01:07:30`     last_khalid: 43
- `01:07:30`     last_crypto_fear: null
- `01:07:30`     last_fear: 33
- `01:07:30`     last_briefing: "2026-04-25T07:01:12.496347+00:00"
- `01:07:30` Done

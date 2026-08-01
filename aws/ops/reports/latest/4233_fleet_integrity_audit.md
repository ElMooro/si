# ops 4233 — full-fleet integrity audit

**Status:** success  
**Duration:** 140.1s  
**Finished:** 2026-08-01T14:25:52+00:00  

## Data

| avg_s | error_pct | errors | expr | function | guarded | invocations | invocations_14d | limit_gb | max_s | pct | section | throttles | timeout_s | used_gb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 322.12 |  | 0.0 | storage |  |  | 0.11 |
|  |  |  |  | justhodl-13f-clone-alpha | False |  |  |  |  |  | D1_self_invoke |  |  |  |
|  |  |  |  | justhodl-equity-research | False |  |  |  |  |  | D1_self_invoke |  |  |  |
|  |  |  |  | justhodl-fundamental-census | True |  |  |  |  |  | D1_self_invoke |  |  |  |
| 4.4 |  |  |  | justhodl-market-tape |  | 4173 |  |  | 30.0 |  | D2_timeout_clipped |  | 30 |  |
| 26.6 |  |  |  | fedliquidityapi |  | 342 |  |  | 30.0 |  | D2_timeout_clipped |  | 30 |  |
| 50.0 |  |  |  | xccy-basis-agent |  | 314 |  |  | 300.0 |  | D2_timeout_clipped |  | 300 |  |
| 2.6 |  |  |  | enhanced-repo-agent |  | 308 |  |  | 60.0 |  | D2_timeout_clipped |  | 60 |  |
| 92.5 |  |  |  | bond-indices-agent |  | 303 |  |  | 300.0 |  | D2_timeout_clipped |  | 300 |  |
| 39.4 |  |  |  | bls-labor-agent |  | 300 |  |  | 180.0 |  | D2_timeout_clipped |  | 180 |  |
| 51.5 |  |  |  | dollar-strength-agent |  | 257 |  |  | 60.0 |  | D2_timeout_clipped |  | 60 |  |
| 49.6 |  |  |  | volatility-monitor-agent |  | 256 |  |  | 60.0 |  | D2_timeout_clipped |  | 60 |  |
| 51.8 |  |  |  | securities-banking-agent |  | 255 |  |  | 60.0 |  | D2_timeout_clipped |  | 60 |  |
| 53.7 |  |  |  | manufacturing-global-agent |  | 252 |  |  | 60.0 |  | D2_timeout_clipped |  | 60 |  |
| 495.3 |  |  |  | justhodl-tradingview |  | 118 |  |  | 900.0 |  | D2_timeout_clipped |  | 900 |  |
| 119.5 |  |  |  | justhodl-fundamental-census |  | 117 |  |  | 900.0 |  | D2_timeout_clipped |  | 900 |  |
| 300.0 |  |  |  | justhodl-search-attention |  | 84 |  |  | 300.0 |  | D2_timeout_clipped |  | 300 |  |
| 96.1 |  |  |  | justhodl-outcome-checker |  | 62 |  |  | 300.0 |  | D2_timeout_clipped |  | 300 |  |
| 111.1 |  |  |  | justhodl-signal-scorecard |  | 60 |  |  | 120.0 |  | D2_timeout_clipped |  | 120 |  |
| 64.8 |  |  |  | justhodl-china-liquidity |  | 55 |  |  | 300.0 |  | D2_timeout_clipped |  | 300 |  |
| 75.9 |  |  |  | justhodl-fx-intelligence |  | 54 |  |  | 120.0 |  | D2_timeout_clipped |  | 120 |  |
| 59.3 |  |  |  | justhodl-cb-injection |  | 47 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 65.3 |  |  |  | justhodl-air-cargo |  | 41 |  |  | 300.0 |  | D2_timeout_clipped |  | 180 |  |
| 84.4 |  |  |  | justhodl-global-liquidity |  | 41 |  |  | 120.0 |  | D2_timeout_clipped |  | 120 |  |
| 41.2 |  |  |  | justhodl-consumer-pulse |  | 40 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 62.0 |  |  |  | justhodl-yen-carry |  | 40 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 103.2 |  |  |  | justhodl-construction-housing |  | 39 |  |  | 120.0 |  | D2_timeout_clipped |  | 120 |  |
| 173.1 |  |  |  | justhodl-gdelt-buzz |  | 39 |  |  | 180.0 |  | D2_timeout_clipped |  | 180 |  |
| 39.6 |  |  |  | justhodl-boj-detail |  | 38 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 59.4 |  |  |  | justhodl-cds-proxy |  | 38 |  |  | 120.0 |  | D2_timeout_clipped |  | 120 |  |
| 544.6 |  |  |  | justhodl-data-census |  | 38 |  |  | 900.0 |  | D2_timeout_clipped |  | 900 |  |
| 40.5 |  |  |  | justhodl-snb-detail |  | 38 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 40.8 |  |  |  | justhodl-activity-nowcast |  | 36 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
| 60.8 |  |  |  | justhodl-ecb-detail |  | 35 |  |  | 90.0 |  | D2_timeout_clipped |  | 90 |  |
|  |  |  | cron(10 7 1 * ? *) | justhodl-warroom-weights |  |  | 1 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 8 ? * MON *) | justhodl-13f-clone-alpha |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 9 ? * SUN *) | justhodl-gsi-horizons |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 14 ? * SUN *) | justhodl-ma-target-predictor |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 7 ? * MON *) | justhodl-playbook-engine |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(45 9 ? * SUN *) | justhodl-signal-orthogonality |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(10 8 ? * SUN *) | justhodl-spx-history |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(10 13 ? * MON *) | justhodl-whales |  |  | 2 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 7 2,16 * ? *) | justhodl-fi-census |  |  | 3 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(15 12 * * ? *) | justhodl-indicator-bus |  |  | 3 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 6 2,16 * ? *) | justhodl-etf-census |  |  | 4 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(5 12 * * ? *) | justhodl-macro-attribution |  |  | 5 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(45 11 * * ? *) | justhodl-cot-feed |  |  | 6 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(10 15 ? * MON,THU *) | justhodl-equity-ftd |  |  | 6 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(50 10 * * ? *) | justhodl-cq-feed |  |  | 7 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(50 11 * * ? *) | justhodl-symbol-resolver |  |  | 8 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 11 * * ? *) | justhodl-te-feed |  |  | 8 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(50 11 * * ? *) | justhodl-gov-sources |  |  | 9 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 23 ? * MON-FRI *) | justhodl-desk-returns |  |  | 10 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 11 ? * TUE-SAT *) | justhodl-ofr-stfm |  |  | 10 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(20 12 * * ? *) | justhodl-source-map |  |  | 11 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(15 11 * * ? *) | justhodl-families-feed |  |  | 12 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(55 12 * * ? *) | justhodl-tv-workbench |  |  | 12 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(15 21 ? * MON-FRI *) | justhodl-spx-ma |  |  | 13 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(10 9 * * ? *) | justhodl-calibration-fleet |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 14 * * ? *) | justhodl-catalyst-classifier |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(15 14 * * ? *) | justhodl-catalyst-clusters |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 23 * * ? *) | justhodl-chart-patterns |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(0 21 * * ? *) | justhodl-credit-equity-divergence |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  |  |  | cron(30 0 * * ? *) | justhodl-desk-allocator |  |  | 14 |  |  |  | D5_broken_wire |  |  |  |
|  | 100.0 | 12 |  | justhodl-ici-flows |  | 12 |  |  |  |  | D8_errors | 0 |  |  |
|  | 100.0 | 84 |  | justhodl-search-attention |  | 84 |  |  |  |  | D8_errors | 0 |  |  |
|  | 100.0 | 57 |  | ultimate-multi-agent |  | 57 |  |  |  |  | D8_errors | 0 |  |  |
|  | 84.5 | 289 |  | fedliquidityapi |  | 342 |  |  |  |  | D8_errors | 5 |  |  |
|  | 83.7 | 211 |  | manufacturing-global-agent |  | 252 |  |  |  |  | D8_errors | 27 |  |  |
|  | 83.1 | 212 |  | securities-banking-agent |  | 255 |  |  |  |  | D8_errors | 24 |  |  |
|  | 81.3 | 209 |  | dollar-strength-agent |  | 257 |  |  |  |  | D8_errors | 22 |  |  |
|  | 75.0 | 6 |  | justhodl-bagger-engine |  | 8 |  |  |  |  | D8_errors | 0 |  |  |
|  | 71.2 | 222 |  | justhodl-deal-scanner |  | 312 |  |  |  |  | D8_errors | 0 |  |  |
|  | 70.0 | 42 |  | justhodl-signal-scorecard |  | 60 |  |  |  |  | D8_errors | 14 |  |  |
|  | 68.8 | 176 |  | volatility-monitor-agent |  | 256 |  |  |  |  | D8_errors | 23 |  |  |
|  | 55.3 | 21 |  | justhodl-data-census |  | 38 |  |  |  |  | D8_errors | 0 |  |  |
|  | 42.6 | 20 |  | justhodl-cb-injection |  | 47 |  |  |  |  | D8_errors | 0 |  |  |
|  | 36.4 | 8 |  | justhodl-ka-metrics |  | 22 |  |  |  |  | D8_errors | 0 |  |  |
|  | 36.4 | 8 |  | justhodl-khalid-metrics |  | 22 |  |  |  |  | D8_errors | 0 |  |  |
|  | 33.3 | 13 |  | justhodl-construction-housing |  | 39 |  |  |  |  | D8_errors | 0 |  |  |
|  | 32.5 | 13 |  | justhodl-consumer-pulse |  | 40 |  |  |  |  | D8_errors | 0 |  |  |
|  | 32.5 | 13 |  | justhodl-yen-carry |  | 40 |  |  |  |  | D8_errors | 0 |  |  |
|  | 31.5 | 17 |  | justhodl-fx-intelligence |  | 54 |  |  |  |  | D8_errors | 0 |  |  |
|  | 28.9 | 11 |  | justhodl-boj-detail |  | 38 |  |  |  |  | D8_errors | 0 |  |  |
|  | 28.9 | 11 |  | justhodl-cds-proxy |  | 38 |  |  |  |  | D8_errors | 0 |  |  |
|  | 28.9 | 11 |  | justhodl-snb-detail |  | 38 |  |  |  |  | D8_errors | 0 |  |  |
|  | 28.2 | 11 |  | justhodl-gdelt-buzz |  | 39 |  |  |  |  | D8_errors | 0 |  |  |
|  | 26.8 | 11 |  | justhodl-air-cargo |  | 41 |  |  |  |  | D8_errors | 0 |  |  |
|  | 25.7 | 9 |  | justhodl-ecb-detail |  | 35 |  |  |  |  | D8_errors | 0 |  |  |
|  | 25.4 | 30 |  | justhodl-tradingview |  | 118 |  |  |  |  | D8_errors | 0 |  |  |
|  | 20.0 | 1 |  | justhodl-jsi-calibrator |  | 5 |  |  |  |  | D8_errors | 0 |  |  |
|  | 4.8 | 3 |  | justhodl-outcome-checker |  | 62 |  |  |  |  | D8_errors | 118 |  |  |
|  | 2.0 | 6 |  | bls-labor-agent |  | 300 |  |  |  |  | D8_errors | 25 |  |  |
|  | 1.3 | 4 |  | bond-indices-agent |  | 303 |  |  |  |  | D8_errors | 22 |  |  |

## Log
## 0. Inventory

- `14:23:39` functions: 765   $LATEST code total: 0.03 GB
- `14:23:39` ACCOUNT CODE STORAGE: 0.11 GB / 322.12 GB  (0.0%)
- `14:23:39` ✅ D11 code storage healthy (0.0%)
## 1. Fleet metrics (14d)

- `14:24:16` ✅ metrics collected for 765 functions
## D1. Self-invocation / recursion chains

- `14:24:16` functions that invoke THEMSELVES in source: 3
- `14:24:16` ✗    justhodl-13f-clone-alpha                     UNGUARDED — recursion risk
- `14:24:16` ✗    justhodl-equity-research                     UNGUARDED — recursion risk
- `14:24:16` ✅    justhodl-fundamental-census                  GUARDED
- `14:24:16` functions AWS has ever loop-broken: justhodl-fundamental-census
## D2. Timeout-clipped engines (silent truncation)

- `14:24:16` engines whose max duration pins the timeout ceiling: 42
- `14:24:16` ✗    justhodl-market-tape                       timeout=  30s max=  30.0s avg=   4.4s inv=4173
- `14:24:16` ✗    fedliquidityapi                            timeout=  30s max=  30.0s avg=  26.6s inv=342
- `14:24:16` ✗    xccy-basis-agent                           timeout= 300s max= 300.0s avg=  50.0s inv=314
- `14:24:16` ✗    enhanced-repo-agent                        timeout=  60s max=  60.0s avg=   2.6s inv=308
- `14:24:16` ✗    bond-indices-agent                         timeout= 300s max= 300.0s avg=  92.5s inv=303
- `14:24:16` ✗    bls-labor-agent                            timeout= 180s max= 180.0s avg=  39.4s inv=300
- `14:24:16` ✗    dollar-strength-agent                      timeout=  60s max=  60.0s avg=  51.5s inv=257
- `14:24:16` ✗    volatility-monitor-agent                   timeout=  60s max=  60.0s avg=  49.6s inv=256
- `14:24:16` ✗    securities-banking-agent                   timeout=  60s max=  60.0s avg=  51.8s inv=255
- `14:24:16` ✗    manufacturing-global-agent                 timeout=  60s max=  60.0s avg=  53.7s inv=252
- `14:24:16` ✗    justhodl-tradingview                       timeout= 900s max= 900.0s avg= 495.3s inv=118
- `14:24:16` ✗    justhodl-fundamental-census                timeout= 900s max= 900.0s avg= 119.5s inv=117
- `14:24:16` ✗    justhodl-search-attention                  timeout= 300s max= 300.0s avg= 300.0s inv=84
- `14:24:16` ✗    justhodl-outcome-checker                   timeout= 300s max= 300.0s avg=  96.1s inv=62
- `14:24:16` ✗    justhodl-signal-scorecard                  timeout= 120s max= 120.0s avg= 111.1s inv=60
- `14:24:16` ✗    justhodl-china-liquidity                   timeout= 300s max= 300.0s avg=  64.8s inv=55
- `14:24:16` ✗    justhodl-fx-intelligence                   timeout= 120s max= 120.0s avg=  75.9s inv=54
- `14:24:16` ✗    justhodl-cb-injection                      timeout=  90s max=  90.0s avg=  59.3s inv=47
- `14:24:16` ✗    justhodl-air-cargo                         timeout= 180s max= 300.0s avg=  65.3s inv=41
- `14:24:16` ✗    justhodl-global-liquidity                  timeout= 120s max= 120.0s avg=  84.4s inv=41
- `14:24:16` ✗    justhodl-consumer-pulse                    timeout=  90s max=  90.0s avg=  41.2s inv=40
- `14:24:16` ✗    justhodl-yen-carry                         timeout=  90s max=  90.0s avg=  62.0s inv=40
- `14:24:16` ✗    justhodl-construction-housing              timeout= 120s max= 120.0s avg= 103.2s inv=39
- `14:24:16` ✗    justhodl-gdelt-buzz                        timeout= 180s max= 180.0s avg= 173.1s inv=39
- `14:24:16` ✗    justhodl-boj-detail                        timeout=  90s max=  90.0s avg=  39.6s inv=38
- `14:24:16` ✗    justhodl-cds-proxy                         timeout= 120s max= 120.0s avg=  59.4s inv=38
- `14:24:16` ✗    justhodl-data-census                       timeout= 900s max= 900.0s avg= 544.6s inv=38
- `14:24:16` ✗    justhodl-snb-detail                        timeout=  90s max=  90.0s avg=  40.5s inv=38
- `14:24:16` ✗    justhodl-activity-nowcast                  timeout=  90s max=  90.0s avg=  40.8s inv=36
- `14:24:16` ✗    justhodl-ecb-detail                        timeout=  90s max=  90.0s avg=  60.8s inv=35
## D3/D4/D5/D6. Schedule wiring integrity

- `14:24:42` D3 duplicate targets remaining: 3
- `14:24:42` ✗    justhodl-alpha-daily-brief -> justhodl-alpha-daily-brief
- `14:24:42` ✗    justhodl-options-confluence-hourly -> justhodl-options-confluence
- `14:24:42` ✗    premortem-engine-daily -> justhodl-premortem-engine
- `14:24:42` D4 orphan schedules (target function does not exist): 0
- `14:24:42` 
- `14:24:42` D5 broken wires — schedule exists, invoke permission missing
- `14:25:21`    functions scheduled WITHOUT an events/scheduler invoke permission: 126
- `14:25:21` ⚠    justhodl-warroom-weights                   inv14d=1       cron(10 7 1 * ? *)
- `14:25:21` ⚠    justhodl-13f-clone-alpha                   inv14d=2       cron(30 8 ? * MON *)
- `14:25:21` ⚠    justhodl-gsi-horizons                      inv14d=2       cron(30 9 ? * SUN *)
- `14:25:21` ⚠    justhodl-ma-target-predictor               inv14d=2       cron(0 14 ? * SUN *)
- `14:25:21` ⚠    justhodl-playbook-engine                   inv14d=2       cron(0 7 ? * MON *)
- `14:25:21` ⚠    justhodl-signal-orthogonality              inv14d=2       cron(45 9 ? * SUN *)
- `14:25:21` ⚠    justhodl-spx-history                       inv14d=2       cron(10 8 ? * SUN *)
- `14:25:21` ⚠    justhodl-whales                            inv14d=2       cron(10 13 ? * MON *)
- `14:25:21` ⚠    justhodl-fi-census                         inv14d=3       cron(0 7 2,16 * ? *)
- `14:25:21` ⚠    justhodl-indicator-bus                     inv14d=3       cron(15 12 * * ? *)
- `14:25:21` ⚠    justhodl-etf-census                        inv14d=4       cron(30 6 2,16 * ? *)
- `14:25:21` ⚠    justhodl-macro-attribution                 inv14d=5       cron(5 12 * * ? *)
- `14:25:21` ⚠    justhodl-cot-feed                          inv14d=6       cron(45 11 * * ? *)
- `14:25:21` ⚠    justhodl-equity-ftd                        inv14d=6       cron(10 15 ? * MON,THU *)
- `14:25:21` ⚠    justhodl-cq-feed                           inv14d=7       cron(50 10 * * ? *)
- `14:25:21` ⚠    justhodl-symbol-resolver                   inv14d=8       cron(50 11 * * ? *)
- `14:25:21` ⚠    justhodl-te-feed                           inv14d=8       cron(0 11 * * ? *)
- `14:25:21` ⚠    justhodl-gov-sources                       inv14d=9       cron(50 11 * * ? *)
- `14:25:21` ⚠    justhodl-desk-returns                      inv14d=10      cron(30 23 ? * MON-FRI *)
- `14:25:21` ⚠    justhodl-ofr-stfm                          inv14d=10      cron(30 11 ? * TUE-SAT *)
- `14:25:21` ⚠    justhodl-source-map                        inv14d=11      cron(20 12 * * ? *)
- `14:25:21` ⚠    justhodl-families-feed                     inv14d=12      cron(15 11 * * ? *)
- `14:25:21` ⚠    justhodl-tv-workbench                      inv14d=12      cron(55 12 * * ? *)
- `14:25:21` ⚠    justhodl-spx-ma                            inv14d=13      cron(15 21 ? * MON-FRI *)
- `14:25:21` ⚠    justhodl-calibration-fleet                 inv14d=14      cron(10 9 * * ? *)
- `14:25:21` ⚠    justhodl-catalyst-classifier               inv14d=14      cron(0 14 * * ? *)
- `14:25:21` ⚠    justhodl-catalyst-clusters                 inv14d=14      cron(15 14 * * ? *)
- `14:25:21` ⚠    justhodl-chart-patterns                    inv14d=14      cron(30 23 * * ? *)
- `14:25:21` ⚠    justhodl-credit-equity-divergence          inv14d=14      cron(0 21 * * ? *)
- `14:25:21` ⚠    justhodl-desk-allocator                    inv14d=14      cron(30 0 * * ? *)
- `14:25:21` 
- `14:25:21` D6 scheduled but ZERO invocations in 14d
- `14:25:21`    count: 0
## D7. Reserved concurrency = 0 (hard-disabled functions)

- `14:25:22` ⚠    justhodl-13f-positions                       reserved=1 (throttle risk)
- `14:25:22` ⚠    justhodl-aaii-sentiment                      reserved=1 (throttle risk)
- `14:25:22` ⚠    justhodl-ai-chat                             reserved=3 (throttle risk)
- `14:25:23` ⚠    justhodl-alert-backtester                    reserved=1 (throttle risk)
- `14:25:23` ⚠    justhodl-apex-fusion                         reserved=1 (throttle risk)
- `14:25:23` ⚠    justhodl-api-keys-admin                      reserved=2 (throttle risk)
- `14:25:24` ⚠    justhodl-backtest-harness                    reserved=1 (throttle risk)
- `14:25:26` ⚠    justhodl-catalyst-calendar                   reserved=1 (throttle risk)
- `14:25:29` ⚠    justhodl-daily-report-v3                     reserved=1 (throttle risk)
- `14:25:30` ⚠    justhodl-divergence-interpreter              reserved=1 (throttle risk)
- `14:25:30` ⚠    justhodl-earnings-whisper                    reserved=1 (throttle risk)
- `14:25:32` ⚠    justhodl-exchange-flows                      reserved=1 (throttle risk)
- `14:25:32` ⚠    justhodl-fed-speak                           reserved=1 (throttle risk)
- `14:25:34` ⚠    justhodl-gdelt-sentiment                     reserved=1 (throttle risk)
- `14:25:34` ⚠    justhodl-global-macro                        reserved=1 (throttle risk)
- `14:25:34` ⚠    justhodl-global-tide                         reserved=1 (throttle risk)
- `14:25:35` ⚠    justhodl-historical-analogs                  reserved=1 (throttle risk)
- `14:25:35` ⚠    justhodl-implied-prob                        reserved=1 (throttle risk)
- `14:25:35` ⚠    justhodl-insider-trades                      reserved=1 (throttle risk)
- `14:25:36` ⚠    justhodl-kill-switch                         reserved=1 (throttle risk)
- `14:25:36` ⚠    justhodl-labor-leading                       reserved=1 (throttle risk)
- `14:25:36` ⚠    justhodl-liquidity-flow                      reserved=1 (throttle risk)
- `14:25:37` ⚠    justhodl-master-ranker                       reserved=1 (throttle risk)
- `14:25:39` ⚠    justhodl-nyfed-dealer-survey                 reserved=1 (throttle risk)
- `14:25:39` ⚠    justhodl-oecd-cli                            reserved=1 (throttle risk)
- `14:25:39` ⚠    justhodl-onchain-ratios                      reserved=1 (throttle risk)
- `14:25:39` ⚠    justhodl-options-gamma                       reserved=1 (throttle risk)
- `14:25:39` ⚠    justhodl-outcome-checker                     reserved=1 (throttle risk)
- `14:25:40` ⚠    justhodl-plumbing-aggregator                 reserved=1 (throttle risk)
- `14:25:41` ⚠    justhodl-price-redundancy                    reserved=1 (throttle risk)
- `14:25:42` ⚠    justhodl-redflag-alerter                     reserved=1 (throttle risk)
- `14:25:44` ⚠    justhodl-sec-10kq                            reserved=1 (throttle risk)
- `14:25:44` ⚠    justhodl-sec-13f                             reserved=1 (throttle risk)
- `14:25:44` ⚠    justhodl-sec-8k                              reserved=1 (throttle risk)
- `14:25:45` ⚠    justhodl-signal-scorecard                    reserved=1 (throttle risk)
- `14:25:45` ⚠    justhodl-smart-wake                          reserved=1 (throttle risk)
- `14:25:47` ⚠    justhodl-tape-reader                         reserved=1 (throttle risk)
- `14:25:48` ⚠    justhodl-trade-journal                       reserved=1 (throttle risk)
- `14:25:49` ⚠    justhodl-vix-curve                           reserved=1 (throttle risk)
- `14:25:49` ⚠    justhodl-vol-regime                          reserved=1 (throttle risk)
- `14:25:49` ⚠    justhodl-watchlist                           reserved=1 (throttle risk)
- `14:25:51` ✅    none hard-disabled
## D8. Error-rate and throttle outliers

- `14:25:51` functions with >=20% error rate or any throttling: 47
- `14:25:51` ✗    justhodl-ici-flows                         err=100.0% (12/12) throttles=0
- `14:25:51` ✗    justhodl-search-attention                  err=100.0% (84/84) throttles=0
- `14:25:51` ✗    ultimate-multi-agent                       err=100.0% (57/57) throttles=0
- `14:25:51` ✗    fedliquidityapi                            err= 84.5% (289/342) throttles=5
- `14:25:51` ✗    manufacturing-global-agent                 err= 83.7% (211/252) throttles=27
- `14:25:51` ✗    securities-banking-agent                   err= 83.1% (212/255) throttles=24
- `14:25:51` ✗    dollar-strength-agent                      err= 81.3% (209/257) throttles=22
- `14:25:51` ✗    justhodl-bagger-engine                     err= 75.0% (6/8) throttles=0
- `14:25:51` ✗    justhodl-deal-scanner                      err= 71.2% (222/312) throttles=0
- `14:25:51` ✗    justhodl-signal-scorecard                  err= 70.0% (42/60) throttles=14
- `14:25:51` ✗    volatility-monitor-agent                   err= 68.8% (176/256) throttles=23
- `14:25:51` ✗    justhodl-data-census                       err= 55.3% (21/38) throttles=0
- `14:25:51` ✗    justhodl-cb-injection                      err= 42.6% (20/47) throttles=0
- `14:25:51` ✗    justhodl-ka-metrics                        err= 36.4% (8/22) throttles=0
- `14:25:51` ✗    justhodl-khalid-metrics                    err= 36.4% (8/22) throttles=0
- `14:25:51` ✗    justhodl-construction-housing              err= 33.3% (13/39) throttles=0
- `14:25:51` ✗    justhodl-consumer-pulse                    err= 32.5% (13/40) throttles=0
- `14:25:51` ✗    justhodl-yen-carry                         err= 32.5% (13/40) throttles=0
- `14:25:51` ✗    justhodl-fx-intelligence                   err= 31.5% (17/54) throttles=0
- `14:25:51` ✗    justhodl-boj-detail                        err= 28.9% (11/38) throttles=0
- `14:25:51` ✗    justhodl-cds-proxy                         err= 28.9% (11/38) throttles=0
- `14:25:51` ✗    justhodl-snb-detail                        err= 28.9% (11/38) throttles=0
- `14:25:51` ✗    justhodl-gdelt-buzz                        err= 28.2% (11/39) throttles=0
- `14:25:51` ✗    justhodl-air-cargo                         err= 26.8% (11/41) throttles=0
- `14:25:51` ✗    justhodl-ecb-detail                        err= 25.7% (9/35) throttles=0
- `14:25:51` ✗    justhodl-tradingview                       err= 25.4% (30/118) throttles=0
- `14:25:51` ✗    justhodl-jsi-calibrator                    err= 20.0% (1/5) throttles=0
- `14:25:51` ✗    justhodl-outcome-checker                   err=  4.8% (3/62) throttles=118
- `14:25:51` ✗    bls-labor-agent                            err=  2.0% (6/300) throttles=25
- `14:25:51` ✗    bond-indices-agent                         err=  1.3% (4/303) throttles=22
## D9. Async failures with nowhere to land

- `14:25:52` scheduled functions WITH errors and NO DLQ/on-failure destination: 0
## D10. Deprecated runtimes

- `14:25:52` functions on deprecated runtimes: 0
## D12. Env vars naming resources that no longer exist

- `14:25:52` ✅    none — env surface clean
## D13. Dead functions (no schedule, no invocations)

- `14:25:52` functions with no schedule AND zero invocations in 14d: 33 of 765 (4% of the fleet)
- `14:25:52`    autonomous-ai-processor
- `14:25:52`    ecb
- `14:25:52`    economyapi
- `14:25:52`    fmp-fundamentals-agent
- `14:25:52`    justhodl-api-keys-admin
- `14:25:52`    justhodl-ask
- `14:25:52`    justhodl-ask-desk
- `14:25:52`    justhodl-charts-agent
- `14:25:52`    justhodl-chat-api
- `14:25:52`    justhodl-daily-macro-report
- `14:25:52`    justhodl-ecb-proxy
- `14:25:52`    justhodl-email-reports
- `14:25:52`    justhodl-feedback
- `14:25:52`    justhodl-fred-proxy
- `14:25:52`    justhodl-history-api
- `14:25:52`    justhodl-investor-agents
- `14:25:52`    justhodl-portfolio-admin
- `14:25:52`    justhodl-public-api-demo
- `14:25:52`    justhodl-push-api
- `14:25:52`    justhodl-stock-ai-research
- `14:25:52`    justhodl-stock-analyzer
- `14:25:52`    justhodl-subscribe
- `14:25:52`    justhodl-trade-journal
- `14:25:52`    justhodl-transcript-query
- `14:25:52`    justhodl-treasury-proxy
- `14:25:52`    justhodl-watchlist
- `14:25:52`    macro-report-api
- `14:25:52`    multi-agent-orchestrator
- `14:25:52`    nasdaq-datalink-agent
- `14:25:52`    nyfed-financial-stability-fetcher
- `14:25:52`    nyfed-primary-dealer-fetcher
- `14:25:52`    nyfedapi-isolated
- `14:25:52`    openbb-system2-api
## SUMMARY — defect ledger

- `14:25:52`    D5_broken_wire             30
- `14:25:52`    D2_timeout_clipped         30
- `14:25:52`    D1_recursion               2
- `14:25:52`    D8_errors                  30
- `14:25:52`    D3_double_fire             3
- `14:25:52`    D13_dead                   33
- `14:25:52` ✅ wrote 4233_fleet_integrity_audit.json

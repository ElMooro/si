# ops 6874 - the slow-source engines today, and what is stale right now (read-only)

**Status:** success  
**Duration:** 270.6s  
**Finished:** 2026-11-21T05:08:21+00:00  

## Data

| feeds_past_limit | freshness_status | per_file_limits | slow_engines | slow_lambda_month | slow_writes_month |
|---|---|---|---|---|---|
| 667 | DEGRADED | 50 | 41 | 19.49 | 2.89 |

## Log
## A. Engines on slow official sources

- `05:05:37` 2026-11-20: about 259046 writes to the live bucket by all engines
- `05:07:09` 2026-11-19: about 509525 writes to the live bucket by all engines
- `05:08:20` 2026-11-18: about 249868 writes to the live bucket by all engines
- `05:08:20` 41 functions match; runs and run time are the last 7 days, files written are 2026-11-20, 2026-11-19, 2026-11-18
- `05:08:20` justhodl-worldbank-full                  runs 239 x   677 s | Lambda $11.55/mo | files/day   2365  21615    596 ($2.46/mo) | sched justhodl-worldbank-full-weekly [rate(7 days)]; sched justhodl-worldbank-full-2h [daily 03:20] | World Bank: annual and quarterly series, revised a few times a year
- `05:08:20` justhodl-eia-bank                        runs  85 x   771 s | Lambda $4.68/mo | files/day    205   1782    937 ($0.29/mo) | sched justhodl-eia-bank-daily [daily 21:31] | EIA: weekly petroleum Wednesday, daily prices
- `05:08:20` justhodl-data-census                     runs   7 x   756 s | Lambda $1.11/mo | files/day      0      0      0 ($0.00/mo) | sched data-census-daily [daily 12:45] | Census: monthly releases
- `05:08:20` justhodl-fundamental-census              runs   8 x   700 s | Lambda $0.60/mo | files/day      0      0      0 ($0.00/mo) | sched fundamental-census-sched [monthly day 1,15 06:00] | Census: monthly releases
- `05:08:20` justhodl-sdmx-walker                     runs  57 x     8 s | Lambda $0.33/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-sdmx-walker-hourly [daily 13:40]; sched justhodl-statcan-retry-hourly [rate(24 hours)]; sched justhodl-ecb-rewalk-weekly [weekly SUN 03:15]; sched justhodl-oecd-retry-hourly [daily 10:53] | BIS / Eurostat / OECD dataset walks: monthly and quarterly data
- `05:08:20` justhodl-census-us                       runs  50 x    63 s | Lambda $0.23/mo | files/day      0    222    255 ($0.05/mo) | sched justhodl-census-us-15min [daily 07:11]; sched justhodl-census-econ-dispatch [daily 05:10] (+1 disabled) | Census: monthly releases
- `05:08:20` justhodl-13f-positions                   runs  15 x   216 s | Lambda $0.23/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-13f-positions-sched [daily 16:21]; sched justhodl-holdings-originals-research [daily 15:40] (+1 disabled) | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-imf-full                        runs   3 x   850 s | Lambda $0.18/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-imf-full-weekly [rate(7 days)] (+1 disabled) | IMF: monthly and quarterly
- `05:08:20` justhodl-boj-full                        runs 168 x     2 s | Lambda $0.04/mo | files/day    411    445      0 ($0.09/mo) | sched justhodl-boj-full-fanout [daily 06:31]; sched justhodl-boj-full-12h [rate(24 hours)] | Bank of Japan: daily and ten-day accounts
- `05:08:20` justhodl-tic-full                        runs   7 x   154 s | Lambda $0.12/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-tic-full-12h [rate(24 hours)] | Treasury TIC: monthly
- `05:08:20` cftc-futures-positioning-agent           runs  53 x    49 s | Lambda $0.09/mo | files/day      0      0      0 ($0.00/mo) | rule cftc-cot-weekly-update [daily 06:48] | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` justhodl-13f-book                        runs  25 x    20 s | Lambda $0.07/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-13f-book-daily [daily 16:37] | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-fdic-bankfind                   runs   7 x    88 s | Lambda $0.04/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-fdic-bankfind-daily [daily 03:10] | FDIC call reports: quarterly
- `05:08:20` justhodl-cot-feed                        runs   7 x   157 s | Lambda $0.04/mo | files/day      0      0      0 ($0.00/mo) | sched cot-feed-daily [daily 11:45] | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` justhodl-eurostat-oecd                   runs   7 x    42 s | Lambda $0.03/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-eurostat-oecd-weekly [daily 06:41] | OECD: monthly
- `05:08:20` justhodl-bis-gleif                       runs   7 x    27 s | Lambda $0.03/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-bis-gleif-weekly [daily 07:59] | BIS: quarterly, some monthly
- `05:08:20` justhodl-cftc-full-datasets              runs   7 x    47 s | Lambda $0.02/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-cftc-full-daily [daily 06:47] | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` bls-labor-agent                          runs  48 x     8 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | rule bls-labor-agent-daily [daily 14:00] | BLS: monthly releases 08:30 ET
- `05:08:20` justhodl-eba-risk-dashboard              runs   7 x    21 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-eba-risk-dashboard-daily [daily 03:40] | EBA risk dashboard: quarterly
- `05:08:20` bea-economic-agent                       runs  64 x     1 s | Lambda $0.00/mo | files/day      0      0     85 ($0.01/mo) | rule bea-economic-agent-daily [daily 14:15] | BEA: monthly and quarterly releases 08:30 ET
- `05:08:20` census-economic-agent                    runs  48 x     6 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | rule census-economic-agent-daily [daily 14:30] | Census: monthly releases
- `05:08:20` justhodl-etf-census                      runs   1 x   131 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | sched etf-census-sched [monthly day 2,16 06:30] | Census: monthly releases
- `05:08:20` eia-energy-agent                         runs   7 x    34 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | rule eia-energy-agent-daily [daily 11:40] | EIA: weekly petroleum Wednesday, daily prices
- `05:08:20` justhodl-dol-full                        runs   7 x    26 s | Lambda $0.01/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-dol-full-6h [rate(24 hours)] | DOL claims: weekly, Thursday 08:30 ET
- `05:08:20` justhodl-13f-clone-alpha                 runs   2 x    15 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-13f-clone-alpha-weekly [weekly MON 08:30] | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-worldbank-wgi                   runs   1 x    49 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-worldbank-wgi-daily [weekly MON 03:05] | World Bank: annual and quarterly series, revised a few times a year
- `05:08:20` justhodl-tic-flows                       runs   7 x    11 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-tic-flows-sched [daily 21:45] (+1 disabled) | Treasury TIC: monthly
- `05:08:20` justhodl-fi-census                       runs   1 x    29 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched fi-census-sched [monthly day 2,16 07:00] | Census: monthly releases
- `05:08:20` justhodl-crypto-cot                      runs   7 x    11 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched crypto-cot-sched [daily 21:00] | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` bls-employment-api-v2                    runs   7 x     4 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | tick daily-eve | BLS: monthly releases 08:30 ET
- `05:08:20` justhodl-cot-extremes-scanner            runs   1 x    29 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-cot-extremes-scanner-weekly [weekly FRI 19:00] | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` justhodl-sec-13f                         runs   7 x     1 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-sec-13f-daily [daily 03:31] | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-boj-detail                      runs   7 x     3 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule boj-detail-daily [daily 11:20] | Bank of Japan: daily and ten-day accounts
- `05:08:20` justhodl-bls-full                        runs   7 x     0 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-bls-full-12h [rate(24 hours)] | BLS: monthly releases 08:30 ET
- `05:08:20` justhodl-oecd-cli                        runs   7 x     0 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-oecd-cli-weekly [daily 15:36] | OECD: monthly
- `05:08:20` justhodl-13f-price-divergence            runs   5 x     0 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-13f-price-divergence-sched [MON-FRI 21:20] (+1 disabled) | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-cot-tracker                     runs   1 x     2 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-cot-tracker-weekly [weekly FRI 22:01] (+1 disabled) | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` justhodl-cftc-deep-view                  runs   2 x     0 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule justhodl-cftc-deep-view-sched [FRI,SAT 21:00] (+1 disabled) | CFTC commitments of traders: weekly, Friday 15:30 ET
- `05:08:20` justhodl-bis-crossborder                 runs   1 x     1 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | sched justhodl-bis-crossborder-weekly [weekly MON 10:31] | BIS: quarterly, some monthly
- `05:08:20` justhodl-smart-money-13f                 runs   1 x     1 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | rule smart-money-13f-weekly [weekly MON 11:03] | SEC 13F: quarterly, filed over the 45 days after quarter end
- `05:08:20` justhodl-eurostat-history                runs   0 x     0 s | Lambda $0.00/mo | files/day      0      0      0 ($0.00/mo) | no enabled trigger | Eurostat: monthly, some daily
- `05:08:20` together: Lambda $19.49 a month, file writes $2.89 a month
## B. The freshness monitor's last report

- `05:08:20` report of 2026-11-20T07:59:11.605314+00:00, status DEGRADED, version 3.0.0
- `05:08:20` counts: {"schema_version": "fleet-freshness-monitor.v3", "n_keys_tracked": 1554, "n_stale": 667, "n_fresh": 887, "n_unknown": 305, "n_invalid_or_empty": 2, "n_source_stale": 326, "n_missing": 34, "n_alerts_raised": 667, "n_alerts_suppressed": 0, "elapsed_s": 109.63, "full_expected_coverage": false, "telegram_sent": false, "sns_sent": true, "notifications_suppressed": false, "semantics": "Artifact age measures S3 LastModified; source age measures the parsed source timestamp. FRESH requires both within threshold. UNKNOWN is unresolved content or time provenance. Results include every evaluated public output; coverage separately records enumeration limits, unresolved families and private exclusions."}
- `05:08:20` past their limit: 667 listed
- `05:08:20`   data/industry-case.json                                    INVALID  age 2260.03 h | limit 26.0 h | Content is not valid JSON.
- `05:08:20`   data/warm/worldbank-full/_state/state.json                 INVALID  age   0.03 h | limit 26.0 h | Content is not valid JSON.
- `05:08:20`   data/13f-clone-alpha.json                                  SOURCE_STALE age  95.44 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/13f-security-master.json                              SOURCE_STALE age  76.32 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/aaii-research-verification.json                       SOURCE_STALE age 1457.57 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/activist-13d.json                                     SOURCE_STALE age 2433.93 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/activist-filings-state.json                           SOURCE_STALE age 1292.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/ai-council.json                                       SOURCE_STALE age 2578.08 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/ai-factory.json                                       SOURCE_STALE age 819.83 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/alfred-vintages.json                                  SOURCE_STALE age 1642.31 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/alpha-atlas.json                                      SOURCE_STALE age 2626.78 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/alpha-research-verification.json                      SOURCE_STALE age 1486.18 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/alpha-triage.json                                     SOURCE_STALE age 2577.59 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/apex-fusion.json                                      SOURCE_STALE age   19.6 h | limit 5.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/asset-discovery.json                                  SOURCE_STALE age  42.44 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/asymmetric-scorer.json                                SOURCE_STALE age 2578.35 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/auction-tenor-signals.json                            SOURCE_STALE age 1499.43 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/backtest-summary.json                                 SOURCE_STALE age 3873.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/bagger-engine.json                                    SOURCE_STALE age 619.85 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/base-rates.json                                       SOURCE_STALE age 137.45 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/beaters-learned-weights.json                          SOURCE_STALE age 136.94 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/bis-crossborder.json                                  SOURCE_STALE age  93.43 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/bls-full-state.json                                   SOURCE_STALE age 1643.52 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/brain-compiler.json                                   SOURCE_STALE age  42.44 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/brief-fusion-coverage.json                            SOURCE_STALE age 1660.71 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/buyback-scanner.json                                  SOURCE_STALE age  91.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/calibration-history.json                              SOURCE_STALE age 118.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/calibration-latest.json                               SOURCE_STALE age 118.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/calibration-snapshot.json                             SOURCE_STALE age 2578.35 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/calls-integrity-status.json                           SOURCE_STALE age 1522.52 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/capital-consumer-verification.json                    SOURCE_STALE age 1466.58 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/capital-flow-history.json                             SOURCE_STALE age 1479.44 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/capital-flow-radar-state.json                         SOURCE_STALE age 1545.46 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/capital-research-verification.json                    SOURCE_STALE age 1467.99 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/capitulation-history.json                             SOURCE_STALE age 1456.2 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/carry-research-verification.json                      SOURCE_STALE age 1480.27 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/carry-surface.json                                    SOURCE_STALE age  498.6 h | limit 30.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/catalyst-clusters.json                                SOURCE_STALE age 1289.71 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/causality-discoveries.json                            SOURCE_STALE age 713.47 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/cb-research-verification.json                         SOURCE_STALE age 1487.72 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/census-sec-surface.json                               SOURCE_STALE age 1644.79 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/census-us-state.json                                  SOURCE_STALE age 1643.52 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/cftc-deep-view.json                                   SOURCE_STALE age 130.95 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/cftc-join.json                                        SOURCE_STALE age 1645.18 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20`   data/cftc-surface.json                                     SOURCE_STALE age 1645.92 h | limit 26.0 h | Source timestamp exceeds the freshness threshold.
- `05:08:20` sections: {"publication": 3, "results": 1554, "unknown": 305, "missing": 34, "invalid_or_empty": 2, "source_stale": 326, "stale": 667, "stale_top_50": 50, "source_stale_top_50": 50, "declared_absent_never_seen": 0, "coverage": 16, "manifest_rules": 1, "thresholds": 3, "summary_limits": 3}
## C. The freshness rules

- `05:08:20` rules: [{"prefix": "data/", "default_max_age_h": 26.0}]
- `05:08:20` left out: 16 prefixes ['data/_', 'data/_alerts/', 'data/_archive/', 'data/_skill/archive/', 'data/activity-nowcast/snapshots/', 'data/ai-commentary/history/', 'data/archive/', 'data/carry-surface/', 'data/crisis-knowledge-base', 'data/digest-trends-ai-history/', 'data/ecb-hist/', 'data/estimate-revisions/', 'data/regime-history/', 'data/track-record/']
- `05:08:20` per-file limits: 50 ({'<= 26 h': 18, '27-72 h': 24, '> 200 h': 6, '73-200 h (weekly)': 2})
- `05:08:20`   data/eba-risk-dashboard.json             limit (default)
- `05:08:20`   data/fdic-bankfind.json                  limit (default)
- `05:08:20`   data/worldbank-wgi.json                  limit (default)
- `05:08:20`   data/cftc-deep-view.json                 limit (default)
## D. The provider banks

- `05:08:20` justhodl-fmp-bank          phase BACKFILL  | updated 2026-11-20T12:02 | last run: calls 5, puts None, unchanged None, 1.2 s | objects 888, 11.3 GB | HOLD until 11-21 08:02: FMP 429 'Bandwidth Limit Reach' at 2026-11-20T12:02:55+00:00: the plan's 30-day bandwidth is spent; holding 20 h
- `05:08:20`       {"key_status": "ok", "runs": 63}
- `05:08:21` justhodl-fmp-walker        phase BACKFILL  | updated 2026-11-20T11:37 | last run: calls 333, puts 269, unchanged 49, 519.3 s | objects 55764, 10.6 GB | HOLD until 11-21 07:37: FMP 429 'Bandwidth Limit Reach' at 2026-11-20T11:37:46+00:00: the plan's 30-day bandwidth is spent; holding 20 h (schedules probe once a day)
- `05:08:21`       {"key_status": "ok", "runs": 57}
- `05:08:21` justhodl-polygon-bank      phase BACKFILL  | updated 2026-11-20T22:54 | last run: calls 2036, puts 785, unchanged 323, 760.8 s | objects 274993, 8.9 GB
- `05:08:21`       {"key_status": "ok", "runs": 303}
- `05:08:21` justhodl-provider-bank     phase STEADY    | updated 2026-11-20T21:58 | last run: calls None, puts 74, unchanged 3, 256.1 s | objects 2999, 0.3 GB
- `05:08:21`       {"runs": 176}
- `05:08:21` justhodl-eia-bank          phase BACKFILL  | updated 2026-11-20T21:44 | last run: calls 277, puts 30, unchanged 244, 777.8 s | objects 20941, 1.4 GB
- `05:08:21`       {"key_status": "ok", "runs": 285}
- `05:08:21` justhodl-polygon-eod       phase STEADY    | updated 2026-11-21T03:13 | last run: calls None, puts None, unchanged None, None s | objects 21183, 0.0 GB
- `05:08:21`       {"key_status": "ok", "runs": 35}
- `05:08:21` justhodl-bank-histories    phase STEADY    | updated 2026-11-21T05:00 | last run: calls None, puts 109043, unchanged 210051, 721.6 s | objects None, 0.0 GB
- `05:08:21` justhodl-bank-factors      phase None      | updated 2026-11-21T02:50 | last run: calls None, puts 27127, unchanged 0, 415.8 s | objects 27130, 0.0 GB
- `05:08:21` ✅ read-only; nothing changed

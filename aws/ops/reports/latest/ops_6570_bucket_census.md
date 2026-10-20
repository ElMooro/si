# ops 6570 - full census of the live bucket by prefix (read-only)

**Status:** success  
**Duration:** 2402.1s  
**Finished:** 2026-10-20T23:29:34+00:00  

## Data

| census | current_gb | current_objects | old_gb | old_versions | prefixes |
|---|---|---|---|---|---|
| s3://justhodl-ops-private-857687956942/census/20261020T224932Z.json | 433.74 | 1281312 | 1819.83 | 7840898 | 239 |

## Log
- `22:49:33` 239 prefixes to walk
- `23:29:33` bucket: 1281312 current objects = 433.74 GB; 7840898 old versions = 1819.83 GB; 82 delete markers; 9311 LIST calls; truncated prefixes: ['data/']
- `23:29:33` prefix | current objects | current GB | older than 7 d GB | older than 30 d GB | old versions | old GB | max versions on a key | keys > 10 versions
- `23:29:33` - WITHHELD | 497349 | 78.33 | 70.11 | 3.55 | 390499 | 2.51 | 195 | 3089
- `23:29:33` - data/holdings-research/ | 23274 | 74.82 | 66.6 | 3.6 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/index/ | 3071 | 60.9 | 60.9 | 60.9 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/option-flow-research/ | 94401 | 42.68 | 36.11 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/archive/ | 46551 | 40.98 | 40.92 | 40.46 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/bond-vol-research/ | 8181 | 24.57 | 21.96 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - index/ | 2581 | 15.18 | 15.18 | 15.18 | 60 | 0.79 | 56 | 1
- `23:29:33` - equity-research/ | 32301 | 10.41 | 9.77 | 3.96 | 43644 | 8.25 | 93 | 62
- `23:29:33` - data/crisis-research/ | 1806 | 8.93 | 8.18 | 0.19 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/etf-holdings-research/ | 107081 | 8.22 | 7.03 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/option-population-research/ | 36147 | 7.98 | 6.94 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/fundgraph/ | 37401 | 6.42 | 5.31 | 2.16 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/crypto-sentiment-research/ | 3385 | 5.08 | 4.27 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/etf-research/ | 1964 | 5.02 | 5.02 | 0.44 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/holdings-overlap/ | 3390 | 4.95 | 4.54 | 0.18 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/crypto-stablecoin-research/ | 3403 | 4.04 | 3.41 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/carry-research/ | 7724 | 3.23 | 2.83 | 0.14 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/offexchange-research/ | 2492 | 3.21 | 2.71 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - equity-research-history/ | 23157 | 2.66 | 2.51 | 1.24 | 26779 | 3.18 | 15 | 2
- `23:29:33` - data/bloomberg-archive/ | 960 | 2.66 | 2.51 | 0.85 | 0 | 0.0 | 0 | 0
- `23:29:33` - history/ | 6373 | 2.37 | 2.2 | 1.08 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/fifx-vol-research/ | 760 | 2.08 | 1.72 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/flow-desk-research/ | 1345 | 1.62 | 1.58 | 0.08 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/capital-research/ | 525 | 1.23 | 1.13 | 0.09 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/momentum-leaders/ | 7697 | 1.07 | 0.94 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/evidence/ | 171396 | 1.02 | 0.91 | 0.07 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/auction-desk-delivery/ | 575 | 0.92 | 0.72 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/daily-research/ | 123 | 0.91 | 0.82 | 0.08 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/liquidity-agent-research/ | 1570 | 0.88 | 0.79 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/microcap-float-squeeze/ | 2107 | 0.81 | 0.6 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - etf-flows/ | 539 | 0.61 | 0.61 | 0.61 | 15721 | 1.02 | 80 | 293
- `23:29:33` - data/ | 751 | 0.6 | 0.14 | 0.01 | 6657106 | 39.03 | 6657080 | 1
- `23:29:33` - data/inflection-research/ | 88 | 0.55 | 0.53 | 0.05 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/capital-structure-research/ | 1626 | 0.48 | 0.48 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/global-stress-research/ | 3641 | 0.47 | 0.43 | 0.02 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/official-research/ | 382 | 0.45 | 0.41 | 0.05 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/foreign-research/ | 492 | 0.44 | 0.26 | 0.18 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/earnings-pead/ | 18 | 0.38 | 0.34 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/etf-desk-research/ | 6134 | 0.37 | 0.33 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/funding-research/ | 1085 | 0.35 | 0.31 | 0.02 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/eps-revision-velocity/ | 1134 | 0.34 | 0.29 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/momentum-breakout/ | 990 | 0.32 | 0.32 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/lce-research/ | 72 | 0.28 | 0.25 | 0.05 | 0 | 0.0 | 0 | 0
- `23:29:33` - data/liquidity-flow-research/ | 177 | 0.28 | 0.23 | 0.0 | 0 | 0.0 | 0 | 0
- `23:29:33` - archive/ | 7283 | 0.27 | 0.27 | 0.2 | 36 | 0.0 | 1 | 0
- `23:29:33` prefixes with keys carrying more than 10 old versions: [('WITHHELD', 3089, [('f61583b5243452d35393f8dfeac5cf12f1c65b7ae7eb611314fefa7fc8b01cfa.json', 195), ('0793cbafce4611c3615d00ce625c79ece3e18638b8d0787dd3b1a2c9e75fa6a7.json', 125), ('09672fdebe26fab6d72221291a6abadedb57cc8e9320f7eb2511f0652f2d7a42.json', 125), ('1f85b72ad6fb7f14893e6e7ca912ec8ada5fd33472e142eb9327bef8817c9eb7.json', 125), ('2b50ca920788c034c992f0687257cf9b9602c4eced2112bd0b0d4b9e1e6a5718.json', 125)]), ('index/', 1, [('_build-cursor.json', 56)]), ('equity-research/', 62, [('NVDA.json', 93), ('ORCL.json', 85), ('AMZN.json', 84), ('GOOGL.json', 84), ('AAPL.json', 82)]), ('equity-research-history/', 2, [('NVDA.json', 15), ('AAOI.json', 14)]), ('etf-flows/', 293, [('constituent-pressure.json', 80), ('stock-exposure-lookup.json', 80), ('daily.json', 79), ('composite.json', 78), ('event-study.json', 78)]), ('data/', 1, [('miss-calibrator-proposals.json', 6657080)]), ('system-events/', 1, [('2026-10-12.jsonl', 573713)]), ('calibration/', 3, [('latest.json', 91), ('weights.json', 82), ('history-index.json', 15)]), ('screener/', 14, [('alpha-score.json', 1836), ('ma-latest.json', 776), ('data.json', 152), ('history.json', 152), ('earnings-sentiment-state.json', 150)]), ('data-census/', 1, [('paths-ledger.json', 83)]), ('analytics/', 6, [('backtest_results.json', 90), ('edgar_insiders_flat.json', 35), ('equity_research_flat.json', 35), ('etf_flows_flat.json', 35), ('manifest.json', 35)]), ('edgar-insiders/', 52, [('AAPL.json', 76), ('ABBV.json', 76), ('ABT.json', 76), ('ADBE.json', 76), ('AMD.json', 76)]), ('', 8, [('student-state.json', 46005), ('crypto-intel.json', 11143), ('repo-data.json', 3513), ('intelligence-report.json', 1797), ('flow-data.json', 292)]), ('data/ops/', 1, [('latest.json', 27)]), ('13f/', 2, [('clone-holdings-cache.json', 25), ('clone-price-cache.json', 25)]), ('domain-barometers/', 1, [('history-ledger.json', 44)]), ('etf-constituents-v2/', 44, [('JNK.json', 14), ('XLI.json', 14), ('DIA.json', 13), ('HYG.json', 13), ('IGV.json', 13)]), ('cot/', 36, [('current.json', 13), ('6B.json', 13), ('6C.json', 13), ('6E.json', 13), ('6J.json', 13)]), ('data/a2a/', 1, [('nudge-ledger.json', 9994)]), ('errors/', 1, [('recent.json', 2199)]), ('equity-prewarm/', 1, [('latest.json', 76)]), ('chokepoint/', 1, [('fundamentals-ledger.json', 149)]), ('spx-beaters/', 1, [('listings-history.json', 14)]), ('config/', 1, [('schedule-manifest.json', 15)]), ('geo/', 1, [('geopolitical-risk-history.json', 75)]), ('macro/', 1, [('regime.json', 78)]), ('flow-anomalies/', 2, [('alerts.json', 77), ('daily.json', 77)]), ('portfolio/', 11, [('risk.json', 2790), ('snapshot.json', 2781), ('sizing.json', 2699), ('sizing-alert-history.json', 1424), ('catalyst-alert-history.json', 150)]), ('spx-ma/', 1, [('member-closes.json', 56)]), ('symbol-resolver/', 1, [('ledger.json', 75)]), ('factory/', 2, [('board.json', 9338), ('scoreboard.json', 9338)]), ('_health/', 6, [('fleet.json', 670), ('cadence-manifest.json', 389), ('fleet_last_alert.json', 255), ('dashboard.json', 134), ('history.jsonl', 134)]), ('reports/', 1, [('scorecard.json', 131)]), ('sentiment/', 1, [('data.json', 158)]), ('backtest/', 3, [('results.json', 77), ('summary.json', 77), ('calls-results.json', 48)]), ('estimate-revisions/', 1, [('state.json', 143)]), ('macro-attribution/', 1, [('ledger.json', 76)]), ('credit/', 1, [('credit-before-equity-history.json', 77)]), ('boom/', 1, [('boom-stage-history.json', 75)]), ('readthrough/', 1, [('consensus-snapshots.json', 137)]), ('regime/', 2, [('current.json', 46), ('history.json', 46)]), ('signals/', 6, [('confluence-prev.json', 2640), ('confluence.json', 2640), ('regime-picks.json', 2640), ('alert-history.json', 1839), ('anomalies.json', 1771)]), ('risk/', 1, [('recommendations.json', 47)]), ('divergence/', 2, [('current.json', 45), ('history.json', 45)]), ('plumbing-composite/', 1, [('history.json', 53)]), ('air/', 1, [('hkia-cargo-levels.json', 82)]), ('state/', 1, [('universe-discovery-snapshot.json', 77)]), ('learning/', 2, [('last_log_run.json', 76), ('morning_run_log.json', 62)]), ('asia/', 2, [('kr-flash-tape.json', 77), ('tw-orders-levels.json', 77)]), ('data/_health/', 1, [('supabase-keepalive.json', 16)]), ('telegram/', 1, [('alert_state.json', 77)])]
- `23:29:33` bytes of today's current objects by the day they were written (last 14 d): 10-06 0.6 GB, 10-07 13.86 GB, 10-08 13.78 GB, 10-09 13.99 GB, 10-10 9.6 GB, 10-11 9.23 GB, 10-12 9.19 GB, 10-13 9.44 GB, 10-14 10.55 GB, 10-15 11.47 GB, 10-16 14.18 GB, 10-17 0.02 GB, 10-18 0.02 GB, 10-19 0.02 GB, 10-20 0.0 GB
- `23:29:33` largest objects: [('data/index/eurostat/t1/CENS_21COBHS_R3.jsonl', 0.94), ('index/ecb/t1/CSEC.jsonl', 0.07), ('data/finra-short-history.json', 0.07), ('WITHHELD', 0.07), ('data/eps-revision-velocity/history/09220262cc877ebd3a7d87bd942677ce82f0382ac74e80d997a83769df5699a2.json', 0.05), ('etf-flows/constituent-history/2026-09-19.json', 0.03), ('data/hiring-velocity/history/5711bb8f44c0341658e4a9d7b73ac51b1b1e59644b417f9beea8f42ce0b010e4.json', 0.03), ('system-events/audit/2026-10-13.jsonl', 0.03)]
- `23:29:34` ✅ census complete

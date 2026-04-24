# Prediction Loop Audit — every data point → prediction → outcome

**Status:** success  
**Duration:** 4.5s  
**Finished:** 2026-04-24T23:01:39+00:00  

## Data

| aws_total | dead_or_orphan | healthy_chains | logged_signals | logged_unweighted | relevant_in_repo | relevant_orphan | total_files | weighted_signals | weighted_unlogged |
|---|---|---|---|---|---|---|---|---|---|
|  | 2 | 10 |  |  |  |  | 19 |  |  |
| 95 |  |  |  |  | 11 | 4 |  |  |  |
|  |  |  | 13 | 8 |  |  |  | 12 | 7 |

## Log
## A. Producer → Consumer chains for each S3 data file

- `23:01:35`   ✓ data/report.json                       1.6m old,  1689KB
- `23:01:35`      ← writer: justhodl-telegram-bot, justhodl-morning-intelligence, justhodl-stock-analyzer
- `23:01:35`      → readers: justhodl-telegram-bot, justhodl-morning-intelligence, justhodl-stock-analyzer, justhodl-bloomberg-v8, justhodl-crypto-enricher
- `23:01:35`   ✓ data/secretary-latest.json            95.1m old,   137KB
- `23:01:35`      ← writer: justhodl-financial-secretary
- `23:01:35`      → readers: justhodl-financial-secretary
- `23:01:35`   ❌ data/intelligence-report.json       (not on S3)
- `23:01:35`   ✓ crypto-intel.json                      6.9m old,    54KB
- `23:01:35`      ← writer: justhodl-telegram-bot, justhodl-morning-intelligence, justhodl-financial-secretary
- `23:01:35`      → readers: justhodl-telegram-bot, justhodl-morning-intelligence, justhodl-financial-secretary, justhodl-signal-logger, justhodl-ai-chat
- `23:01:35`   ✓ flow-data.json                         3.7m old,    30KB
- `23:01:35`      ← writer: justhodl-morning-intelligence, justhodl-options-flow, justhodl-financial-secretary
- `23:01:35`      → readers: justhodl-morning-intelligence, justhodl-options-flow, justhodl-financial-secretary
- `23:01:35`   ✓ edge-data.json                        57.4m old,     1KB
- `23:01:35`      ← writer: justhodl-morning-intelligence, justhodl-edge-engine, justhodl-signal-logger
- `23:01:35`      → readers: justhodl-morning-intelligence, justhodl-edge-engine, justhodl-signal-logger
- `23:01:35`   ✓ predictions.json                    1806.1m old,    14KB
- `23:01:35`      ← writer: justhodl-morning-intelligence, justhodl-ml-predictions, justhodl-crypto-intel
- `23:01:35`      → readers: justhodl-morning-intelligence, justhodl-ml-predictions, justhodl-crypto-intel
- `23:01:35`   ✓ valuations-data.json                33660.8m old,     2KB
- `23:01:35`      ← writer: justhodl-morning-intelligence, justhodl-signal-logger, justhodl-valuations-agent
- `23:01:35`      → readers: justhodl-morning-intelligence, justhodl-signal-logger, justhodl-valuations-agent
- `23:01:35`   💀 DEAD DATA data.json                           94200.7m old,    59KB
- `23:01:35`      flags: no writer found in code, no consumer Lambda
- `23:01:35`   ❌ report.json                         (not on S3)
- `23:01:35`   ✓ screener/data.json                   211.0m old,   318KB
- `23:01:35`      ← writer: justhodl-morning-intelligence, justhodl-news-sentiment, justhodl-stock-screener
- `23:01:35`      → readers: justhodl-morning-intelligence, justhodl-news-sentiment, justhodl-stock-screener, justhodl-signal-logger
- `23:01:35`   ❌ screener/picks.json                 (not on S3)
- `23:01:35`   ❌ ml/predictions.json                 (not on S3)
- `23:01:36`   ✓ intelligence-report.json              55.9m old,     2KB
- `23:01:36`      ← writer: justhodl-intelligence, justhodl-morning-intelligence, justhodl-signal-logger
- `23:01:36`      → readers: justhodl-intelligence, justhodl-morning-intelligence, justhodl-signal-logger, justhodl-ai-chat
- `23:01:36`   ❌ ath-data.json                       (not on S3)
- `23:01:36`   ✓ repo-data.json                         0.6m old,    16KB
- `23:01:36`      ← writer: justhodl-morning-intelligence, justhodl-repo-monitor, justhodl-signal-logger
- `23:01:36`      → readers: justhodl-morning-intelligence, justhodl-repo-monitor, justhodl-signal-logger, justhodl-crypto-intel
- `23:01:36`   ❌ fund_flows.json                     (not on S3)
- `23:01:36`   💀 DEAD DATA stock-picks-data.json               77156.1m old,    95KB
- `23:01:36`      flags: no writer found in code, no consumer Lambda
- `23:01:36`   ❌ fed-liquidity.json                  (not on S3)
## B. Calibrator + ml-predictions: AWS-only Lambdas (not in repo)

- `23:01:37`   Total Lambdas in AWS: 95
- `23:01:37` 
  Prediction-loop Lambdas in AWS but NOT in repo:
- `23:01:37`     🔴 FinancialIntelligence-Backend
- `23:01:37`     🔴 MLPredictor
- `23:01:37`     🔴 justhodl-calibrator
- `23:01:37`     🔴 permanent-market-intelligence
- `23:01:37` 
  Prediction-loop Lambdas in AWS AND in repo:
- `23:01:37`     ✓ justhodl-crypto-intel
- `23:01:37`     ✓ justhodl-edge-engine
- `23:01:37`     ✓ justhodl-intelligence
- `23:01:37`     ✓ justhodl-khalid-metrics
- `23:01:37`     ✓ justhodl-ml-predictions
- `23:01:37`     ✓ justhodl-morning-intelligence
- `23:01:37`     ✓ justhodl-outcome-checker
- `23:01:37`     ✓ justhodl-signal-logger
- `23:01:37`     ✓ justhodl-stock-screener
- `23:01:37`     ✓ justhodl-valuations-agent
- `23:01:37`     ✓ macro-financial-intelligence
## C. Signal logger — what signals are being recorded?

- `23:01:37`   Distinct signal types in last 200 entries:
- `23:01:37`     screener_top_pick               119x
- `23:01:37`     edge_regime                      11x
- `23:01:37`     ml_risk                           9x
- `23:01:37`     plumbing_stress                   9x
- `23:01:37`     edge_composite                    8x
- `23:01:37`     carry_risk                        8x
- `23:01:37`     market_phase                      7x
- `23:01:37`     khalid_index                      6x
- `23:01:37`     momentum_gld                      6x
- `23:01:37`     crypto_fear_greed                 5x
- `23:01:37`     crypto_risk_score                 5x
- `23:01:37`     momentum_uso                      5x
- `23:01:37`     momentum_spy                      2x
- `23:01:37` 
  Total signals logged: 4579
- `23:01:37` 
  Signal-logger source — what does it scan for?
- `23:01:37`     Code references 8 signal type names
- `23:01:37`       carry_risk_score
- `23:01:37`       composite_score
- `23:01:37`       crypto_btc_signal
- `23:01:37`       crypto_risk_score
- `23:01:37`       edge_regime
- `23:01:37`       khalid_index
- `23:01:37`       ml_risk_score
- `23:01:37`       risk_score
## D. Outcome scoring — what time horizons are compared?

- `23:01:37`     'weekly': 1 mentions
- `23:01:37` 
  EventBridge schedule for outcome-checker:
- `23:01:38`     [ENABLED] justhodl-outcome-checker-weekly: cron(0 8 ? * SUN *)
## E. Calibrator — does it actually run?

- `23:01:38`   ✓ Found in AWS: justhodl-calibrator
- `23:01:38`     LastModified: 2026-03-11T09:12:17.000+0000
- `23:01:38` ⚠     ⚠ Source NOT in repo — version-control it
- `23:01:38`     Invocations last 7d: 1
- `23:01:38`     EB: [ENABLED] justhodl-calibrator-weekly: cron(0 9 ? * SUN *)
## F. Coverage gap — signals logged vs signals weighted

- `23:01:39`   Signal types in logger:    13
- `23:01:39`   Signal types weighted:     12
- `23:01:39` 
  Logged but UNWEIGHTED (system isn't learning from these):
- `23:01:39`     🔴 carry_risk
- `23:01:39`     🔴 edge_composite
- `23:01:39`     🔴 market_phase
- `23:01:39`     🔴 ml_risk
- `23:01:39`     🔴 momentum_gld
- `23:01:39`     🔴 momentum_spy
- `23:01:39`     🔴 momentum_uso
- `23:01:39`     🔴 plumbing_stress
- `23:01:39` 
  Weighted but NOT logged (weights stale/orphan):
- `23:01:39`     ⚠ cftc_bitcoin
- `23:01:39`     ⚠ cftc_crude
- `23:01:39`     ⚠ cftc_gold
- `23:01:39`     ⚠ cftc_spx
- `23:01:39`     ⚠ crypto_btc_signal
- `23:01:39`     ⚠ crypto_eth_signal
- `23:01:39`     ⚠ valuation_composite
- `23:01:39` Done

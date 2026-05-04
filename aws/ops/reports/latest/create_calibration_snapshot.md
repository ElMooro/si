# Create justhodl-calibration-snapshot + 30min schedule

**Status:** success  
**Duration:** 13.1s  
**Finished:** 2026-05-04T13:19:16+00:00  

## Log
- `13:19:03`   zip size: 3,011b
- `13:19:07` ✅   ✓ updated existing
# EventBridge schedule (every 30 minutes)

- `13:19:13` ✅   ✓ wired
# Smoke test

- `13:19:16`   status: 200  duration: 2.2s
- `13:19:16`   resp: {"statusCode": 200, "body": "{\"n_signal_types\": 47, \"n_outcomes_60d\": 1302, \"n_rated\": 19, \"weighted_avg_accuracy_60d\": 0.387, \"duration_s\": 1.35}"}
# S3 verify

- `13:19:16`   n_signal_types_tracked: 47
- `13:19:16`   n_signal_types_with_rolling_data: 19
- `13:19:16`   n_outcomes_60d: 1302
- `13:19:16`   weighted_avg_accuracy_60d: 0.387
- `13:19:16`   best_signal_60d: edge_regime
- `13:19:16`   worst_signal_60d: market_phase
- `13:19:16`   signals tracked: 47
- `13:19:16`   top 5 by accuracy:
- `13:19:16`     edge_regime                   
- `13:19:16`     crisis_hy_oas_vs_hyg           acc=0.9231 weight=1.4159 n=13
- `13:19:16`     khalid_index                  
- `13:19:16`     edge_composite                 acc=0.5714 weight=0.8317 n=49
- `13:19:16`     crypto_risk_score              acc=0.2949 weight=0.3961 n=78

# Create justhodl-calibration-snapshot + 30min schedule

**Status:** success  
**Duration:** 11.1s  
**Finished:** 2026-05-04T13:11:10+00:00  

## Log
- `13:10:59`   zip size: 3,327b
- `13:11:00` ✅   ✓ created
# EventBridge schedule (every 30 minutes)

- `13:11:08` ✅   ✓ wired
# Smoke test

- `13:11:10`   status: 200  duration: 1.7s
- `13:11:10`   resp: {"statusCode": 200, "body": "{\"n_signal_types\": 46, \"n_outcomes_60d\": 0, \"weighted_avg_accuracy\": 0.4251, \"duration_s\": 0.69}"}
# S3 verify

- `13:11:10`   n_signal_types_tracked: 46
- `13:11:10`   n_signal_types_with_accuracy: 5
- `13:11:10`   n_outcomes_60d: 0
- `13:11:10`   weighted_avg_accuracy: 0.4251
- `13:11:10`   best_signal: edge_composite
- `13:11:10`   worst_signal: momentum_uso
- `13:11:10`   signals tracked: 46
- `13:11:10`   top 5 by accuracy:
- `13:11:10`     edge_composite                 acc=0.5714 weight=0.8317 n=49
- `13:11:10`     plumbing_stress                acc=0.4286 weight=0.5429 n=49
- `13:11:10`     crypto_fear_greed              acc=0.3974 weight=0.4981 n=78
- `13:11:10`     crypto_risk_score              acc=0.2949 weight=0.3961 n=78
- `13:11:10`     momentum_uso                   acc=0.2727 weight=0.3816 n=55

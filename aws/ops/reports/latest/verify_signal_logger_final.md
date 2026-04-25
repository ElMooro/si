# Verify signal-logger writes real ml_risk + carry_risk values

**Status:** success  
**Duration:** 11.9s  
**Finished:** 2026-04-25T00:21:40+00:00  

## Log
## Trigger fresh signal-logger

- `00:21:40`   Status: 200
- `00:21:40`   Body:   {"statusCode": 200, "body": "{\"logged\": 25}"}
## Last 5 min: ml_risk + carry_risk signal_value field

- `00:21:40` 
  ml_risk: 1 signals in last 5 min
- `00:21:40`     signal_value: 60.0
- `00:21:40`     predicted_dir: NEUTRAL
- `00:21:40`     confidence: 0.2
- `00:21:40`     metadata.score: 60
- `00:21:40`     baseline_price: 713.94
- `00:21:40`     schema_version: 2
- `00:21:40`     regime_at_log: BEAR
- `00:21:40` 
  carry_risk: 0 signals in last 5 min
## Compare to dead-zero past

- `00:21:40` 
  ml_risk (6-24h ago, sample of 0):
- `00:21:40` 
  carry_risk (6-24h ago, sample of 0):
- `00:21:40` Done

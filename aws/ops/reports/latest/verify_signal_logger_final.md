# Verify signal-logger writes real ml_risk + carry_risk values

**Status:** success  
**Duration:** 10.6s  
**Finished:** 2026-04-25T00:23:02+00:00  

## Log
## Trigger fresh signal-logger

- `00:23:02`   Status: 200
- `00:23:02`   Body:   {"statusCode": 200, "body": "{\"logged\": 25}"}
## Last 5 min: ml_risk + carry_risk signal_value field

- `00:23:02` 
  ml_risk: 1 signals in last 5 min
- `00:23:02`     signal_value: 60.0
- `00:23:02`     predicted_dir: NEUTRAL
- `00:23:02`     confidence: 0.2
- `00:23:02`     metadata.score: 60
- `00:23:02`     baseline_price: 713.94
- `00:23:02`     schema_version: 2
- `00:23:02`     regime_at_log: BEAR
- `00:23:02` 
  carry_risk: 0 signals in last 5 min
## Compare to dead-zero past

- `00:23:02` 
  ml_risk (6-24h ago, sample of 0):
- `00:23:02` 
  carry_risk (6-24h ago, sample of 0):
- `00:23:02` Done

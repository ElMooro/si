# Phase 10 — bootstrap auction-crisis-detector

**Status:** success  
**Duration:** 8.1s  
**Finished:** 2026-04-27T12:48:22+00:00  

## Log
## 1. Locate source

- `12:48:14`   SOURCE_DIR: /home/runner/work/si/si/aws/lambdas/justhodl-auction-crisis-detector/source
- `12:48:14`   Python files: ['lambda_function.py']
## 2. Build deployment zip

- `12:48:14`   zip size: 7,572 bytes
## 3. Create or update Lambda

- `12:48:14`   Lambda doesn't exist — creating fresh
- `12:48:15`   ✅ created Lambda: arn:aws:lambda:us-east-1:857687956942:function:justhodl-auction-crisis-detector
## 4. EventBridge schedule

- `12:48:15`   ✅ rule justhodl-auction-crisis-refresh rate(1 hour) ENABLED
## 5. EB → Lambda permission + target

- `12:48:15`   ✅ permission added
- `12:48:15`   ✅ EB target wired
## 6. Wait for Active state, then seed-invoke

- `12:48:18`   state: Active
- `12:48:22`   ✅ first run OK (4.0s)
- `12:48:22`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 3.06, \"regime\": \"CALM\", \"composite_score\": 6.3, \"n_recent\": 15, \"issuance_anomaly_pct\": -7.6}"}
## FINAL

- `12:48:22`   Phase 10 auction-crisis-detector live.
- `12:48:22`   Output: s3://justhodl-dashboard-live/data/auction-crisis.json
- `12:48:22`   Schedule: rate(1 hour)
- `12:48:22` Done

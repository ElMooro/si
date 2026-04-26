# Phase 9.1 — create justhodl-crisis-plumbing Lambda

**Status:** success  
**Duration:** 12.1s  
**Finished:** 2026-04-26T15:03:19+00:00  

## Log
## 1. Pre-flight check

- `15:03:07`   ✅ justhodl-crisis-plumbing does not exist — safe to create
## 2. Build zip from aws/lambdas/justhodl-crisis-plumbing/source/

- `15:03:07`   zip: 8150B
## 3. Create justhodl-crisis-plumbing

- `15:03:11`   ✅ created and Active
## 4. Test-invoke justhodl-crisis-plumbing

- `15:03:13`   ✅ OK (2.2s)
- `15:03:13`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 1.4, \"composite_signal\": \"NORMAL\", \"composite_score\": 37.0, \"n_indices\": 4, \"n_flagged\": 0, \"s3_key\": \"data/crisis-plumbing.json\"}"}
## 5. Verify s3://.../data/crisis-plumbing.json

- `15:03:18`   ✅ written: 4369B  age=5s
- `15:03:18`   composite signal: NORMAL
- `15:03:18`   composite score: 37.0
- `15:03:18`   n_indices_available: 4
- `15:03:18`   flagged: []
- `15:03:18`     ✅ STLFSI4: pct=11.7 val=-0.7584
- `15:03:18`     ✅ NFCI: pct=49.2 val=-0.497
- `15:03:18`     ✅ ANFCI: pct=54.4 val=-0.474
- `15:03:18`     ✅ KCFSI: pct=32.5 val=-0.586617129294089
- `15:03:18` ⚠     ✗ OFRFSI: not available
## 6. Create EventBridge rule justhodl-crisis-plumbing-refresh (rate(6 hours))

- `15:03:18`   ✅ rule created
- `15:03:19`   ✅ EventBridge invoke permission granted
- `15:03:19`   ✅ rule targets justhodl-crisis-plumbing
## FINAL

- `15:03:19`   Lambda: justhodl-crisis-plumbing
- `15:03:19`   S3 output: data/crisis-plumbing.json
- `15:03:19`   Schedule: rate(6 hours) via justhodl-crisis-plumbing-refresh
- `15:03:19` 
- `15:03:19`   Next: create /crisis.html on the website to consume this data
- `15:03:19` Done

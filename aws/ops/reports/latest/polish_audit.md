# Polish audit — verify recent fixes + investigate stale data

**Status:** success  
**Duration:** 3.4s  
**Finished:** 2026-04-24T23:32:54+00:00  

## Log
## A. Did BRK.B make it into the latest report.json?

- `23:32:50`   report.json age: 2.8 min
- `23:32:50`   Total stocks: 188
- `23:32:50` ✅   ✓ Found 'BRK.B': name='Berkshire Hathaway', price=$469.32
## B. Are signals from the last 2h still getting baseline_price?

- `23:32:51`   Signals logged in last 2h: 25
- `23:32:51`   With baseline_price: 25 (100%)
- `23:32:51`     screener_top_pick               15/15
- `23:32:51`     market_phase                    1/1
- `23:32:51`     ml_risk                         1/1
- `23:32:51`     plumbing_stress                 1/1
- `23:32:51`     crypto_risk_score               1/1
- `23:32:51`     edge_regime                     1/1
- `23:32:51`     carry_risk                      1/1
- `23:32:51`     crypto_fear_greed               1/1
- `23:32:51` ✅   ✓ Fix holding — 100% have baseline
## C. Why is predictions.json stale?

- `23:32:51`   predictions.json: 30.6h old, 14351 bytes
- `23:32:51`   Last modified: 2026-04-23T16:55:28+00:00
- `23:32:51` 
  Lambda: justhodl-ml-predictions
- `23:32:51`     LastModified: 2026-02-20T01:41:42.000+0000
- `23:32:51`     State: Active
- `23:32:51`     StateReason: (none)
- `23:32:51`     LastUpdateStatus: Successful
- `23:32:51`     LastUpdateStatusReason: (none)
- `23:32:51`     EB rules: ['justhodl-ml-predictions-schedule']
- `23:32:51`       [ENABLED] justhodl-ml-predictions-schedule: rate(4 hours)
- `23:32:52`     Last 48h: 18 invocations, 0 errors
- `23:32:52` 
  Lambda: MLPredictor
- `23:32:52`     LastModified: 2025-05-30T03:51:24.000+0000
- `23:32:52`     State: Active
- `23:32:52`     StateReason: (none)
- `23:32:52`     LastUpdateStatus: Successful
- `23:32:52`     LastUpdateStatusReason: (none)
- `23:32:52`     EB rules: ['MLPredictorDaily']
- `23:32:52`       [ENABLED] MLPredictorDaily: cron(15 12 * * ? *)
- `23:32:53`     Last 48h: 2 invocations, 0 errors
## D. Why is valuations-data.json 23 days stale?

- `23:32:53`   valuations-data.json: 23.4 days old, 2188 bytes
- `23:32:53`   Last modified: 2026-04-01T14:00:46+00:00
- `23:32:53` 
  Lambda: justhodl-valuations-agent
- `23:32:53`     LastModified: 2026-03-05T05:08:39.000+0000
- `23:32:53`     State: Active
- `23:32:53`     StateReason: (none)
- `23:32:53`     LastUpdateStatus: Successful
- `23:32:53`     Timeout: 120s, Memory: 512MB
- `23:32:53`       [ENABLED] valuations-monthly-update: cron(0 14 1 * ? *)
- `23:32:54`     Daily breakdown last 30 days (invocations / errors):
- `23:32:54`       2026-03-31: 1 inv / 0 err
- `23:32:54`     Total last 30d: 1 invocations, 0 errors
- `23:32:54` Done

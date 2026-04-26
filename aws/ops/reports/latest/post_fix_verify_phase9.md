# Phase 9 post-fix end-to-end verify

**Status:** success  
**Duration:** 4.6s  
**Finished:** 2026-04-26T22:02:49+00:00  

## Log
## 1. crisis-plumbing — re-invoke + read

- `22:02:47`   ✅ invoke OK (2.5s)
- `22:02:47`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 1.8, \"composite_signal\": \"NORMAL\", \"composite_score\": 37.0, \"n_indices\": 4, \"n_flagged\": 0, \"s3_key\": \"data/crisis-plumbing.json\"}"}
- `22:02:47`   schema: 1.1  size: 7010B  series_fetched: 27
- `22:02:47`   crisis_indices:           4/5 populated
- `22:02:47`   plumbing_tier2:           4/4 populated
- `22:02:47`   funding_credit_signals:   5/6 populated
- `22:02:47`   xcc_basis_proxy:          3/3 populated
- `22:02:47`   mmf_composition:          None (expected null after fix)
- `22:02:47` 
- `22:02:47`   Critical-signal spot check:
- `22:02:47`     SOFR-IORB:  0.0bps  signal=NORMAL  z=0.11
- `22:02:47`     HY OAS:     3bps  signal=NORMAL
- `22:02:47`     USD-JPY rate diff: 2.42%  z=-1.4  signal=WATCH
- `22:02:47`     Broad USD:  118.08  z=-1.77  signal=WATCH
## 2. correlation-breaks — re-invoke + read

- `22:02:49`   ✅ invoke OK (1.3s)
- `22:02:49`   payload: {"statusCode": 200, "body": "{\"status\": \"warming_up\", \"n_dates\": 0}"}
- `22:02:49` ⚠   ⚠ status: warming_up — Insufficient aligned data: 0 dates (need ≥312)
## FINAL VERDICT

- `22:02:49`   ✅  crisis-plumbing — all sections populated
- `22:02:49`   ✗  correlation-breaks — composite + pairs
- `22:02:49` 
- `22:02:49`   🟡 some gaps — see above
- `22:02:49` Done

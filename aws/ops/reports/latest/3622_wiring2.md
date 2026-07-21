# ops 3621 — regime-context + sentinel + best-setups wiring

**Status:** success  
**Duration:** 28.8s  
**Finished:** 2026-07-21T03:18:58+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:18:29`   zip: 107204 bytes
## 1. Lambda

- `03:18:30`   Lambda exists — updating
- `03:18:35` ✅   ✓ updated justhodl-best-setups
## 3. Smoke test

- `03:18:35`   invoking justhodl-best-setups…
- `03:18:43` ✅   ✓ smoke test passed
- `03:18:43`     ok                       True
- `03:18:43`     n_setups                 488
- `03:18:43`     strong_buy               1
- `03:18:43`     buy                      14
- `03:18:43`     weight_source            prior-only
- `03:18:49` G1_shared_and_boom False
- `03:18:49`   zip: 89562 bytes
## 1. Lambda

- `03:18:50`   Lambda exists — updating
- `03:18:53` ✅   ✓ updated justhodl-alert-sentinel
## 3. Smoke test

- `03:18:53`   invoking justhodl-alert-sentinel…
- `03:18:55` ✅   ✓ smoke test passed
- `03:18:55`     sent                     False
- `03:18:55`     changes                  0
- `03:18:58` G2_sentinel_kr True
- `03:18:58` VERDICT: GAPS: G1_shared_and_boom

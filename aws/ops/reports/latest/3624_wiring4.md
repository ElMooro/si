# ops 3621 — regime-context + sentinel + best-setups wiring

**Status:** success  
**Duration:** 23.3s  
**Finished:** 2026-07-21T03:24:43+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:24:20`   zip: 107303 bytes
## 1. Lambda

- `03:24:21`   Lambda exists — updating
- `03:24:23` ✅   ✓ updated justhodl-best-setups
## 3. Smoke test

- `03:24:23`   invoking justhodl-best-setups…
- `03:24:30` ✅   ✓ smoke test passed
- `03:24:30`     ok                       True
- `03:24:30`     n_setups                 488
- `03:24:30`     strong_buy               1
- `03:24:30`     buy                      14
- `03:24:30`     weight_source            prior-only
- `03:24:35` G1_shared_and_boom True
- `03:24:35`   zip: 89562 bytes
## 1. Lambda

- `03:24:35`   Lambda exists — updating
- `03:24:40` ✅   ✓ updated justhodl-alert-sentinel
## 3. Smoke test

- `03:24:40`   invoking justhodl-alert-sentinel…
- `03:24:42` ✅   ✓ smoke test passed
- `03:24:42`     sent                     False
- `03:24:42`     changes                  0
- `03:24:43` G2_sentinel_kr True
- `03:24:43` VERDICT: PASS_ALL

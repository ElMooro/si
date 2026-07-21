# ops 3621 — regime-context + sentinel + best-setups wiring

**Status:** success  
**Duration:** 28.8s  
**Finished:** 2026-07-21T03:12:51+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:12:22`   zip: 107081 bytes
## 1. Lambda

- `03:12:23`   Lambda exists — updating
- `03:12:28` ✅   ✓ updated justhodl-best-setups
## 3. Smoke test

- `03:12:28`   invoking justhodl-best-setups…
- `03:12:36` ✅   ✓ smoke test passed
- `03:12:36`     ok                       True
- `03:12:36`     n_setups                 488
- `03:12:36`     strong_buy               1
- `03:12:36`     buy                      14
- `03:12:36`     weight_source            prior-only
- `03:12:42` G1_shared_and_boom False
- `03:12:42`   zip: 89562 bytes
## 1. Lambda

- `03:12:42`   Lambda exists — updating
- `03:12:46` ✅   ✓ updated justhodl-alert-sentinel
## 3. Smoke test

- `03:12:46`   invoking justhodl-alert-sentinel…
- `03:12:48` ✅   ✓ smoke test passed
- `03:12:48`     sent                     False
- `03:12:48`     changes                  5
- `03:12:51` G2_sentinel_kr False
- `03:12:51` VERDICT: GAPS: G1_shared_and_boom,G2_sentinel_kr

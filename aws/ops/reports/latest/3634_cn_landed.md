# ops 3634 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 106.8s  
**Finished:** 2026-07-21T04:48:05+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:46:19`   zip: 92099 bytes
## 1. Lambda

- `04:46:19`   Lambda exists — updating
- `04:46:22` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:46:22`   invoking justhodl-asia-leads…
- `04:46:39` ✅   ✓ smoke test passed
- `04:46:39`     ok                       True
- `04:46:39`     kr_yoy                   47.96
- `04:46:39`     tw_yoy                   48.33
- `04:46:55` G1_tw True
- `04:46:55`   zip: 90049 bytes
## 1. Lambda

- `04:46:56`   Lambda exists — updating
- `04:47:01` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:47:01`   invoking justhodl-china-liquidity…
- `04:47:49` ✅   ✓ smoke test passed
- `04:47:49`     ok                       True
- `04:47:49`     regime                   NEUTRAL
- `04:47:49`     credit_impulse_pp        -5.52
- `04:47:49`     m2_yoy                   8.21
- `04:48:05` G2_cn True
- `04:48:05` VERDICT: PASS_ALL

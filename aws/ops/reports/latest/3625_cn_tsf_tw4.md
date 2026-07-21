# ops 3625 — CN TSF via edge + TW stage-4

**Status:** success  
**Duration:** 66.8s  
**Finished:** 2026-07-21T03:40:11+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:39:04`   zip: 88361 bytes
## 1. Lambda

- `03:39:04`   Lambda exists — updating
- `03:39:10` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `03:39:10`   invoking justhodl-china-liquidity…
- `03:39:25` ✅   ✓ smoke test passed
- `03:39:25`     ok                       True
- `03:39:25`     regime                   NEUTRAL
- `03:39:25`     credit_impulse_pp        -5.52
- `03:39:25`     m2_yoy                   8.21
- `03:39:33` G1_cn_tsf True
- `03:39:33`   zip: 90352 bytes
## 1. Lambda

- `03:39:33`   Lambda exists — updating
- `03:39:36` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `03:39:36`   invoking justhodl-asia-leads…
- `03:39:56` ✅   ✓ smoke test passed
- `03:39:56`     ok                       True
- `03:39:56`     kr_yoy                   47.96
- `03:39:56`     tw_yoy                   48.33
- `03:40:11` G2_tw_stage4 True
- `03:40:11` VERDICT: PASS_ALL

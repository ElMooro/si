# ops 3625 — CN TSF via edge + TW stage-4

**Status:** success  
**Duration:** 82.5s  
**Finished:** 2026-07-21T03:48:35+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:47:13`   zip: 88580 bytes
## 1. Lambda

- `03:47:13`   Lambda exists — updating
- `03:47:16` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `03:47:16`   invoking justhodl-china-liquidity…
- `03:47:51` ✅   ✓ smoke test passed
- `03:47:51`     ok                       True
- `03:47:51`     regime                   NEUTRAL
- `03:47:51`     credit_impulse_pp        -5.52
- `03:47:51`     m2_yoy                   8.21
- `03:47:59` G1_cn_tsf False
- `03:47:59`   zip: 90868 bytes
## 1. Lambda

- `03:47:59`   Lambda exists — updating
- `03:48:05` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `03:48:05`   invoking justhodl-asia-leads…
- `03:48:21` ✅   ✓ smoke test passed
- `03:48:21`     ok                       True
- `03:48:21`     kr_yoy                   47.96
- `03:48:21`     tw_yoy                   48.33
- `03:48:35` G2_tw_stage4 None
- `03:48:35` VERDICT: GAPS: G1_cn_tsf,G2_tw_stage4

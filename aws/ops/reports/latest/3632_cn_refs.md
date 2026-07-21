# ops 3632 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 80.1s  
**Finished:** 2026-07-21T04:35:06+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:33:46`   zip: 92097 bytes
## 1. Lambda

- `04:33:46`   Lambda exists — updating
- `04:33:51` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:33:51`   invoking justhodl-asia-leads…
- `04:34:10` ✅   ✓ smoke test passed
- `04:34:10`     ok                       True
- `04:34:10`     kr_yoy                   47.96
- `04:34:10`     tw_yoy                   48.33
- `04:34:26` G1_tw True
- `04:34:26`   zip: 89332 bytes
## 1. Lambda

- `04:34:27`   Lambda exists — updating
- `04:34:29` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:34:29`   invoking justhodl-china-liquidity…
- `04:34:52` ✅   ✓ smoke test passed
- `04:34:52`     ok                       True
- `04:34:52`     regime                   NEUTRAL
- `04:34:52`     credit_impulse_pp        -5.52
- `04:34:52`     m2_yoy                   8.21
- `04:35:06` G2_cn True
- `04:35:06` VERDICT: PASS_ALL

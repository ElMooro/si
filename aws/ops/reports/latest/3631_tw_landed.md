# ops 3631 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 74.8s  
**Finished:** 2026-07-21T04:27:49+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:26:34`   zip: 92097 bytes
## 1. Lambda

- `04:26:35`   Lambda exists — updating
- `04:26:38` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:26:38`   invoking justhodl-asia-leads…
- `04:26:58` ✅   ✓ smoke test passed
- `04:26:58`     ok                       True
- `04:26:58`     kr_yoy                   47.96
- `04:26:58`     tw_yoy                   48.33
- `04:27:14` G1_tw True
- `04:27:14`   zip: 88983 bytes
## 1. Lambda

- `04:27:14`   Lambda exists — updating
- `04:27:17` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:27:17`   invoking justhodl-china-liquidity…
- `04:27:38` ✅   ✓ smoke test passed
- `04:27:38`     ok                       True
- `04:27:38`     regime                   NEUTRAL
- `04:27:38`     credit_impulse_pp        -5.52
- `04:27:38`     m2_yoy                   8.21
- `04:27:49` G2_cn True
- `04:27:49` VERDICT: PASS_ALL

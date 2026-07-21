# ops 3633 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 116.9s  
**Finished:** 2026-07-21T04:41:21+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:39:25`   zip: 92099 bytes
## 1. Lambda

- `04:39:25`   Lambda exists — updating
- `04:39:29` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:39:29`   invoking justhodl-asia-leads…
- `04:39:55` ✅   ✓ smoke test passed
- `04:39:55`     ok                       True
- `04:39:55`     kr_yoy                   47.96
- `04:39:55`     tw_yoy                   48.33
- `04:40:12` G1_tw True
- `04:40:12`   zip: 89752 bytes
## 1. Lambda

- `04:40:12`   Lambda exists — updating
- `04:40:16` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:40:16`   invoking justhodl-china-liquidity…
- `04:40:42` ✅   ✓ smoke test passed
- `04:40:42`     ok                       True
- `04:40:42`     regime                   NEUTRAL
- `04:40:42`     credit_impulse_pp        -5.52
- `04:40:42`     m2_yoy                   8.21
- `04:41:21` G2_cn True
- `04:41:21` VERDICT: PASS_ALL

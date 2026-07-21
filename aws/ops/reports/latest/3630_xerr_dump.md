# ops 3630 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 71.4s  
**Finished:** 2026-07-21T04:21:48+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:20:37`   zip: 91607 bytes
## 1. Lambda

- `04:20:37`   Lambda exists — updating
- `04:20:42` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:20:42`   invoking justhodl-asia-leads…
- `04:21:02` ✅   ✓ smoke test passed
- `04:21:02`     ok                       True
- `04:21:02`     kr_yoy                   47.96
- `04:21:02`     tw_yoy                   48.33
- `04:21:19` G1_tw True
- `04:21:19`   zip: 88983 bytes
## 1. Lambda

- `04:21:19`   Lambda exists — updating
- `04:21:24` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:21:24`   invoking justhodl-china-liquidity…
- `04:21:37` ✅   ✓ smoke test passed
- `04:21:37`     ok                       True
- `04:21:37`     regime                   NEUTRAL
- `04:21:37`     credit_impulse_pp        -5.52
- `04:21:37`     m2_yoy                   8.21
- `04:21:48` G2_cn True
- `04:21:48` VERDICT: PASS_ALL

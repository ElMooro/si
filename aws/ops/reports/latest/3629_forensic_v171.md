# ops 3629 — deployed-zip forensic + v1.7.1/v2.5

**Status:** success  
**Duration:** 147.2s  
**Finished:** 2026-07-21T04:17:30+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:15:03`   zip: 91607 bytes
## 1. Lambda

- `04:15:04`   Lambda exists — updating
- `04:15:09` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `04:15:09`   invoking justhodl-asia-leads…
- `04:15:50` ✅   ✓ smoke test passed
- `04:15:50`     ok                       True
- `04:15:50`     kr_yoy                   47.96
- `04:15:50`     tw_yoy                   48.33
- `04:16:22` G1_tw True
- `04:16:22`   zip: 88970 bytes
## 1. Lambda

- `04:16:22`   Lambda exists — updating
- `04:16:28` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `04:16:28`   invoking justhodl-china-liquidity…
- `04:16:53` ✅   ✓ smoke test passed
- `04:16:53`     ok                       True
- `04:16:53`     regime                   NEUTRAL
- `04:16:53`     credit_impulse_pp        -5.52
- `04:16:53`     m2_yoy                   8.21
- `04:17:30` G2_cn True
- `04:17:30` VERDICT: PASS_ALL

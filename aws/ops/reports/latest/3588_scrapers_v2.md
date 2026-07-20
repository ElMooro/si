# ops 3588 — PBoC AFRE + DGBAS export-orders live

**Status:** success  
**Duration:** 60.1s  
**Finished:** 2026-07-20T19:01:19+00:00  

## Error

```
SystemExit: 0
```

## Log
- `19:00:20`   zip: 86937 bytes
## 1. Lambda

- `19:00:20`   Lambda exists — updating
- `19:00:25` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `19:00:25`   invoking justhodl-china-liquidity…
- `19:00:43` ✅   ✓ smoke test passed
- `19:00:43`     ok                       True
- `19:00:43`     regime                   NEUTRAL
- `19:00:43`     credit_impulse_pp        -5.52
- `19:00:43`     m2_yoy                   8.21
- `19:00:43`   zip: 85383 bytes
## 1. Lambda

- `19:00:43`   Lambda exists — updating
- `19:00:49` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `19:00:49`   invoking justhodl-asia-leads…
- `19:00:58` ✅   ✓ smoke test passed
- `19:00:58`     ok                       True
- `19:00:58`     kr_yoy                   47.96
- `19:00:58`     tw_yoy                   48.33
- `19:00:58` PASS  G1_deployed — both v-bumps deployed via helper
- `19:01:08` PASS  G2_tw_orders — v1.2.0 orders yoy=None% usd_bn=None period=None err=regex found no orders print on page raw='       Import on customs basis Growth Rate       Export on customs basis Growth Rate       Total Population   '
- `19:01:19` FAIL  G3_pboc_afre — report='Report on Aggregate Financing to the Real Economy (Flow) (August 2025)' rows=0 afre_latest=None (100M RMB) n_vals=0 cache=None err=None
- `19:01:19` PASS  G4_page_row — served: Taiwan export ORDERS row
- `19:01:19` VERDICT: GAPS: G3_pboc_afre

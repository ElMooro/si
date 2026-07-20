# ops 3587 — PBoC AFRE + DGBAS export-orders live

**Status:** success  
**Duration:** 137.1s  
**Finished:** 2026-07-20T18:57:28+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:55:12`   zip: 86673 bytes
## 1. Lambda

- `18:55:12`   Lambda exists — updating
- `18:55:18` ✅   ✓ updated justhodl-china-liquidity
## 3. Smoke test

- `18:55:18`   invoking justhodl-china-liquidity…
- `18:55:25` ✅   ✓ smoke test passed
- `18:55:25`     ok                       True
- `18:55:25`     regime                   NEUTRAL
- `18:55:25`     credit_impulse_pp        -5.52
- `18:55:25`     m2_yoy                   8.21
- `18:55:26`   zip: 85144 bytes
## 1. Lambda

- `18:55:26`   Lambda exists — updating
- `18:55:29` ✅   ✓ updated justhodl-asia-leads
## 3. Smoke test

- `18:55:29`   invoking justhodl-asia-leads…
- `18:55:33` ✅   ✓ smoke test passed
- `18:55:33`     ok                       True
- `18:55:33`     kr_yoy                   47.96
- `18:55:33`     tw_yoy                   48.33
- `18:55:33` PASS  G1_deployed — both v-bumps deployed via helper
- `18:55:36` PASS  G2_tw_orders — v1.2.0 orders yoy=None% usd_bn=None period=None err=regex found no orders print on page raw='                        #chartdiv { width: 100%; height: 500px; max-width: 100%; }     const jhxiaoQS = '?sid='
- `18:55:43` FAIL  G3_pboc_afre — report='Report on Aggregate Financing to the Real Economy (Flow) (August 2025)' rows=0 afre_latest=None (100M RMB) n_vals=0 cache=None err=None
- `18:57:28` PASS  G4_page_row — served: Taiwan export ORDERS row
- `18:57:28` VERDICT: GAPS: G3_pboc_afre

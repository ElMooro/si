# ops 3866 — WIRE finviz-universe donor + gate on measured coverage

**Status:** failure  
**Duration:** 20.9s  
**Finished:** 2026-07-25T16:46:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| after_rows | before_liquidity_nonneutral | before_rotation_nonneutral | before_rotation_notes | before_rows | before_s3 | liquidity_nonneutral | previously_unresolved_present | rotation_nonneutral | rotation_notes | still_untilted |
|---|---|---|---|---|---|---|---|---|---|---|
|  | 16 | 16 | 16 | 25 | 2026-07-24T22:49:08+00:00 |  |  |  |  |  |
| 25 |  |  |  |  |  | 0 | 4 | 25 | 25 | [] |

## Log
## 1. BEFORE

## 2. ZIP-SETTLE BY MARKER — never invoke the old artifact

- `16:46:03` ✅   new artifact live on attempt 1 (105,263 zip bytes)
- `16:46:03` ✅   State=Active LastUpdateStatus=Successful
## 3. invoke (async — the ranker fans out across ~30 feeds)

- `16:46:23` ✅   artifact rewritten on attempt 1 (2026-07-25T16:46:10+00:00)
## 4. AFTER — measured coverage

- `16:46:23` ✅   rotation coverage improved vs before
- `16:46:23` ✅   rotation tilt on >= 20/25
- `16:46:23` ✗   liquidity overlay tracks the same sector map
- `16:46:23` ✅   no row lost its tilt (no regression)
## 5. honest note on what was deliberately NOT changed

- `16:46:23`   risk_regime_mult stays 0/25 by design: probe 3865 measured risk_regime_score = 2.5, inside the engine's neutral band (-12, 12]. A neutral regime SHOULD produce no tilt; forcing one would be a manufactured signal.
- `16:46:23` ✗ FAILED 1: ['liquidity overlay tracks the same sector map']

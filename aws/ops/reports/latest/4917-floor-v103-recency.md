# ops 4917 -- floor-audit v1.0.3 recency-first bind

**Status:** failure  
**Duration:** 40.6s  
**Finished:** 2026-08-19T19:47:55+00:00  

## Error

```
SystemExit: 1
```

## Data

| as_of | bmnr_cov | bmnr_crypto | bmnr_verdict | brokers | g0_ok | g1 | g2 | g3 | suspects | universe | wrappers |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | PASS |  |  |  |  |  |
| 2026-08-19T19:47:46+00:00 |  |  |  | 2 | 28 |  | PASS |  | 1 | 33 | 6 |
|  | 0.9752 | 0.9468 | IN_LINE |  |  |  |  | PASS |  |  |  |

## Log
## G1 redeploy

- `19:47:14`   zip: 110136 bytes
## 1. Lambda

- `19:47:15`   Lambda exists — updating
- `19:47:18` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `19:47:18`   invoking justhodl-floor-audit…
## G2 fresh run

## G3 BMNR regression

## G4 BTBT spec case (HARD)

- `19:47:55` FAIL G4: BTBT crypto_coverage=0.6502 bind=CryptoAssetFairValueCurrent+CryptoAssetFairValueNoncurrent end=2026-06-30 (spec ~1/3 of mcap, fresh-quarter bind required)

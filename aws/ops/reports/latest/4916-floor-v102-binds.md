# ops 4916 -- floor-audit v1.0.2 bind gates

**Status:** failure  
**Duration:** 52.2s  
**Finished:** 2026-08-19T19:37:11+00:00  

## Error

```
SystemExit: 1
```

## Data

| as_of | bmnr_bind | bmnr_cov | bmnr_crypto | bmnr_verdict | brokers | btbt_tag | end | g0_ok | g1 | g2 | g3 | suspects | universe | val | wrappers |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |
| 2026-08-19T19:37:02+00:00 |  |  |  |  | 0 |  |  | 28 |  | PASS |  | 1 | 33 |  | 6 |
|  | "CryptoAssetFairValue" | 0.9663 | 0.9382 | IN_LINE |  |  |  |  |  |  | PASS |  |  |  |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetCost | 2026-06-30 |  |  |  |  |  |  | 269113000 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedLossOperating | 2026-06-30 |  |  |  |  |  |  | 149911000 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedAndUnrealizedGainLossNonoperating | 2025-09-30 |  |  |  |  |  |  | 145990197 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetFairValueCurrent | 2026-06-30 |  |  |  |  |  |  | 120149000 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetFairValueNoncurrent | 2026-06-30 |  |  |  |  |  |  | 120149000 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedAndUnrealizedGainNonoperating | 2024-12-31 |  |  |  |  |  |  | 55709711 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedGainOperating | 2023-12-31 |  |  |  |  |  |  | 18789998 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetMining | 2026-06-30 |  |  |  |  |  |  | 6076000 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetFairValue | 2026-03-31 |  |  |  |  |  |  | 2296509 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetNumberOfUnits | None |  |  |  |  |  |  | None |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedAndUnrealizedGainLossOperating | 2025-12-31 |  |  |  |  |  |  | -29214789 |  |
|  |  |  |  |  |  | us-gaap:CryptoAssetRealizedGainLossOperating | 2026-06-30 |  |  |  |  |  |  | -149911000 |  |

## Log
## G1 redeploy

- `19:36:19`   zip: 109390 bytes
## 1. Lambda

- `19:36:20`   Lambda exists — updating
- `19:36:25` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `19:36:25`   invoking justhodl-floor-audit…
## G2 fresh run

## G3 BMNR un-quarantined

## G4 BTBT spec-case bind (HARD)

- `19:37:10` FAIL G4: BTBT crypto_coverage=0.0046 (spec ~1/3 of mcap). Tag inventory follows:

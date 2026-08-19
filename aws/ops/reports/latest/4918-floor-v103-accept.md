# ops 4918 -- floor-audit v1.0.3 acceptance

**Status:** success  
**Duration:** 131.9s  
**Finished:** 2026-08-19T19:53:17+00:00  

## Data

| alert | as_of | bmnr_cov | bmnr_crypto | bmnr_verdict | broker_sheets | brokers | btbt_bind | btbt_bind_end | btbt_cov | btbt_crypto_cov | btbt_dd20 | btbt_filing_fv | btbt_mark_ratio | btbt_superseded | btbt_verdict | cov | crypto | dat | dd20 | duration_s | edge | g0_ok | g1 | g2 | g3 | g4 | g5 | g6 | res | sense | sev | status | suspects | universe | v | verdict | wrappers |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 2026-08-19T19:51:40+00:00 |  |  |  |  | 2 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 28 |  | PASS |  |  |  |  |  |  |  |  | 1 | 33 |  |  | 6 |
|  |  | 0.976 | 0.9476 | IN_LINE |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | CryptoAssetFairValueCurrent+CryptoAssetFairValueNoncurrent | 2026-06-30 | 0.6945 | 0.6503 | -0.14375 | 240298000.0 | 1.337003 | {"end": "2026-03-31", "val": 2296509.0, "why": "stale vs splits"} | SENSELESS_DRAWDOWN |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | COIN,HOOD |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |  |
| AIFC |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6.5579 |  |  |  |  |  |  |  |  |  |  |  |  | -0.838866 | 0 | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| CNTN |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 3.9434 |  |  |  |  |  |  |  |  |  |  |  |  | -0.658078 | 0 | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| UPXI |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.7121 |  |  |  |  |  |  |  |  |  |  |  |  | None | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| TONX |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2.994 |  |  |  |  |  |  |  |  |  |  |  |  | None | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| FWDI |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.1086 |  |  |  |  |  |  |  |  |  |  |  |  | None | None | CRITICAL |  |  |  | BELOW_LIQUID_FLOOR |  |  |
| BTBT |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.6945 |  |  |  |  |  |  |  |  |  |  |  |  | -0.520276 | 0 | HIGH |  |  |  | SENSELESS_DRAWDOWN |  |  |
| ABTC |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.8147 |  |  |  |  |  |  |  |  |  |  |  |  | -0.589157 | 0 | MEDIUM |  |  |  | SENSELESS_DRAWDOWN |  |  |
| HIVE |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.3096 |  |  |  |  |  |  |  |  |  |  |  |  | -0.434797 | 0 | MEDIUM |  |  |  | STRETCHED |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.6945 | 0.6503 | BTBT | -0.14375 |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  | SENSELESS_DRAWDOWN |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.976 | 0.9476 | BMNR | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  | IN_LINE |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None | None | MSTR | None |  |  |  |  |  |  |  |  |  |  |  |  | SKIP |  |  |  | None |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.9068 | 0.8701 | SBET | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  | IN_LINE |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | -0.4184 | 0.6974 | DFDV | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  | IN_LINE |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.7121 | 2.9599 | UPXI | -0.001249 |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  | BELOW_LIQUID_FLOOR |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PENDING |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 131 |  |  |  |  |  |  |  |  |  |  |  | GREEN |  |  |  |  |  |

## Log
## G1 redeploy

- `19:51:05`   zip: 110136 bytes
## 1. Lambda

- `19:51:06`   Lambda exists — updating
- `19:51:09` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `19:51:09`   invoking justhodl-floor-audit…
## G2 fresh run

## G3 BMNR regression

## G4 BTBT spec case (HARD)

## G5 broker quarantine live

## G6 DAT board

## edge (soft)


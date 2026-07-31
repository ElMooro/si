# ops 4170 — IPYY + FI + label activation

**Status:** success  
**Duration:** 121.4s  
**Finished:** 2026-07-31T18:27:09+00:00  

## Data

| FI | IPYY | fi_bulk_countries | fi_mask | freq | ip_yoy | ip_yoy_cc | tr_pick |
|---|---|---|---|---|---|---|---|
|  |  |  |  |  | -1.23 | JPN |  |
|  |  |  |  | M |  |  | YOY_PCH_PA_PT |
|  |  | 146 | ..CP01.YOY_PCH_PA_PT.M |  |  |  |  |
| 141 | 41 |  |  |  |  |  |  |

## Log
## A. IPYY: obs=13 yoy, retried, JPN->USA fallback

## B. FI: full-doc transform/freq sets, ranked pick

- `18:25:53`   attr order: ["COUNTRY", "INDEX_TYPE", "COICOP_1999", "TYPE_OF_TRANSFORMATION", "FREQUENCY"]
- `18:25:53`   transforms: ["IX", "POP_PCH_PA_PT", "SRP_IX", "SRP_POP_PCH_PA_PT", "SRP_YOY_PCH_PA_PT", "WGT", "WGT_PT", "YOY_PCH_PA_PT"]
- `18:25:53`   freqs: ["A", "M", "Q"]
- `18:25:54`   spots: [["USA", "2.761783572593346"]]
## C. wire feed v1.6 + vault v3.24.0

- `18:26:04` ✅   justhodl-families-feed settled at loop 1
- `18:26:14` ✅   justhodl-tradingview settled at loop 1
## D. invokes: families, symbol (bare-misses), vault

- `18:27:09` ✅   vault fired — 4171 converts
- `18:27:09` ✅   IPYY computable
- `18:27:09` ✅   FI mask proven >=40
- `18:27:09` ✅   feed v1.6 settled
- `18:27:09` ✅   vault v3.24.0 settled
- `18:27:09` ✅   IPYY >= 25
- `18:27:09` ✅   FI >= 60
- `18:27:09` ✅ IPYY+FI WIRED — mask=..CP01.YOY_PCH_PA_PT.M

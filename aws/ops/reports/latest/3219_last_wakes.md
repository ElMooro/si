# ops 3219 — expression tiles curated via minus, deposit rates laddered, DVE retired

**Status:** success  
**Duration:** 77.3s  
**Finished:** 2026-07-13T05:50:31+00:00  

## Data

| active_before | active_now | coverage_now | curated_now | dve | n_fails | n_warns | verdict | woken |
|---|---|---|---|---|---|---|---|---|
|  |  |  | 5 | retired (no free source) |  |  |  |  |
|  |  | 74.4 |  |  |  |  |  |  |
| 117 | 117 |  |  |  |  |  |  | 0 |
|  |  |  |  |  | 0 | 1 | PASS |  |

## Log
## 1. Probe-gated curations

- `05:49:15` ✅ TVC:DE10Y-TVC:IT10Y → FRED~IRLTLT01DEM156N~minus~FRED~IRLTLT01ITM156N  (422)
- `05:49:16` ✅ TVC:FR10Y-TVC:IT10Y → FRED~IRLTLT01FRM156N~minus~FRED~IRLTLT01ITM156N  (422)
- `05:49:16` ✅ TVC:ES10Y-TVC:IT10Y → FRED~IRLTLT01ESM156N~minus~FRED~IRLTLT01ITM156N  (422)
- `05:49:17` ✅ ECONOMICS:EUDIR → ECBDFR  (10053)
- `05:49:17` ✅ ECONOMICS:GBDIR → IR3TIB01GBM156N  (433)
## 2. Fleet re-run — wakes by name

- `05:50:31` ⚠ no wakes — read the two engines' fresh reasons

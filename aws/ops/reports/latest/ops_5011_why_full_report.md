# ops 5011 -- why.html full report layer (equity-research v2.5)

**Status:** success  
**Duration:** 137.0s  
**Finished:** 2026-08-27T02:21:43+00:00  

## Data

| aaoi_fv | aaoi_ratio | aaoi_sev | aaoi_strength | aaoi_verdict | gen_s | live_page_marker | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | 159 |
|  |  |  |  |  | 4.4 |  | 2.5 | AAOI |  |
|  |  |  |  |  | 3.6 |  | 2.5 | NVDA |  |
| 12.28 | 9.26 | 5 | 6 | SIGNIFICANTLY OVERVALUED |  |  |  |  |  |
|  |  |  |  |  |  | True |  |  |  |

## Log
## G1 deploy v2.5 (code only)

- `02:19:33` ✅ code updated; configuration/env untouched
## P1 regenerate AAOI+NVDA with real data

- `02:19:34` caches busted for AAOI
- `02:19:34` caches busted for NVDA
- `02:19:42` ✅ AAOI asserted
- `02:19:42` ✅ NVDA asserted
## G2 page markers

- `02:21:43` ✅ OPS 5011 PASS -- fair value, valuation history, strength/profitability, CAGRs, signs, peer returns and profile live on real data

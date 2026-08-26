# ops 5010 -- why.html Quant Layer (equity-research v2.4)

**Status:** success  
**Duration:** 193.1s  
**Finished:** 2026-08-26T23:07:42+00:00  

## Data

| cdn_has_quant | cdn_schema | gen_s | live_page_marker | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 152 |
|  |  | 2.9 |  | 2.4 | AAOI |  |
|  |  | 1.5 |  | 2.4 | NVDA |  |
| True | 2.4 |  |  |  |  |  |
|  |  |  | True |  |  |  |

## Log
## G1 deploy justhodl-equity-research v2.4 (code only)

- `23:04:36` ✅ code updated; configuration/env untouched
## P1 regenerate AAOI+NVDA with real data

- `23:04:36` cache busted for AAOI
- `23:04:36` cache busted for NVDA
- `23:04:41` ✅ AAOI asserted
- `23:04:41` ✅ NVDA asserted
## G2 page markers

- `23:07:42` ⚠ NVDA not blue-chip (5/7) -- review thresholds
- `23:07:42` ✅ OPS 5010 PASS -- classification, base-rate expectations, EMA distance, industry risk & 5y growth live on real data

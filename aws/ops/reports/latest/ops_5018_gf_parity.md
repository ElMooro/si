# ops 5018 -- full GuruFocus summary parity (v2.9)

**Status:** failure  
**Duration:** 12.1s  
**Finished:** 2026-08-27T19:34:36+00:00  

## Error

```
SystemExit: deep asserts failed
```

## Data

| altman | beneish | de_buyb_m | de_div_m | de_iss_m | doc_kb | est_fy | est_rev_m | est_rows | gen_s | geo_regions | geo_top | ladder_methods | ladder_sh_m | news | peer_pts | peer_series | piotroski | risk | roic | schema | sh_yield | ticker | transcripts | wacc | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 180 |
|  |  |  |  |  | 162 |  |  |  | 3.7 |  |  |  |  |  |  |  |  |  |  | 2.9.2 |  | AAOI |  |  |  |
| 12.61(Safe) | -0.59 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 4 |  | None |  |  | AAOI |  | None |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 6 | 80.24 |  |  |  |  |  |  |  |  | AAOI |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 3 | CHINA |  |  |  |  |  |  |  |  |  |  | AAOI |  |  |  |
|  |  |  |  |  |  | 2026-12 | 1048.9 | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  | AAOI |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 121 | 3 |  |  |  |  |  | AAOI |  |  |  |
|  |  | 0.0 | 0.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 | AAOI |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8 |  |  |  | Medium |  |  |  | AAOI | 8 |  |  |
|  |  |  |  |  | 176 |  |  |  | 2.0 |  |  |  |  |  |  |  |  |  |  | 2.9.2 |  | NVDA |  |  |  |
| 71.6(Safe) | -1.13 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 4 |  | None |  |  | NVDA |  | None |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 6 | 24221.0 |  |  |  |  |  |  |  |  | NVDA |  |  |  |
|  |  |  |  |  |  | 2027-01 | 395057.0 | 4 |  |  |  |  |  |  |  |  |  |  |  |  |  | NVDA |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 121 | 3 |  |  |  |  |  | NVDA |  |  |  |
|  |  | 40086.0 | 974.0 | 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.74 | NVDA |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8 |  |  |  | Low |  |  |  | NVDA | 8 |  |  |

## Log
## G0 preflight

- `19:34:23` ✅ v2.9 markers, OPS5018 block, 6 bus subscriptions
## G1 deploy (code only)

- `19:34:30` ✅ code updated; configuration/env untouched
## P1 deep real-data asserts

- `19:34:34` ✅ AAOI: all gf_extras sections checked
- `19:34:36` ✅ NVDA: all gf_extras sections checked
- `19:34:36` ✗ AAOI: wacc missing/insane: None (beta_used=None rf=None)
- `19:34:36` ✗ AAOI: roic missing
- `19:34:36` ✗ AAOI: AAOI risk must assess High (vol/beta/drawdown): got 'Medium'
- `19:34:36` ✗ AAOI: shareholder yield zero/missing
- `19:34:36` ✗ NVDA: wacc missing/insane: None (beta_used=None rf=None)
- `19:34:36` ✗ NVDA: roic missing

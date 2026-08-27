# ops 5018 -- full GuruFocus summary parity (v2.9)

**Status:** failure  
**Duration:** 13.5s  
**Finished:** 2026-08-27T18:16:34+00:00  

## Error

```
SystemExit: deep asserts failed
```

## Data

| altman | beneish | doc_kb | est_fy | est_rev_m | est_rows | gen_s | geo_regions | geo_top | news | peer_pts | peer_series | piotroski | risk | roic | schema | sh_yield | ticker | transcripts | wacc | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 179 |
|  |  | 160 |  |  |  | 4.8 |  |  |  |  |  |  |  |  | 2.9.1 |  | AAOI |  |  |  |
| 163.05(Safe) | -2.77 |  |  |  |  |  |  |  |  |  |  | 6 |  | None |  |  | AAOI |  | None |  |
|  |  |  |  |  |  |  | 3 | CHINA |  |  |  |  |  |  |  |  | AAOI |  |  |  |
|  |  |  | 2025-12 | 452.7 | 4 |  |  |  |  |  |  |  |  |  |  |  | AAOI |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 121 | 3 |  |  |  |  |  | AAOI |  |  |  |
|  |  |  |  |  |  |  |  |  | 8 |  |  |  | Low |  |  | 0.0 | AAOI | 8 |  |  |
|  |  | 173 |  |  |  | 1.9 |  |  |  |  |  |  |  |  | 2.9.1 |  | NVDA |  |  |  |
| 4948.89(Safe) | -2.85 |  |  |  |  |  |  |  |  |  |  | 7 |  | None |  |  | NVDA |  | None |  |
|  |  |  | 2025-01 | 129426.1 | 4 |  |  |  |  |  |  |  |  |  |  |  | NVDA |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 121 | 3 |  |  |  |  |  | NVDA |  |  |  |
|  |  |  |  |  |  |  |  |  | 8 |  |  |  | Low |  |  | 0.0 | NVDA | 8 |  |  |

## Log
## G0 preflight

- `18:16:20` ✅ v2.9 markers, OPS5018 block, 6 bus subscriptions
## G1 deploy (code only)

- `18:16:27` ✅ code updated; configuration/env untouched
## P1 deep real-data asserts

- `18:16:32` ✅ AAOI: all gf_extras sections checked
- `18:16:34` ✅ NVDA: all gf_extras sections checked
- `18:16:34` ⚠ AAOI: AAOI FY-next consensus revenue outside expected band: 453M
- `18:16:34` ✗ AAOI: wacc missing
- `18:16:34` ✗ AAOI: valuation ladder thin
- `18:16:34` ✗ AAOI: fv band thin: not enough history for the anchor
- `18:16:34` ✗ AAOI: AAOI risk must assess High (vol/beta/drawdown): got 'Low'
- `18:16:34` ✗ NVDA: wacc missing
- `18:16:34` ✗ NVDA: valuation ladder thin
- `18:16:34` ✗ NVDA: fv band thin: not enough history for the anchor

# ops 4652 — khalid-five on stock-buying

**Status:** failure  
**Duration:** 230.0s  
**Finished:** 2026-08-13T18:08:04+00:00  

## Error

```
SystemExit: 1
```

## Data

| accelerating | buffett_pass | fn_error | k5_missing | peg_lt_1 | retiring | universe | us10y |
|---|---|---|---|---|---|---|---|
|  |  | None |  |  |  |  |  |
|  |  |  | {"shares_qoq(from_yoy/4)": 498, "peg": 498, "net_issuance": 498, "eps_accel": 498, "rev_accel": 498} |  |  | 0 | 4.7 |
| 0 | 0 |  |  | 0 | 0 |  |  |

## Log
## matrix column-name evidence (five-relevant)

- `18:04:15` ⚠ matrix: 'list' object has no attribute 'keys'
## deploy (ops-side) + settle

- `18:07:40` ✅   [deploy] v1.1.0 live
## run + khalid-five truth

- `18:08:04` ✗   [five-block] CONTRACT MISS — khalid_five on every row
- `18:08:04` ✅   [us10y] US10Y fleet-join = 4.7
- `18:08:04` ✗   [why-link] CONTRACT MISS — why links use house ?ticker= standard
- `18:08:04` ✗   [signal-counts] CONTRACT MISS — peg<1:0 retiring:0 accel:0 buffett:0 (any nonzero proves the wiring; misses counted honestly)
## verdict

- `18:08:04` ✗ khalid-five: 3 red

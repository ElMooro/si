# ops 5018 -- full GuruFocus summary parity (v2.9)

**Status:** failure  
**Duration:** 14.2s  
**Finished:** 2026-08-27T18:11:40+00:00  

## Error

```
SystemExit: deep asserts failed
```

## Data

| doc_kb | gen_s | schema | ticker | zip_kb |
|---|---|---|---|---|
|  |  |  |  | 179 |
| 138 | 4.2 | 2.9 | AAOI |  |
| 150 | 2.8 | 2.9 | NVDA |  |

## Log
## G0 preflight

- `18:11:26` ✅ v2.9 markers, OPS5018 block, 6 bus subscriptions
## G1 deploy (code only)

- `18:11:33` ✅ code updated; configuration/env untouched
## P1 deep real-data asserts

- `18:11:40` ✗ AAOI: gf_extras unavailable
- `18:11:40` ✗ NVDA: gf_extras unavailable

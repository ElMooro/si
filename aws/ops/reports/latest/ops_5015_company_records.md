# ops 5015 -- company records (equity-research v2.7)

**Status:** failure  
**Duration:** 14.3s  
**Finished:** 2026-08-27T15:18:34+00:00  

## Error

```
SystemExit: real-data asserts failed
```

## Data

| days | executives | from_cache | gen_s | listings | lynch_years | member_of | next_earnings | past_events | pos_eps_years | press | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  | 166 |
|  |  | False | 4.0 |  |  |  |  |  |  |  | 2.7.1 | AAOI |  |
| 70 |  |  |  |  |  |  | 2026-11-05 | 8 |  |  |  | AAOI |  |
|  |  |  |  |  | 10 |  |  |  | 2 |  |  | AAOI |  |
|  |  |  |  |  |  |  |  |  |  | 8 |  | AAOI |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  | 2 |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  |  |  | (none) |  |  |  |  |  | AAOI |  |
|  |  | False | 3.2 |  |  |  |  |  |  |  | 2.7.1 | NVDA |  |
| 83 |  |  |  |  |  |  | 2026-11-18 | 8 |  |  |  | NVDA |  |
|  |  |  |  |  | 10 |  |  |  | 10 |  |  | NVDA |  |
|  |  |  |  |  |  |  |  |  |  | 8 |  | NVDA |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  | 3 |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  |  |  | S&P 500,Nasdaq-100,Dow Jones Industrial Average |  |  |  |  |  | NVDA |  |

## Log
## G0 preflight

- `15:18:20` ✅ v2.7 markers and OPS5015 block present
## G1 deploy (code only)

- `15:18:27` ✅ code updated; configuration/env untouched
## P1 real-data asserts (version gate regenerates on its own)

- `15:18:31` ✅ AAOI: company records checked
- `15:18:34` ✅ NVDA: company records checked
- `15:18:34` ✗ AAOI: filings: 0 rows (need 3) -- no parseable filing rows
- `15:18:34` ✗ NVDA: filings: 0 rows (need 3) -- no parseable filing rows

# ops 5015 -- company records (equity-research v2.7)

**Status:** failure  
**Duration:** 15.8s  
**Finished:** 2026-08-27T15:10:26+00:00  

## Error

```
SystemExit: real-data asserts failed
```

## Data

| days | executives | from_cache | gen_s | listings | lynch_years | member_of | next_earnings | past_events | pos_eps_years | press | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  | 165 |
|  |  | False | 5.0 |  |  |  |  |  |  |  | 2.7 | AAOI |  |
| 70 |  |  |  |  |  |  | 2026-11-05 | 8 |  |  |  | AAOI |  |
|  |  |  |  |  | 10 |  |  |  | 2 |  |  | AAOI |  |
|  |  |  |  |  |  |  |  |  |  | 8 |  | AAOI |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  | 2 |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  |  |  | (none) |  |  |  |  |  | AAOI |  |
|  |  | False | 3.7 |  |  |  |  |  |  |  | 2.7 | NVDA |  |
| 83 |  |  |  |  |  |  | 2026-11-18 | 8 |  |  |  | NVDA |  |
|  |  |  |  |  | 10 |  |  |  | 10 |  |  | NVDA |  |
|  |  |  |  |  |  |  |  |  |  | 8 |  | NVDA |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  | 3 |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  |  |  | S&P 500,Nasdaq-100,Dow Jones Industrial Average |  |  |  |  |  | NVDA |  |

## Log
## G0 preflight

- `15:10:11` ✅ v2.7 markers and OPS5015 block present
## G1 deploy (code only)

- `15:10:18` ✅ code updated; configuration/env untouched
## P1 real-data asserts (version gate regenerates on its own)

- `15:10:23` ✅ AAOI: company records checked
- `15:10:26` ✅ NVDA: company records checked
- `15:10:26` ✗ AAOI: filings: 0 rows (need 3) -- SEC filings endpoint returned nothing on this plan tier
- `15:10:26` ✗ NVDA: filings: 0 rows (need 3) -- SEC filings endpoint returned nothing on this plan tier

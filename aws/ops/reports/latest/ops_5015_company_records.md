# ops 5015 -- company records (equity-research v2.7)

**Status:** success  
**Duration:** 12.6s  
**Finished:** 2026-08-27T15:20:25+00:00  

## Data

| days | executives | filings | from_cache | gen_s | listings | lynch_years | member_of | next_earnings | past_events | pos_eps_years | press | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 166 |
|  |  |  | False | 3.7 |  |  |  |  |  |  |  | 2.7.2 | AAOI |  |
| 70 |  |  |  |  |  |  |  | 2026-11-05 | 8 |  |  |  | AAOI |  |
|  |  |  |  |  |  | 10 |  |  |  | 2 |  |  | AAOI |  |
|  |  | 14 |  |  |  |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  |  |  |  |  |  |  |  | 8 |  | AAOI |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  |  | 2 |  |  |  |  |  |  |  | AAOI |  |
|  |  |  |  |  |  |  | (none) |  |  |  |  |  | AAOI |  |
|  |  |  | False | 1.7 |  |  |  |  |  |  |  | 2.7.2 | NVDA |  |
| 83 |  |  |  |  |  |  |  | 2026-11-18 | 8 |  |  |  | NVDA |  |
|  |  |  |  |  |  | 10 |  |  |  | 10 |  |  | NVDA |  |
|  |  | 14 |  |  |  |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  |  |  |  |  |  |  |  | 8 |  | NVDA |  |
|  | 8 |  |  |  |  |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  |  | 3 |  |  |  |  |  |  |  | NVDA |  |
|  |  |  |  |  |  |  | S&P 500,Nasdaq-100,Dow Jones Industrial Average |  |  |  |  |  | NVDA |  |

## Log
## G0 preflight

- `15:20:12` ✅ v2.7 markers and OPS5015 block present
## G1 deploy (code only)

- `15:20:19` ✅ code updated; configuration/env untouched
## P1 real-data asserts (version gate regenerates on its own)

- `15:20:23` ✅ AAOI: company records checked
- `15:20:25` ✅ NVDA: company records checked
## G2 live page carries OPS5015

- `15:20:25` ✅ served page carries the Peter Lynch chart + company records
- `15:20:25` ✅ OPS 5015 PASS -- every GuruFocus summary panel from the screenshots is now built; v2.7 rolls to all tickers via the version gate + in-place auto-upgrade

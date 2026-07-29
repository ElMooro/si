# ops 4079 — is the ECONOMICS tier actually yielding?

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-07-29T05:43:32+00:00  

## Data

| econ_sourced | matched | rate | sourced | tier1_sourced | total | walked | yield_pct |
|---|---|---|---|---|---|---|---|
|  | 94 | 77.2 | 231 |  | 10159 | 10159 | 0.9 |
| 0 |  |  |  | 67 |  |  |  |

## Log
## A. walk progress

- `05:43:31`   sync age     : 2.0 min
- `05:43:31`   walked       : 10159/10159 (100.0%)
- `05:43:31`   tier1_done   : 5082
- `05:43:31`   rate         : 77.2/min  elapsed 7894s
- `05:43:31`   matched      : 94
- `05:43:31`   sc 591/9568  sc2 589/9570  ss 0/50
- `05:43:31`   → live attribution yield: 0.9%
## B. THE question — did ECONOMICS/FRED resolve?

- `05:43:31`   ECONOMICS/FRED rows with a source: 0
- `05:43:31`   ✗ ZERO. Every ECONOMICS/FRED symbol walked so far came back without a publisher. The scanner/symbol-page routes do not carry attribution for TV's macro namespace, so more walking will NOT produce agency rows.
## C. yield by prefix — where attribution actually lives

- `05:43:31`      61  AMEX
- `05:43:31`      47  NASDAQ
- `05:43:31`      35  TVC  ← tier1
- `05:43:31`      32  CBOE  ← tier1
- `05:43:31`      16  CRYPTOCAP
- `05:43:31`       8  NYSE
- `05:43:31`       5  EURONEXT
- `05:43:31`       2  SPCFD
- `05:43:31`       2  TSX
- `05:43:31`       2  OMXCOP
- `05:43:31`       2  ICEUS
- `05:43:31`       1  SIX
- `05:43:31`   tier1 sourced 67 of 5082 tier1 walked
## D. refresh the rollup

- `05:43:32`   {"statusCode": 200, "body": "{\"symbols_with_source\": 196, \"agency_rows\": 0, \"economics_symbols\": 0}"}
- `05:43:32`   agency_rows=0 venue_rows=164 economics_symbols=0
## VERDICT

- `05:43:32` ⛔ STRUCTURALLY DRY: 300+ tier-1 symbols walked, zero ECONOMICS/FRED attribution. The route is the problem, not the ordering. Needs a different extraction path before more walking is worth anything.

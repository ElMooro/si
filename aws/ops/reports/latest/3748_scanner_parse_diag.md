# ops 3748 — scanner 95% failure classification

**Status:** success  
**Duration:** 18.7s  
**Finished:** 2026-07-22T23:41:21+00:00  

## Data

| diagnosis | fetch_fail | not_purchase | purchases | sample |
|---|---|---|---|---|
| NOT_PURCHASE dominates — the 95%% 'failure' is a MISNOMER; m | 0 | 62 | 7 | 80 |

## Log
## A — recent Form 4 filings from the daily index

- `23:41:03` ✅   2026-07-21: 503 Form-4 rows
- `23:41:03`   classifying 80 filings
## B — per-filing classification

## C — results

- `23:41:21`   NOT_PURCHASE     62  (77.5%)  eg=M
- `23:41:21`   NO_TXN           10  (12.5%)  eg=
- `23:41:21`   PURCHASE          7  ( 8.8%)  eg=
- `23:41:21`   NO_NONDERIV       1  ( 1.2%)  eg=
- `23:41:21`   transaction code census: [('S', 38), ('F', 25), ('A', 15), ('M', 11), ('P', 11), ('D', 10), ('J', 5), ('G', 1)]
## VERDICT

- `23:41:21`   DIAGNOSIS: NOT_PURCHASE dominates — the 95%% 'failure' is a MISNOMER; most Form 4s are sells/grants/exercises correctly skipped
- `23:41:21` ✅ DIAG COMPLETE

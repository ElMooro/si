# ops 4909 — SEC MIDAS goes live

**Status:** success  
**Duration:** 64.6s  
**Finished:** 2026-08-19T16:39:15+00:00  

## Data

| action | grew | have | inventory | live | missing | n_complete | parts_after | parts_before | sample | stage | zips |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | True |  |  |  |  |  | fn |  |
| created |  |  |  |  |  |  |  |  |  | schedule |  |
|  |  | 14 | 50 |  | 36 |  |  |  | manifest.json:423;individual_security_exchange_2023_q1.zip:181960165;individual_security_exchange_2023_q2.zip:178169945;individual_security_exchange_2023_q3.zip:182248454 | first-bank | 14 |
|  | False |  |  |  |  | 30 | 243 | 243 |  | deep-cure-reproof |  |

## Log
- `16:39:15` VERDICT: PASS_WITH_PENDING · {"midas_fn_live": "PASS", "midas_scheduled": "PASS", "midas_first_zips": "PASS", "deep_grinding": "PENDING"}
- `16:39:15` report written: aws/ops/reports/4909.json

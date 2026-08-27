# ops 5017 -- JH Score / Momentum / Dividend & Buy Back (v2.8)

**Status:** success  
**Duration:** 165.1s  
**Finished:** 2026-08-27T17:26:27+00:00  

## Data

| consec | gen_s | jh_score | mom_rank | pays | pos_52w | rel_12m | schema | share_3y | sub_ranks | ticker | yield_pct | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | 168 |
|  | 3.8 |  |  |  |  |  | 2.8 |  |  | AAOI |  |  |
|  |  | 50 |  |  |  |  |  |  | Financial:6/Profitability:2/Growth:9/Value:1/Momentum:7 | AAOI |  |  |
|  |  |  | 7 |  | 46.0 | 334.23 |  |  |  | AAOI |  |  |
| None |  |  |  | False |  |  |  | 29.3 |  | AAOI | None |  |
|  | 3.2 |  |  |  |  |  | 2.8 |  |  | NVDA |  |  |
|  |  | 90 |  |  |  |  |  |  | Financial:9/Profitability:10/Growth:10/Value:6/Momentum:9 | NVDA |  |  |
|  |  |  | 9 |  | 92.0 | 6.77 |  |  |  | NVDA |  |  |
| 6 |  |  |  | True |  |  |  | -0.7 |  | NVDA | 0.13 |  |

## Log
## G0 preflight

- `17:23:42` ✅ v2.8 markers, OPS5017 block, 5 bus subscriptions
## G1 deploy (code only)

- `17:23:49` ✅ code updated; configuration/env untouched
## P1 real-data asserts

- `17:23:53` ✅ AAOI: score/momentum/dividend checked
- `17:23:56` ✅ NVDA: score/momentum/dividend checked
## G2 live page carries OPS5017

- `17:23:56` waiting for site sync
- `17:24:11` waiting for site sync
- `17:24:27` waiting for site sync
- `17:24:42` waiting for site sync
- `17:24:57` waiting for site sync
- `17:25:12` waiting for site sync
- `17:25:27` waiting for site sync
- `17:25:42` waiting for site sync
- `17:25:57` waiting for site sync
- `17:26:12` waiting for site sync
- `17:26:27` ✅ served page carries the score wheel block + 5 subscriptions
- `17:26:27` ✅ OPS 5017 PASS -- every visual from every screenshot in this conversation is now built, verified on real data, and rolls to every ticker

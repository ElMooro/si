# ops 3232 — growth pair closed from live families

**Status:** success  
**Duration:** 35.0s  
**Finished:** 2026-07-13T07:02:28+00:00  

## Data

| active_before | active_now | coverage_now | dry_ledgered | n_fails | n_warns | replacements | verdict | woken |
|---|---|---|---|---|---|---|---|---|
|  |  |  | 2 |  |  | 3 |  |  |
|  |  | 74.1 |  |  |  |  |  |  |
| 121 | 121 |  |  |  |  |  |  | 0 |
|  |  |  |  | 0 | 0 |  | PASS |  |

## Log
## 1. Probe-gated replacements

- `07:01:53`   ECONOMICS:EUGDPYY    FRED~CLVMNACSCAB1GQEA19~pct4                         121
- `07:01:54`   ECONOMICS:EUIPYY     Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I21.EA19          0
- `07:01:55`   ECONOMICS:EUBCOI     Eurostat/ei_bssi_m_r2/M.BS-ICI.SA.EA19               0
- `07:01:55`   ECONOMICS:FRUR       Eurostat/une_rt_m/M.SA.TOTAL.PC_ACT.T.FR             431
- `07:01:57`   ECONOMICS:FRBR       ECB/MIR/M.FR.B.A2A.A.R.A.2240.EUR.N                  317
- `07:01:57`   ECONOMICS:FRIPYY     Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I21.FR            0
## 2. Write + fleet — wakes by name

- `07:02:28`   → Europe Growth    DORMANT (needs >=6 members on a free source — map more of i)
- `07:02:28`   → France           DORMANT (needs >=6 members on a free source — map more of i)

# ops 3837 — which condition blocks PROVEN?

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-25T00:50:42+00:00  

## Data

| binding | engines | fdr_pass | n_ok | proven | t_ok |
|---|---|---|---|---|---|
| fdr_pass | 207 | 0 | 179 | 0 | 4 |

## Log
## 1. Read the evidence feed

- `00:50:42` ✅   207 engines · generated_at 2026-07-24T22:30:02.937777+00:00
## 2. Condition-by-condition pass counts

- `00:50:42`   engines total ............ 207
- `00:50:42`   carry a w13 block ........ 195
- `00:50:42`   fdr_pass ................. 0
- `00:50:42`   |t_stat| >= 2 ............ 4
- `00:50:42`   n_effective >= 6 ......... 179
- `00:50:42`   ALL THREE (= proven) ..... 0
- `00:50:42` ⚠   BINDING CONSTRAINT: fdr_pass — only 0/207 pass it
## 3. Closest candidates (nearest to graduating)

- `00:50:42`   engine                    theme         fdr   t_stat   n_eff
- `00:50:42`   Commercial Real Estate    OTHER       False     2.29    23.7
- `00:50:42`   EuroDollar Futures        DOLLAR      False     2.12    60.8
- `00:50:42`   Commercial Banks          OTHER       False     2.07    27.8
- `00:50:42`   European Bonds            CREDIT      False     2.03    82.4
- `00:50:42`   Global 10 year yields     CREDIT      False     1.99    27.2
- `00:50:42`   Commercial banks          OTHER       False     1.83    28.2
- `00:50:42`   Credit Crunch             CREDIT      False     1.67    10.3
- `00:50:42`   Commercial banks          OTHER       False     1.63    28.8
- `00:50:42`   Dollar Shortage Indicator DOLLAR      False    -1.61    19.2
- `00:50:42`   Africa                    OTHER       False      1.6    21.9
- `00:50:42`   Emerging Markets Liquidit LIQUIDITY   False     1.52    10.6
- `00:50:42`   Global Commodities prices INFLATION   False     -1.5    27.7
## 4. Verdict

- `00:50:42` ⚠   FDR correction rejects every panel. Either the panels have no real edge, or the test is applied over too wide a family. Worth reviewing the correction scope — NOT worth disabling.
## 5. Arm a schedule for wl-fusion (ops 3836 found none)

- `00:50:42` ✅   Scheduler armed cron(35 22 * * ? *)
- `00:50:42` ✅ DIAGNOSIS COMPLETE — bar not loosened, schedule armed

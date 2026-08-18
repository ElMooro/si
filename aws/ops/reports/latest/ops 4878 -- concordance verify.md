# G0. banks

**Status:** success  
**Duration:** 163.5s  
**Finished:** 2026-08-18T00:58:04+00:00  

## Log
- `00:55:20` ✅ tic japan bank rows=498
- `00:55:20`   2026-04-01  +14705 $mn
- `00:55:20`   2026-05-01  -2838 $mn
- `00:55:20`   2026-06-01  -3753 $mn
- `00:55:21` ✅ mof bank rows=1127
# 1. settle + invoke

- `00:55:21` ✅ marker settled (attempt 1)
- `00:55:32` ✅ fresh in 10s
# 2. independent recompute

- `00:55:32` ✅ doc == recompute: n=258 sign_agree=63.6% corr_lag0=0.226 leads_1m=0.07
- `00:55:32`   2026-04  MOF -2677.2 JPYbn | TIC +14.71 $bn
- `00:55:32`   2026-05  MOF +4647.5 JPYbn | TIC -2.84 $bn
- `00:55:32`   2026-06  MOF +516.7 JPYbn | TIC -3.75 $bn
# 3. page

- `00:55:32` ✅   committed token
- `00:58:04` ✅   SERVED (151s)
# 4. verdict

- `00:58:04` ✅ weekly carry bid formally joined to the monthly TIC print -- stats honest, independently recomputed

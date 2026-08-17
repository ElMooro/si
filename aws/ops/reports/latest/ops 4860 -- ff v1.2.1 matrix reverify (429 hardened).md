# 1. settle + invoke

**Status:** success  
**Duration:** 114.5s  
**Finished:** 2026-08-17T22:22:09+00:00  

## Log
- `22:20:25` ✅ marker settled (attempt 2)
- `22:21:38` ✅ fresh in 72s
- `22:21:38`   30s quota-window breather before sampling
# 2. matrix truths

- `22:22:08` ✅   21 countries, 21 OK
- `22:22:08` ✅   ordered by holdings desc (top: japan 1046B)
- `22:22:09` ✅   INDIA 170.5B @ 2026-06-01 == FRED refetch
- `22:22:09` ✅   identity gaps on 21 countries
- `22:22:09` ✅   equity block 21 OK; JAPAN eq 1400.4B == refetch
- `22:22:09` ✅   bank exists: FORLTTREASPOS42102
- `22:22:09` ✅   bank exists: FORLTEQTYPOS42609
- `22:22:09`   readout top6: japan 1046 (tx12m 16.8) | united_kingdom 836 (tx12m 150.7) | china 589 (tx12m -91.3) | canada 422 (tx12m 25.8) | france 373 (tx12m -0.9) | taiwan 298 (tx12m -3.9)
# 3. page

- `22:22:09` ✅   committed equity tokens present
- `22:22:09` ✅   SERVED (0s)
# 4. verdict

- `22:22:09` ✅ 21-country matrix + equity holdings LIVE, sampled against FRED, banked, served

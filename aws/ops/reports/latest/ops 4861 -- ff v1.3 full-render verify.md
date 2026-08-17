# 1. settle + invoke

**Status:** failure  
**Duration:** 179.1s  
**Finished:** 2026-08-17T22:32:28+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:29:29` ✅ marker settled (attempt 1)
- `22:31:55` ✅ fresh in 145s
- `22:31:55`   30s quota-window breather before sampling
# 2. matrix truths

- `22:32:25` ✅   21 countries, 21 OK
- `22:32:25` ✅   ordered by holdings desc (top: japan 1046B)
- `22:32:25` ✅   INDIA 170.5B @ 2026-06-01 == FRED refetch
- `22:32:25` ✅   identity gaps on 21 countries
- `22:32:26` ✅   equity block 21 OK; JAPAN eq 1400.4B == refetch
- `22:32:26` ✅   bank exists: FORLTTREASPOS42102
- `22:32:26` ✅   bank exists: FORLTEQTYPOS42609
- `22:32:26` ✗   hist_10y broken: keys=[]
- `22:32:26` ✅   holder splits: 6 families, 6 OK
- `22:32:26` ✗   tx_3m on 0 countries
- `22:32:26`   readout top6: japan 1046 (tx12m 16.8) | united_kingdom 836 (tx12m 150.7) | china 589 (tx12m -91.3) | canada 422 (tx12m 25.8) | france 373 (tx12m -0.9) | taiwan 298 (tx12m -3.9)
# 3. page

- `22:32:26` ✅   committed equity tokens present
- `22:32:28` ✅   SERVED (2s)
# 4. verdict

- `22:32:28` ✗ HARD FAILS: ['hist', 't3']

# ops 3957 — gov-sources v2.0 FULL-PULL + FRED env fix

**Status:** failure  
**Duration:** 129.4s  
**Finished:** 2026-07-27T03:45:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_live | n_series_pulled | per_agency |
|---|---|---|
| 10 | 57 | {'boj': 1, 'mof_japan': 15, 'us_treasury': 14, 'eurostat': 7, 'norges': 7, 'bcrp': 2, 'ecb': 4, 'boe': 4, 'snb': 1, 'imf': 1, 'fred': 1, 'bcch': 0, 'gov_proxy': 0} |

## Log
## env fix: copy FRED_KEY/FMP_KEY from donor

- `03:43:20`   donor has: ['FMP_KEY', 'FRED_KEY']; fn had FRED_KEY: False
- `03:43:27` ✅   env updated
## settle v2.0 (marker + full-pull strings in zip)

- `03:43:28` ✅   settled attempt 1
## invoke + gates

- `03:43:49` ✅   artifact ~20s
- `03:43:49`   FRED: LIVE US 10Y (DGS10)=4.71 @ 2026-07-23
## served page (new KPI)

- `03:45:29` ✅   served attempt 6
- `03:45:29` ✅   FRED_KEY on function
- `03:45:29` ✅   v2.0 settled
- `03:45:29` ✅   artifact written
- `03:45:29` ✅   FRED LIVE with real DGS10
- `03:45:29` ✅   n_series_pulled >= 45
- `03:45:29` ✅   us_treasury >= 12 series
- `03:45:29` ✅   mof_japan >= 14 series
- `03:45:29` ✅   boe >= 4 series
- `03:45:29` ✅   ecb >= 3 series
- `03:45:29` ✅   norges >= 4 series
- `03:45:29` ✅   eurostat >= 6 series
- `03:45:29` ✗   boj >= 2 series
- `03:45:29` ✅   13 agencies
- `03:45:29` ✅   page served with v2 markers
- `03:45:29` ✗ FAILED: ['boj >= 2 series']

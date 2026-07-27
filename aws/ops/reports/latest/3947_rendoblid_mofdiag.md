# ops 3947 — rendoblid switch + MOF self-diagnosis + IMF unkeyed

**Status:** failure  
**Duration:** 553.6s  
**Finished:** 2026-07-27T02:06:45+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 80.6 | 452 | {'META': 1, 'LIVE': 452, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 106} |

## Log
## IMF unkeyed pull — learn real series attributes inline

- `01:57:32`   FCL?lastNObservations=1&startPeriod=2026: 299999b Obs=YES
- `01:57:32` ✅   FIRST OBS ATTRS: <Obs TIME_PERIOD="2026-M06" OBS_VALUE="6835420052.85176" DERIVATION_TYPE="O"/>
- `01:57:32` ✅   settled attempt 1
- `02:06:45` ✅   refreshed ~540s
## MOF self-diagnosis from inside the Lambda

- `02:06:45`   https://www.mof.go.jp/english/policy/jgbs/reference/interest -> OK 21 lines
- `02:06:45`   JP02Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `02:06:45`   CH02Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `02:06:45`   CH03Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `02:06:45` ✅   v3.5.2 settled (strings in zip)
- `02:06:45` ✅   force run wrote
- `02:06:45` ✅   debug_mof captured
- `02:06:45` ✗   CH02Y LIVE + 2026 asof via rendoblid
- `02:06:45` ✗   n_live >= 455
- `02:06:45` ✅   zero bare UNRESOLVED
- `02:06:45` ✗ FAILED: ['CH02Y LIVE + 2026 asof via rendoblid', 'n_live >= 455']

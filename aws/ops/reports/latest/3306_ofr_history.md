## 1. Deploy + run ofr-stfm v1.3

**Status:** failure  
**Duration:** 78.0s  
**Finished:** 2026-07-14T18:20:16+00:00  

## Error

```
SystemExit: FAILS: fails history start 2015-01-07 (expected early 1990s)
```

## Data

| fails | fails_chart_pts | fails_chart_start | fails_cross_start | fsi_chart_pts | fsi_components | fsi_latest | fsi_n | fsi_pctile_full | fsi_start | ofr_page_markers | shard_has_fsi_components | shard_n_series | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'ftd_tot': '2015-01-07', 'ftr_tot': '2015-01-07'} |  | {'credit': -1.199, 'equity_valuation': -0.588, 'funding': -0.088, 'safe_assets': -0.321, 'volatility': -0.315} | -2.512 | 6712 | 27.9 | 2000-01-03 |  |  |  | 1.3.1 |  |
|  | 539 | 2015-01-07 |  | 605 |  |  |  |  |  |  | 5 | 210 |  |  |
|  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |
| ['fails history start 2015-01-07 (expected early 1990s)'] |  |  |  |  |  |  |  |  |  |  |  |  |  | [] |

## Log
- `18:18:58`   zip: 77609 bytes
## 1. Lambda

- `18:18:58`   Lambda exists — updating
- `18:19:03` ✅   ✓ updated justhodl-ofr-stfm
## 2. Verify FSI block

## 3. Verify charts shard

## 3b. Runner-side probe: fails depth ground truth

- `18:19:25` probe mnemonic=NYPD-PD_AFtD_TOT-A&start_date=1990-01-01 -> 600 pts, first=2015-01-07
- `18:19:25` probe mnemonic=NYPD-PD_AFtD_TOT-A -> 600 pts, first=2015-01-07
## 4. Live page markers


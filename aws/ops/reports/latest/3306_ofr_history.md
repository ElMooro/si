## 1. Deploy + run ofr-stfm v1.3

**Status:** success  
**Duration:** 88.9s  
**Finished:** 2026-07-14T18:24:07+00:00  

## Data

| fails | fails_chart_pts | fails_chart_start | fails_cross_start | fsi_chart_pts | fsi_components | fsi_latest | fsi_n | fsi_pctile_full | fsi_start | nyfed_ustet_first | nyfed_ustet_n | ofr_page_markers | sf_deep_pts | sf_deep_start | shard_has_fsi_components | shard_n_series | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'ftd_tot': '2015-01-07', 'ftr_tot': '2015-01-07'} |  | {'credit': -1.199, 'equity_valuation': -0.588, 'funding': -0.088, 'safe_assets': -0.321, 'volatility': -0.315} | -2.512 | 6712 | 27.9 | 2000-01-03 |  |  |  |  |  |  |  | 1.3.1 |  |
|  | 539 | 2015-01-07 |  | 605 |  |  |  |  |  |  |  |  |  |  | 5 | 210 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 160 | 2013-04-03 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 2013-04-03 | 692 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |
| [] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [] |

## Log
- `18:22:39`   zip: 77609 bytes
## 1. Lambda

- `18:22:39`   Lambda exists — updating
- `18:22:44` ✅   ✓ updated justhodl-ofr-stfm
## 2. Verify FSI block

## 3. Verify charts shard

## 3a2. settlement-fails deep history (primary source)

- `18:23:05`   zip: 75653 bytes
## 1. Lambda

- `18:23:05`   Lambda exists — updating
- `18:23:08` ✅   ✓ updated justhodl-settlement-fails
- `18:23:08` ✅   ✓ Function URL: https://snnbusv66bzwt2aacjdcnpd57u0upihr.lambda-url.us-east-1.on.aws/
## 3b. Runner-side probe: fails depth ground truth

- `18:23:17` probe mnemonic=NYPD-PD_AFtD_TOT-A&start_date=1990-01-01 -> 600 pts, first=2015-01-07
- `18:23:17` probe mnemonic=NYPD-PD_AFtD_TOT-A -> 600 pts, first=2015-01-07
## 4. Live page markers

- `18:24:07` OPS 3306 PASS — FSI charted since 2000-01-03, fails since 2015-01-07, 210 series clickable-to-chart.

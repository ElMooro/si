## 1. Deploy nyfed-pd v2 (+TELEGRAM tripwire env)

**Status:** success  
**Duration:** 156.9s  
**Finished:** 2026-07-14T16:20:26+00:00  

## Data

| all_time_min | as_of | buckets_latest | claim | corp_series_bucketed | corp_series_discovered | corp_unbucketed | cp_b | credit_stress_fresh | dealer_positioning | fails | hist_start | hist_weeks | ledger_classes | live_page | net_5yplus_b | net_bonds_b | net_under5y_b | pctile_history | prior_negative_date | regime | version | warns | ytd_avg_b | z_52w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'cp': -7.4, 'y10p': -8.61, 'm13m_5y': 3.31, 'y5_10': -5.08, 'u13m': 6.5} |  | 17 | 18 | [] |  |  |  |  |  |  | ['ABS', 'AGENCY_DEBT', 'AGENCY_MBS', 'MUNIS', 'TREASURY_EXTIPS'] |  |  |  |  |  |  |  | 2.0.0 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 2013-04-03 | 692 |  |  |  |  |  |  |  |  |  |  |  |  |
| (-19.74, '2019-05-08') | 2026-07-01 |  | net~-4B ytd-avg; 5y+~-13.7B; <5y~+9.66B; first since 1998 |  |  |  | -7.4 |  |  |  |  |  |  |  | -13.68 | -3.87 | 9.81 | 3.8 | 2026-06-17 | NET_SHORT |  |  | -3.85 | -0.42 |
|  |  |  |  |  |  |  |  | True | {'net_bonds_b': -3.87, 'regime': 'NET_SHORT', 'net_5yplus_b': -13.68, 'net_under5y_b': 9.81, 'as_of': '2026-07-01', 'z_52w': -0.42, 'squeeze_setup': True, 'read': "Dealers are NET SHORT $3.9B of corporate bonds (net short again in available history since 2013) — 5y+ book -13.7B vs short-end +9.8B. The market's shock absorber is gone; a rally forces covering into supply that pension/insurance holders rarely sell."} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | [] |  |  |  |  |  |  |  |  |  |  |  | [] |  |  |

## Log
- `16:17:49` telegram env copied from dollar-radar
- `16:17:49`   zip: 79022 bytes
## 1. Lambda

- `16:17:50`   Lambda exists — updating
- `16:17:55` ✅   ✓ updated justhodl-nyfed-pd
- `16:17:55` ✅   ✓ Function URL: https://wr7n27ssgahakrkbjbcxmnxwce0prtjk.lambda-url.us-east-1.on.aws/
## 2. Poll fresh output + corporate integrity

## 3. THE HEADLINE CHECK — Fed file vs Bloomberg/Crisil claim

## 4. Credit engine join

- `16:19:10`   zip: 77769 bytes
## 1. Lambda

- `16:19:11`   Lambda exists — updating
- `16:19:14` ✅   ✓ updated justhodl-credit-stress
## 5. Page markers

- `16:20:26` OPS 3301 PASS — dealers have their own desk: net corp -3.87B (NET_SHORT), 5y+ -13.68B / <5y 9.81B, wired into credit-stress + Telegram tripwire armed.

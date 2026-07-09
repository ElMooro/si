## 0. Wait for this push's deploys to land

**Status:** failure  
**Duration:** 111.6s  
**Finished:** 2026-07-09T18:24:16+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | canary_grid_code_age_min | code_age_min | early_warning | eem_rvol | fred_probe | hyg_lqd | n_available | n_fails | n_signals | n_warns | new_markets_present | oil_available | oil_term | resolution_table | risk_ratios_exists | rr_n_live | sub_grids | verdict | warroom_macro_watched | warroom_score | warroom_total_watched |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 0.1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | 10.2 |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |
|  |  |  |  |  | {"TRESEGCNM052N": "3 rows, latest 2026-04-01=3477516.923138242", "TRESEGUSM052N": "3 rows, latest 2026-03-01=241283.1165493934", "TRESEGJPM052N": "3 rows, latest 2026-05-01=1182401.011933803", "TRESEGCHM052N": "DEAD: HTTP Error 400: Bad Request", "TRESEGEZM052N": "3 rows, latest 2018-04-01=362495673509.383", "TRESEGXMM052N": "DEAD: HTTP Error 400: Bad Request", "IGREA": "3 rows, latest 2026-04-01=32.720189", "IRLTLT01ITM156N": "3 rows, latest 2026-04-01=3.818", "IRLTLT01DEM156N": "3 rows, latest 2026-05-01=3.0465"} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 36.88 |  | 0.73952 |  |  |  |  |  | True | 0.12 |  |  | 7 |  |  |  |  |  |
| WATCH |  |  | 39.6 |  |  |  | 49 |  | 55 |  |  |  |  |  |  |  | ['commodity_cycle', 'funding_plumbing', 'global_risk', 'labor_industrial', 'rates_credit', 'trade_shipping'] |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"repo_sofr_iorb": "LIVE -0.07ppt", "rrp_parking": "LIVE 5.6$bn 13w", "bank_reserves": "LIVE -8.03%52wk", "bill4w_floor": "LIVE -0.06ppt", "hyg_lqd": "LIVE 0.97%3m", "hy_etf": "LIVE -0.44%3m", "acwi_tape": "LIVE 8.72%3m", "em_vol": "LIVE 36.88% ann.", "euro_hy_oas": "LIVE 2.58ppt", "em_corp_oas": "LIVE 1.43ppt", "btp_bund": "DEAD Noneppt", "global_metals": "LIVE 31.91%YoY", "chile_tot_proxy": "LIVE -14.7%YoY", "peru_tot_proxy": "LIVE -20.46%YoY", "core_capex_orders": "LIVE 10.39%YoY", "mfg_capacity": "LIVE 75.7%", "mfg_employment": "LIVE -0.3%YoY", "igrea_global": "DEAD Noneindex", "cp3m_ff": "LIVE 0.09ppt", "global_fx_reserves": "DEAD None%12m", "euribor3m": "LIVE 0.29ppt 6m", "eu_curve_30_5": "LIVE 0.84ppt", "us_hy_ytw": "LIVE 7.09%", "em_hy_ytw": "LIVE 7.4%", "em_hy_oas": "LIVE 3.06ppt", "oil_term": "LIVE 0.12$/bbl", "fallen_angels_rs": "LIVE 0.37%3m", "global_consumer": "LIVE 1.23%3m"} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | ['EM Small Cap', 'Pacific (VPL)', 'Global Consumer Disc.'] |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 49 | None | 337 |
|  |  |  |  |  |  |  |  | 2 |  | 1 |  |  |  |  |  |  |  | FAIL |  |  |  |

## Log
## 0.5 FRED id probe (runner-side, definitive)

## 1. Risk-ratios engine

## 2. Canary grid v2 regeneration

## 3. Leading markets +3

## 4. War room aggregation

## verdict

- `18:24:16` FAIL: btp_bund canary unavailable (HARD)
- `18:24:16` FAIL: igrea_global canary unavailable (HARD)

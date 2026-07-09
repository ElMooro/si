## 0. Wait for deploys

**Status:** failure  
**Duration:** 1058.6s  
**Finished:** 2026-07-09T19:32:37+00:00  

## Error

```
SystemExit: 1
```

## Data

| band | barometer | bkln_hyg | cftc_row | code_ages_min | copper_gold | factor_row | invoke_result | mechanisms | move_vix | n_canaries | n_ciss_rows | n_fails | n_signals | n_votes | n_warns | page_everything_tab | page_gauge | smh_acwi | v3_table | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | {'justhodl-canary-warroom': 0.1, 'justhodl-signal-board': 'STALE', 'justhodl-morning-intelligence': 'STALE'} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | LIVE 0.25624 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | LIVE 0.09987 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | LIVE 3.87605 |  |  |
|  |  |  |  |  |  |  |  |  | LIVE 4.05 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 63 |  |  |  |  |  | {"discount_window": "LIVE 7778.0$mn", "fin_cp_bill": "LIVE 0.07ppt", "bbb_aaa": "LIVE 0.56ppt", "curve_velocity": "LIVE -0.17ppt 3m", "bkln_hyg": "LIVE -0.25%3m", "copper_gold": "LIVE 23.11%3m", "smh_acwi": "LIVE 32.54%3m", "move_vix": "LIVE 4.05ratio"} |  |
|  |  |  |  |  |  |  | 32.4 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| WATCH | 32.4 |  | False |  |  | True |  | ['macro_grid', 'funding', 'leading_markets', 'dollar', 'vol', 'ciss', 'factor_regime', 'cftc', 'alerts'] |  | 155 | 40 |  |  | 177 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 1 |  |  | 0 |  |  |  |  | FAIL |

## Log
## 1. Risk-ratios v3 (4 new metrics)

## 2. Grid v3 regeneration (+8 canaries)

## 2b. Warroom v3 (9 mechanisms + barometer)

## 3. Live page checks (CDN lag = warn-level)

- `19:32:37` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `19:32:37` FAIL: cftc canary missing

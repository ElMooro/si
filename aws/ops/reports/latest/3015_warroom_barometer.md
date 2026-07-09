## 0. Wait for deploys

**Status:** success  
**Duration:** 75.4s  
**Finished:** 2026-07-09T19:01:58+00:00  

## Data

| barometer_band | barometer_score | calm_rows_per_mechanism | code_ages_min | family_rows | invoke_result | n_all_canaries | n_fails | n_votes | n_warns | page_everything_tab | page_gauge | per_mechanism | signal_board_has_warroom | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'justhodl-canary-warroom': 0.1, 'justhodl-signal-board': 0.1, 'justhodl-morning-intelligence': 0.4} |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | {"ok": true, "barometer": 34.6, "master_ew": 24.7, "n_firing": 47, "n_divergences": 1} |  |  |  |  |  |  |  |  |  |
| WATCH | 34.6 | {"macro_grid": 32, "funding": 7, "leading_markets": 8, "dollar": 7, "vol": 5} |  | 7 |  | 106 |  | 128 |  |  |  | {"macro_grid": 52, "funding": 9, "leading_markets": 22, "dollar": 15, "vol": 8} |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |
|  |  |  |  |  |  |  |  |  |  | False | False |  |  |  |
|  |  |  |  |  |  |  | 0 |  | 1 |  |  |  |  | PASS |

## Log
## 1. Warroom regeneration + barometer

## 2. Signal-board fusion

## 3. Live page checks (CDN lag = warn-level)

- `19:01:58` morning-intelligence wiring verified by deploy + compile; prompt line lands in tomorrow's 8AM brief (not invoked here -- LLM cost discipline).
## verdict

- `19:01:58` PASS -- barometer 34.6 (WATCH) over 128 equal votes; full inventory live

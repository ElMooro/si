## 1. Ensure engine exists + settled

**Status:** success  
**Duration:** 26.8s  
**Finished:** 2026-07-09T23:50:22+00:00  

## Data

| earned_band5 | earned_score | fn_exists | invoke | learned_detail | n_fails | n_learned | n_mechanisms | n_warns | page_earned_toggle | pm_score | schedule | verdict | warroom_invoke | weighted_mechs | weights | weights_asof |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | {"ok": true, "n_learned": 8, "weights": {"macro_grid": 0.719, "funding": 0.974, "vol": 1.222, "dollar": 0.989, "ciss": 1.0, "global_stress": 1.298, "leading_markets": 1.116, "plumbing": 0.784, "eurodollar": 0.899, "factor_regime": 1.0, "cftc": 1.0, "alerts": 1.0}} |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | {"macro_grid": {"hit": 0.125, "lead": 3.0, "fa": 0.069, "n": 8}, "funding": {"hit": 0.625, "lead": 4.4, "fa": 0.28, "n": 8}, "vol": {"hit": 0.875, "lead": 4.9, "fa": 0.177, "n": 8}, "dollar": {"hit": 0.625, "lead": 5.0, "fa": 0.442, "n": 8}, "global_stress": {"hit": 0.8, "lead": 3.0, "fa": 0.127, "n": 5}, "leading_markets": {"hit": 0.8, "lead": 6.0, "fa": 0.6, "n": 5}, "plumbing": {"hit": 0.25, "lead": 5.0, "fa": 0.186, "n": 8}, "eurodollar": {"hit": 0.5, "lead": 4.3, "fa": 0.336, "n": 6}} |  | 8 | 12 |  |  |  |  |  |  |  | {"macro_grid": 0.719, "funding": 0.974, "vol": 1.222, "dollar": 0.989, "ciss": 1.0, "global_stress": 1.298, "leading_markets": 1.116, "plumbing": 0.784, "eurodollar": 0.899, "factor_regime": 1.0, "cftc": 1.0, "alerts": 1.0} |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | {"ok": true, "barometer": 36.6, "per_mechanism": 40.5, "earned": 40.1, "master_ew": 23.9, "n_firing": 108, "n_divergences": 1} |  |  |  |
| ELEVATED | 40.1 |  |  |  |  |  |  |  |  | 40.5 |  |  |  | {"macro_grid": 0.719, "funding": 0.974, "leading_markets": 1.116, "dollar": 0.989, "vol": 1.222, "ciss": 1.0, "factor_regime": 1.0, "cftc": 1.0, "global_stress": 1.298, "plumbing": 0.784, "eurodollar": 0.899, "alerts": 1.0} |  | 2026-07-09T23:50:08.488555+00:00 |
|  |  |  |  |  |  |  |  |  |  |  | created cron(10 7 1 * ? *) |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |
|  |  |  |  |  | 0 |  |  | 0 |  |  |  | PASS |  |  |  |  |

## Log
## 2. Event study run

## 3. Warroom earned view

## 4. Monthly schedule (EventBridge Scheduler)

## 5. Live page (warn-level)

## verdict

- `23:50:22` PASS -- earned 40.1 vs per-mech 40.5; 8 learned

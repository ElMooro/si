## 1. Ensure engine exists + settled

**Status:** failure  
**Duration:** 621.5s  
**Finished:** 2026-07-10T00:33:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| ea_reserves | earned_band5 | earned_score | fn_exists | invoke | learned_detail | n_fails | n_learned | n_mechanisms | n_warns | page_earned_toggle | pm_score | probes | schedule | swiss_reserves | verdict | warroom_invoke | weighted_mechs | weights | weights_asof |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | {"IMF/IFS/M.CH.RAFA_USD": "1 docs, latest 2025-06", "IMF/IRFCL/M.CH.RAFA_USD": "1 docs, latest 2025-07", "IMF/IFS/M.U2.RAFA_USD": "1 docs, latest 2025-06", "IMF/IRFCL/M.U2.RAFA_USD": "2 docs, latest 2025-07", "https://api.truflation.com/current?format=json": "ERR HTTP Error 404: Not Found", "https://api.truflation.com/index/current": "ERR HTTP Error 404: Not Found", "https://truflation.com/api/us-inflation": "ERR HTTP Error 404: Not Found"} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | LIVE 15.31 |  |  |  |  |  |
| LIVE 26.3 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | {"ok": true, "n_learned": 11, "weights": {"macro_grid": 0.709, "funding": 0.954, "vol": 1.19, "dollar": 0.968, "ciss": 1.323, "global_stress": 1.264, "leading_markets": 1.089, "plumbing": 0.772, "eurodollar": 0.882, "factor_regime": 0.975, "cftc": 0.874, "alerts": 1.0}} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | {"macro_grid": {"hit": 0.125, "lead": 3.0, "fa": 0.069, "n": 8}, "funding": {"hit": 0.625, "lead": 4.4, "fa": 0.28, "n": 8}, "vol": {"hit": 0.875, "lead": 4.9, "fa": 0.177, "n": 8}, "dollar": {"hit": 0.625, "lead": 5.0, "fa": 0.442, "n": 8}, "ciss": {"hit": 0.667, "lead": 4.5, "fa": 0.077, "n": 6}, "global_stress": {"hit": 0.8, "lead": 3.0, "fa": 0.127, "n": 5}, "leading_markets": {"hit": 0.8, "lead": 6.0, "fa": 0.6, "n": 5}, "plumbing": {"hit": 0.25, "lead": 5.0, "fa": 0.186, "n": 8}, "eurodollar": {"hit": 0.5, "lead": 4.3, "fa": 0.336, "n": 6}, "factor_regime": {"hit": 0.4, "lead": 5.5, "fa": 0.127, "n": 5}, "cftc": {"hit": 0.375, "lead": 3.7, "fa": 0.151, "n": 8}} |  | 11 | 12 |  |  |  |  |  |  |  |  |  | {"macro_grid": 0.709, "funding": 0.954, "vol": 1.19, "dollar": 0.968, "ciss": 1.323, "global_stress": 1.264, "leading_markets": 1.089, "plumbing": 0.772, "eurodollar": 0.882, "factor_regime": 0.975, "cftc": 0.874, "alerts": 1.0} |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"ok": true, "barometer": 36.5, "per_mechanism": 40.5, "earned": 39.3, "master_ew": 23.7, "n_firing": 106, "n_divergences": 1} |  |  |  |
|  | GUARDED | 39.3 |  |  |  |  |  |  |  |  | 40.5 |  |  |  |  |  | {"macro_grid": 0.709, "funding": 0.954, "leading_markets": 1.089, "dollar": 0.968, "vol": 1.19, "ciss": 1.323, "factor_regime": 0.975, "cftc": 0.874, "global_stress": 1.264, "plumbing": 0.772, "eurodollar": 0.882, "alerts": 1.0} |  | 2026-07-10T00:25:08.430992+00:00 |
|  |  |  |  |  |  |  |  |  |  |  |  |  | exists |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 1 |  |  | 0 |  |  |  |  |  | FAIL |  |  |  |  |

## Log
## 1.5 Probes (DBnomics reserves + Truflation M21)

## 1.7 Grid v3.1 regeneration (Swiss+EA legs, SOFT)

## 2. Event study run

## 3. Warroom earned view

## 4. Monthly schedule (EventBridge Scheduler)

## 5. Live page (warn-level)

## verdict

- `00:33:18` FAIL: warroom code not settled

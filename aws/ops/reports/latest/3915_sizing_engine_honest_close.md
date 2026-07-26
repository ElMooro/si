# ops 3915 — sizing-engine wire: observable, or honestly unobservable (no recs)

**Status:** failure  
**Duration:** 0.1s  
**Finished:** 2026-07-26T19:03:09+00:00  

## Error

```
SystemExit: 1
```

## Data

| gate_fields_present | gross | n_recommendations | ratio |
|---|---|---|---|
|  | 123.0 | 30 |  |
| False |  |  | None |

## Log
- `19:03:09`   sample rec: {"ticker": "CMCSA", "direction": "LONG", "engine": "eng:gf-value", "engine_gate": "OK", "claimed_conf": 0.9, "calibrator_scale": 1.0, "chain": {"quarter_kelly_w": 7.5, "conf_adj_x": 1.27, "vol_ann_pct": 39.5, "vol_scalar_x": 0.76, "cluster_corr_max": 0.0, "haircut_x": 1.0}, "spy_corr": 0.07, "final_
- `19:03:09` ✗ recs exist but gate not applied

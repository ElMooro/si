# ops 3916 — sizing-engine clean re-invoke with full settle discipline

**Status:** success  
**Duration:** 20.6s  
**Finished:** 2026-07-26T19:07:28+00:00  

## Data

| gate_fields | last_update | n_recs | ratio | state |
|---|---|---|---|---|
|  | Successful |  |  | Active |
|  |  | 30 |  |  |
| True |  |  | 0.45 |  |

## Log
- `19:07:28`   sample: {"ticker": "CMCSA", "direction": "LONG", "engine": "eng:gf-value", "engine_gate": "OK", "claimed_conf": 0.9, "calibrator_scale": 1.0, "chain": {"quarter_kelly_w": 7.5, "conf_adj_x": 1.27, "vol_ann_pct": 39.5, "vol_scalar_x": 0.76, "cluster_corr_max": 0.0, "haircut_x": 1.0}, "spy_corr": 0.07, "final_w_pct": 2.25, "risk_gate_posture": "RISK_OFF", "pre_gate_w_pct": 5.0, "dollars_per_100k": 5000, "overlap_flags": [], "ba
- `19:07:28` ✅ PASS_ALL — sizing-engine gated live: CMCSA pre_gate 5.0% -> final 2.25% (RISK_OFF)

# 1) Wait + redeploy

**Status:** success  
**Duration:** 9.2s  
**Finished:** 2026-05-05T10:40:17+00:00  

## Log
- `10:40:13` ✅   ✓ deployed, mod=2026-05-05T10:40:09.000+0000
# 2) Invoke calibrator

- `10:40:17`   status: 200, duration: 3.7s
- `10:40:17`   total_outcomes: 1620
- `10:40:17`   n_horizon_lift: 6
- `10:40:17` 
- `10:40:17`   Genuine horizon-uplifts (n>=5 at best horizon):
- `10:40:17`     edge_composite                flat=0.51 → day_1: w=1.29  Δ+0.79
- `10:40:17`     crypto_fear_greed             flat=0.86 → day_14: w=1.44  Δ+0.58
- `10:40:17`     plumbing_stress               flat=0.99 → day_14: w=1.41  Δ+0.42
- `10:40:17`     crisis_hy_oas_vs_hyg          flat=0.96 → day_3: w=1.22  Δ+0.26
- `10:40:17`     momentum_spy                  flat=1.06 → day_7: w=1.31  Δ+0.25
- `10:40:17`     ml_risk                       flat=1.30 → day_30: w=1.45  Δ+0.15
# 3) Inspect calibration JSON for clean uplifts

- `10:40:17`   signals with measured horizons (n>=5): 21/27
- `10:40:17` 
- `10:40:17`   Verified uplifts (≥0.15 + n>=5):
- `10:40:17`     edge_composite                flat=0.51 → day_1: w=1.29  acc=80% n=44  Δ+0.78
- `10:40:17`     crypto_fear_greed             flat=0.86 → day_14: w=1.44  acc=97% n=37  Δ+0.58
- `10:40:17`     plumbing_stress               flat=0.99 → day_14: w=1.41  acc=92% n=25  Δ+0.42
- `10:40:17`     crisis_hy_oas_vs_hyg          flat=0.96 → day_3: w=1.22  acc=75% n=20  Δ+0.26
- `10:40:17`     momentum_spy                  flat=1.06 → day_7: w=1.31  acc=100% n=8  Δ+0.25
- `10:40:17`     ml_risk                       flat=1.30 → day_30: w=1.45  acc=100% n=30  Δ+0.15

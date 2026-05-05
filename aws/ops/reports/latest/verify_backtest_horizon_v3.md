# 1) Force redeploy with avg weight fix

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-05-05T11:39:27+00:00  

## Log
- `11:39:17`   zip size: 7,316b
- `11:39:19` ✅   ✓ deployed, mod=2026-05-05T11:39:17.000+0000
# 2) Invoke with avg weight fix

- `11:39:22`   status: 200, duration: 2.9s
- `11:39:22`   total_return_pct: 70.7373%
- `11:39:22`   alpha_vs_spy_pct: 61.5148%
- `11:39:22`   n_horizon_weighted: 1317
# 3) Signal-level avg weights + windows_used

- `11:39:22`   Top contributors with horizon mix:
- `11:39:22`     screener_top_pick             avg_w=1.338  n= 555                           [day_30=555]
- `11:39:22`     ml_risk                       avg_w=1.164  n=  75                           [day_14=25, day_30=30, day_7=20]
- `11:39:22`     carry_risk                    avg_w=1.453  n=  30                           [day_30=30]
- `11:39:22`     crypto_fear_greed             avg_w=1.072  n=  84                           [day_1=7, day_14=37, day_3=16, day_7=24]
- `11:39:22`     plumbing_stress               avg_w=0.998  n=  99                           [day_1=24, day_14=25, day_30=30, day_7=20]
- `11:39:22`     momentum_spy                  avg_w=1.183  n=  10                           [day_3=2, day_7=8]
- `11:39:22`     momentum_uup                  avg_w=0.540  n=   2                           [flat=2]
- `11:39:22`     crisis_hy_oas_vs_spy          avg_w=0.620  n=   4                           [flat=4]
- `11:39:22` 
- `11:39:22`   Bottom contributors:
- `11:39:22`     edge_composite                avg_w=0.653  n=  69                           [day_1=24, day_14=25, day_7=20]
- `11:39:22`     crypto_risk_score             avg_w=0.338  n=  85                           [day_1=8, day_14=37, day_3=16, day_7=24]
- `11:39:22`     market_phase                  avg_w=0.310  n=  30                           [day_30=30]
- `11:39:22`     edge_regime                   avg_w=0.310  n=  30                           [day_30=30]
- `11:39:22`     khalid_index                  avg_w=0.311  n=  75                           [day_14=25, day_30=30, day_7=20]
# 4) Verify backtest.html with horizon attribution renders

- `11:39:27`   ✓ status=200, size=27,462b
- `11:39:27`     ✓ title
- `11:39:27`     ✓ horizon section
- `11:39:27`     ✓ renderHorizonAttribution fn
- `11:39:27`     ✓ nav active
- `11:39:27`     ✓ loads results.json
# 5) Deep diff: 'plumbing_stress' before/after horizon awareness

- `11:39:27`   plumbing_stress avg_w=0.998  n=99  win=69%  total_contrib=+1.072
- `11:39:27`     horizon mix: {'day_14': 25, 'day_30': 30, 'day_1': 24, 'day_7': 20}
- `11:39:27`   Before horizon-aware (flat w=0.99): contribution would have been roughly:
- `11:39:27`     n×avg_return × flat_weight × 0.005 (position size)

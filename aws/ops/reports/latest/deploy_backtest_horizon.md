# 1) Wait + redeploy backtest-engine

**Status:** success  
**Duration:** 11.3s  
**Finished:** 2026-05-05T11:32:43+00:00  

## Log
- `11:32:32`   zip size: 7,193b
- `11:32:37` ✅   ✓ deployed, mod=2026-05-05T11:32:33.000+0000
# 2) Inspect deployed source for horizon code

- `11:32:38`   ✓ get_horizon_weights fn
- `11:32:38`   ✓ resolve_weight fn
- `11:32:38`   ✓ paginator usage
- `11:32:38`   ✓ horizon counters
- `11:32:38`   ✓ v1.1 method tag
# 3) Invoke backtest-engine — measure horizon attribution

- `11:32:40`   status: 200, duration: 2.4s
- `11:32:40` 
- `11:32:40`   Headline metrics:
- `11:32:40`     n_outcomes:        1351
- `11:32:40`     total_return_pct:  70.7373%
- `11:32:40`     spy_return_pct:    9.2225%
- `11:32:40`     alpha_vs_spy_pct:  61.5148%
- `11:32:40`     final_nav:         $170,737
- `11:32:40`     max_dd_pct:        0.5931%
- `11:32:40`     sharpe:            10.0567
- `11:32:40` 
- `11:32:40`   Horizon attribution:
- `11:32:40`     n_horizon_weighted: 1317
- `11:32:40`     n_flat_weighted:    34
- `11:32:40`     day_1: 71
- `11:32:40`     day_14: 174
- `11:32:40`     day_3: 142
- `11:32:40`     day_30: 735
- `11:32:40`     day_5: 12
- `11:32:40`     day_7: 183
# 4) Inspect S3 backtest/results.json — top contributors

- `11:32:40`   method: calibrated_alpha_replay_v3_horizon_aware
- `11:32:40`   v: 1.1
- `11:32:40`   generated: 2026-05-05T11:32:39.112537+00:00
- `11:32:40`   by_signal: 26 signals
- `11:32:40` 
- `11:32:40`   Top 8 contributors (post-horizon):
- `11:32:40`     screener_top_pick             w=1.34  n= 555  win=83%  total_contrib=+49.8458
- `11:32:40`     ml_risk                       w=0.37  n=  75  win=80%  total_contrib=+3.0226
- `11:32:40`     carry_risk                    w=1.45  n=  30  win=100%  total_contrib=+2.4932
- `11:32:40`     crypto_fear_greed             w=0.42  n=  84  win=77%  total_contrib=+1.2388
- `11:32:40`     plumbing_stress               w=0.67  n=  99  win=69%  total_contrib=+1.0717
- `11:32:40`     momentum_spy                  w=0.68  n=  10  win=80%  total_contrib=+0.0461
- `11:32:40`     momentum_uup                  w=0.54  n=   2  win=0%  total_contrib=-0.0024
- `11:32:40`     crisis_hy_oas_vs_spy          w=0.62  n=   4  win=25%  total_contrib=-0.0044
- `11:32:40` 
- `11:32:40`   Bottom 5 contributors:
- `11:32:40`     edge_composite                w=1.29  n=  69  win=23%  total_contrib=-0.1676
- `11:32:40`     crypto_risk_score             w=0.40  n=  85  win=8%  total_contrib=-0.4284
- `11:32:40`     market_phase                  w=0.31  n=  30  win=0%  total_contrib=-0.5316
- `11:32:40`     edge_regime                   w=0.31  n=  30  win=0%  total_contrib=-0.5316
- `11:32:40`     khalid_index                  w=0.31  n=  75  win=1%  total_contrib=-0.6710
# 5) Verify backtest.html still loads + renders horizon attribution

- `11:32:43`   ✓ status=200, size=23,721b
- `11:32:43`     ✓ title
- `11:32:43`     ✗ horizon section
- `11:32:43`     ✗ renderHorizonAttribution
- `11:32:43`     ✓ nav active

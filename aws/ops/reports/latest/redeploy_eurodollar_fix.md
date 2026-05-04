# 1) Redeploy justhodl-eurodollar-stress with 2 bug fixes

**Status:** success  
**Duration:** 13.1s  
**Finished:** 2026-05-04T19:51:52+00:00  

## Log
- `19:51:39`   zip size: 4,376b
- `19:51:46` ✅   ✓ deployed at 2026-05-04T19:51:41.000+0000
# 2) Invoke + check 8/8 signals

- `19:51:49`   status: 200  duration: 2.9s
- `19:51:49`   resp: {"statusCode": 200, "body": "{\"composite_score\": 39.01, \"severity\": \"CALM\", \"regime\": \"CALM\", \"n_signals_used\": 8, \"duration_s\": 1.94}"}
- `19:51:49`   composite_score: 39.01
- `19:51:49`   severity: CALM  regime: CALM
- `19:51:49`   n_signals_used: 8/8
- `19:51:49` 
- `19:51:49`   Signal breakdown:
- `19:51:49`     ofr_fsi         value=   -0.6782  score= 28.9/100  █████
- `19:51:49`     hy_oas          value=      2.77  score= 11.2/100  ██
- `19:51:49`     ig_oas          value=      0.81  score= 22.8/100  ████
- `19:51:49`     vix             value=     16.99  score= 34.9/100  ██████
- `19:51:49`     broad_dollar    value=  118.7294  score= 34.4/100  ██████
- `19:51:49`     t_bill_3m       value=      3.59  score= 57.0/100  ███████████
- `19:51:49`     rate_vol_10y    value=    0.7269  score= 34.8/100  ██████
- `19:51:49`     repo_spread     value=      0.02  score= 88.1/100  █████████████████
- `19:51:49` 
- `19:51:49`   🔴 hot signals (>=70):
- `19:51:49`     repo_spread     score=88.1  (SOFR – Fed Funds Spread)
- `19:51:49` 
- `19:51:49`   🟢 cold signals (<=30):
- `19:51:49`     hy_oas          score=11.2  (HY Credit OAS)
- `19:51:49`     ig_oas          score=22.8  (IG Credit OAS)
- `19:51:49`     ofr_fsi         score=28.9  (St Louis Fed FSI)
# 3) Reinvoke wave-signal-logger v3 — verify eurodollar_stress dispatch lights up

- `19:51:52`   status: 200  duration: 2.9s
- `19:51:52`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 21, \"by_type\": {\"earnings_pead\": 5, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 0, \"correlation_break\": 5, \"divergence_extreme\": 1, \"cot_extreme\": 1}, \"duration_s\": 1.98}"}
# 4) Confirm in DDB — recent eurodollar_stress signals

- `19:51:52`   total recent signals (5min): 21
- `19:51:52`     correlation_break              n=5
- `19:51:52`     earnings_pead                  n=5
- `19:51:52`     etf_rotation                   n=3
- `19:51:52`     squeeze_risk                   n=2
- `19:51:52`     yc_regime                      n=1
- `19:51:52`     macro_composite_z              n=1
- `19:51:52`     cot_extreme                    n=1
- `19:51:52`     divergence_extreme             n=1
- `19:51:52`     sector_breadth                 n=1
- `19:51:52`     analog_signal                  n=1
- `19:51:52`   (no eurodollar_stress signal — score is in mid-zone 30-70, no actionable extreme)

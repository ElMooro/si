# 1) Force-redeploy wave-signal-logger

**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-05-04T19:54:27+00:00  

## Log
- `19:54:21`   zip size: 7,833b
- `19:54:24` ✅   ✓ deployed at 2026-05-04T19:54:22.000+0000
# 2) Invoke and confirm eurodollar_stress in dispatch

- `19:54:27`   status: 200  duration: 2.9s
- `19:54:27`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 21, \"by_type\": {\"earnings_pead\": 5, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 0, \"correlation_break\": 5, \"divergence_extreme\": 1, \"cot_extreme\": 1, \"eurodollar_stress\": 0}, \"duration_s\": 2.01}"}
- `19:54:27` 
- `19:54:27`   total signal types in dispatch: 14
- `19:54:27`   eurodollar_stress in dispatch:  True
- `19:54:27` ✅   ✓ eurodollar_stress dispatch entry present (n_signals=0)

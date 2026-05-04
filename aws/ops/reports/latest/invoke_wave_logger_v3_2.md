- `19:16:54`   ready: state=Active mod=2026-05-04T19:16:46.000+0000
# Invoke v3.2
**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-05-04T19:17:00+00:00  

## Log

- `19:16:57`   status=200 duration=2.7s
- `19:16:57`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 21, \"by_type\": {\"earnings_pead\": 5, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 0, \"correlation_break\": 5, \"divergence_extreme\": 1, \"cot_extreme\": 1}, \"duration_s\": 1.9}"}
# Verify all v3 signal types in DDB (last 5 min)

- `19:17:00`   ★ correlation_break              n=5
- `19:17:00`     earnings_pead                  n=5
- `19:17:00`     etf_rotation                   n=3
- `19:17:00`     squeeze_risk                   n=2
- `19:17:00`     macro_composite_z              n=1
- `19:17:00`     analog_signal                  n=1
- `19:17:00`     yc_regime                      n=1
- `19:17:00`     sector_breadth                 n=1
- `19:17:00`   ★ divergence_extreme             n=1
- `19:17:00`   ★ cot_extreme                    n=1

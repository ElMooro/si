# 1) Redeploy v3.1 with float casts in divergence + cot translators

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-05-04T19:23:10+00:00  

## Log
- `19:23:03`   zip size: 7,351b
- `19:23:06` ✅   ✓ deployed at 2026-05-04T19:23:03.000+0000
# 2) Invoke

- `19:23:09`   status: 200  duration: 3.2s
- `19:23:09`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 21, \"by_type\": {\"earnings_pead\": 5, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 0, \"correlation_break\": 5, \"divergence_extreme\": 1, \"cot_extreme\": 1}, \"duration_s\": 2.41}"}
# 3) Verify counts in DDB (last 5 min)

- `19:23:10`   total recent signals (5min): 21
- `19:23:10`     correlation_break              n=5
- `19:23:10`     earnings_pead                  n=5
- `19:23:10`     etf_rotation                   n=3
- `19:23:10`     squeeze_risk                   n=2
- `19:23:10`   ★ cot_extreme                    n=1
- `19:23:10`   ★ divergence_extreme             n=1
- `19:23:10`     macro_composite_z              n=1
- `19:23:10`     yc_regime                      n=1
- `19:23:10`     sector_breadth                 n=1
- `19:23:10`     analog_signal                  n=1
# 4) Sample fixed-type signal records

- `19:23:10`   cot_extreme:
- `19:23:10`     value: HG_EXTREME_LONG_p98
- `19:23:10`     pred: DOWN, conf: 0.85
- `19:23:10`     against: JJC, baseline: $19.56
- `19:23:10`     rationale: Spec net at 98th pct (5y) — contrarian EXTREME_LONG
- `19:23:10`   divergence_extreme:
- `19:23:10`     value: QQQ_vs_DGS10_z+2.16
- `19:23:10`     pred: DOWN, conf: 0.54
- `19:23:10`     against: QQQ, baseline: $674.15
- `19:23:10`     rationale: Nasdaq vs 10Y Yield: z=+2.16, qqq appears rich vs dgs10, intact=True

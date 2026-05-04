# Redeploy wave-signal-logger v2

**Status:** success  
**Duration:** 6.0s  
**Finished:** 2026-05-04T18:19:59+00:00  

## Log
- `18:19:53`   zip size: 5,328b
- `18:19:56` ✅   ✓ updated
# Invoke v2 — verify all 10 handlers

- `18:19:59`   status: 200  duration: 2.2s
- `18:19:59`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 17, \"by_type\": {\"earnings_pead\": 5, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 3}, \"duration_s\": 1.5}"}
# Per-handler log lines

- `18:19:59`   [wave-logger] starting at 2026-05-04T18:19:57.688346+00:00
- `18:19:59`   [LOG] earnings_pead=STRONG_POSITIVE_DRIFT UP conf=0.80 against=ROKU $123.58
- `18:19:59`   [LOG] earnings_pead=STRONG_POSITIVE_DRIFT UP conf=0.80 against=QCOM $177.01
- `18:19:59`   [LOG] earnings_pead=STRONG_POSITIVE_DRIFT UP conf=0.80 against=TMUS $196.06
- `18:19:59`   [LOG] earnings_pead=STRONG_POSITIVE_DRIFT UP conf=0.80 against=NOW $91.16
- `18:19:59`   [LOG] earnings_pead=STRONG_POSITIVE_DRIFT UP conf=0.80 against=ELV $372.68
- `18:19:59`   [wave-logger] earnings_pead: 5 signals logged
- `18:19:59`   [LOG] squeeze_risk=DTC_12.7 UP conf=0.70 against=LIN $507.92
- `18:19:59`   [LOG] squeeze_risk=DTC_8.7 UP conf=0.58 against=SHOP $127.67
- `18:19:59`   [wave-logger] squeeze_risk: 2 signals logged
- `18:19:59`   [LOG] etf_rotation=ROTATION_IN UP conf=0.45 against=UNG $10.71
- `18:19:59`   [LOG] etf_rotation=ROTATION_IN UP conf=0.45 against=DBA $28.11
- `18:19:59`   [LOG] etf_rotation=ROTATION_OUT DOWN conf=0.45 against=XLRE $44.32
- `18:19:59`   [wave-logger] etf_flow_extreme: 3 signals logged
- `18:19:59`   [LOG] macro_composite_z=z_1.10_GROWTH_SURPRISE_POSITIVE UP conf=0.37 against=SPY $720.65
- `18:19:59`   [wave-logger] macro_composite_z: 1 signals logged
- `18:19:59`   [LOG] yc_regime=BEAR_STEEPENER DOWN conf=0.45 against=TLT $85.61
- `18:19:59`   [wave-logger] yc_regime: 1 signals logged
- `18:19:59`   [LOG] analog_signal=BULLISH UP conf=0.85 against=SPY $720.65
- `18:19:59`   [wave-logger] analog_signal: 1 signals logged
- `18:19:59`   [wave-logger] event_signal: 0 signals logged
- `18:19:59`   [wave-logger] auction_crisis: 0 signals logged
- `18:19:59`   [LOG] sector_breadth=NARROW_LEADERSHIP DOWN conf=0.45 against=SPY $720.65
- `18:19:59`   [wave-logger] sector_breadth: 1 signals logged
- `18:19:59`   [LOG] momentum_top_pick=composite_99.6 UP conf=0.75 against=SNDK $1187.00
- `18:19:59`   [LOG] momentum_top_pick=composite_99.0 UP conf=0.75 against=STX $726.93
- `18:19:59`   [LOG] momentum_top_pick=composite_99.0 UP conf=0.75 against=INTC $99.62
- `18:19:59`   [wave-logger] momentum_top_pick: 3 signals logged
- `18:19:59`   [wave-logger] DONE — 17 signals logged in 1.5s

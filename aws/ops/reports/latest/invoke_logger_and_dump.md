# Invoke justhodl-wave-signal-logger

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-05-04T18:15:32+00:00  

## Log
- `18:15:32`   status: 200  duration: 2.0s
- `18:15:32`   resp: {"statusCode": 200, "body": "{\"total_signals_logged\": 9, \"by_type\": {\"earnings_pead\": 0, \"squeeze_risk\": 2, \"etf_flow_extreme\": 3, \"macro_composite_z\": 1, \"yc_regime\": 1, \"analog_signal\": 1, \"event_signal\": 0, \"auction_crisis\": 0, \"sector_breadth\": 1, \"momentum_top_pick\": 0}, \"duration_s\": 1.2}"}
# CloudWatch log tail

- `18:15:32`   START RequestId: b0faee9c-360e-4a78-8230-ec3d4d1bf614 Version: $LATEST
- `18:15:32`   [wave-logger] starting at 2026-05-04T18:15:31.120258+00:00
- `18:15:32`   [wave-logger] earnings_pead: 0 signals logged
- `18:15:32`   [LOG] squeeze_risk=DTC_12.7 UP conf=0.70 against=LIN $507.92
- `18:15:32`   [LOG] squeeze_risk=DTC_8.7 UP conf=0.58 against=SHOP $127.67
- `18:15:32`   [wave-logger] squeeze_risk: 2 signals logged
- `18:15:32`   [LOG] etf_rotation=ROTATION_IN UP conf=0.45 against=UNG $10.71
- `18:15:32`   [LOG] etf_rotation=ROTATION_IN UP conf=0.45 against=DBA $28.11
- `18:15:32`   [LOG] etf_rotation=ROTATION_OUT DOWN conf=0.45 against=XLRE $44.32
- `18:15:32`   [wave-logger] etf_flow_extreme: 3 signals logged
- `18:15:32`   [LOG] macro_composite_z=z_1.10_GROWTH_SURPRISE_POSITIVE UP conf=0.37 against=SPY $720.65
- `18:15:32`   [wave-logger] macro_composite_z: 1 signals logged
- `18:15:32`   [LOG] yc_regime=BEAR_STEEPENER DOWN conf=0.45 against=TLT $85.61
- `18:15:32`   [wave-logger] yc_regime: 1 signals logged
- `18:15:32`   [LOG] analog_signal=BULLISH UP conf=0.85 against=SPY $720.65
- `18:15:32`   [wave-logger] analog_signal: 1 signals logged
- `18:15:32`   [wave-logger] event_signal: 0 signals logged
- `18:15:32`   [wave-logger] auction_crisis: 0 signals logged
- `18:15:32`   [LOG] sector_breadth=NARROW_LEADERSHIP DOWN conf=0.45 against=SPY $720.65
- `18:15:32`   [wave-logger] sector_breadth: 1 signals logged
- `18:15:32`   [wave-logger] momentum_top_pick: 0 signals logged
- `18:15:32`   [wave-logger] DONE — 9 signals logged in 1.2s
- `18:15:32`   END RequestId: b0faee9c-360e-4a78-8230-ec3d4d1bf614
- `18:15:32`   REPORT RequestId: b0faee9c-360e-4a78-8230-ec3d4d1bf614	Duration: 1205.48 ms	Billed Duration: 1739 ms	Memory Size: 512 MB	Max Memory Used: 99 MB	Init Duration: 533.44 ms	

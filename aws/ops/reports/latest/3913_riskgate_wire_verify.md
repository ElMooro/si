# ops 3913 — risk-gate wired into 5 engines, live-output verified

**Status:** failure  
**Duration:** 109.9s  
**Finished:** 2026-07-26T18:55:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| gate_posture | gate_sizing |
|---|---|
| RISK_OFF | 0.45 |

## Log
## 1. zip-settle all 5

- `18:54:29` ✅   justhodl-position-sizer-v2: marker live attempt 6
- `18:54:41` ✅   justhodl-sizing-engine: marker live attempt 2
- `18:54:41` ✅   justhodl-opportunity-engine: marker live attempt 1
- `18:54:42` ✅   justhodl-best-setups: marker live attempt 1
- `18:54:42` ✅   justhodl-master-ranker: marker live attempt 1
## 2. invoke fast consumers + verify live output fields

- `18:55:02` ✗   justhodl-sizing-engine: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `18:55:09`   justhodl-master-ranker: rows w/ risk_gate fields=25, sample posture=RISK_OFF rank_mult=0.88 sizing_mult=0.45
- `18:55:17`   justhodl-best-setups: rows w/ risk_gate fields=85, sample posture=RISK_OFF rank_mult=0.88 sizing_mult=0.45
## 3. gate value sanity vs live risk-gate.json

- `18:55:17` ✅   justhodl-position-sizer-v2 settled
- `18:55:17` ✅   justhodl-sizing-engine settled
- `18:55:17` ✅   justhodl-opportunity-engine settled
- `18:55:17` ✅   justhodl-best-setups settled
- `18:55:17` ✅   justhodl-master-ranker settled
- `18:55:17` ✗   justhodl-sizing-engine invoke
- `18:55:17` ✅   justhodl-master-ranker rows carry gate fields
- `18:55:17` ✅   justhodl-best-setups rows carry gate fields
- `18:55:17` ✅   risk-gate feed live and valid
- `18:55:17` ✗ FAILED 1: ['justhodl-sizing-engine invoke']

# ops 4112 — family adapters: settle, fire, verify

**Status:** failure  
**Duration:** 861.9s  
**Finished:** 2026-07-30T02:55:15+00:00  

## Error

```
SystemExit: 1
```

## Data

| total_live |
|---|
| 0 |

## Log
- `02:45:11` ✅   async invoke fired; polling artifact
- `02:55:15`   spot ECONOMICS:BRINTR: got=None want~14.25
- `02:55:15`   spot ECONOMICS:PEINTR: got=None want~4.25
- `02:55:15`   spot ECONOMICS:BRFER: got=None want~368899
- `02:55:15` ✗   v3.15.0 settled in deployed zip
- `02:55:15` ✗   artifact moved with v3.15.0 marker
- `02:55:15` ✗   INTR family >=25 LIVE
- `02:55:15` ✗   FER family >=80 LIVE
- `02:55:15` ✗   WB trio >=250 LIVE
- `02:55:15` ✗   spot ECONOMICS:BRINTR
- `02:55:15` ✗   spot ECONOMICS:PEINTR
- `02:55:15` ✗   spot ECONOMICS:BRFER
- `02:55:15` ✗ FAILED: ['v3.15.0 settled in deployed zip', 'artifact moved with v3.15.0 marker', 'INTR family >=25 LIVE', 'FER family >=80 LIVE', 'WB trio >=250 LIVE', 'spot ECONOMICS:BRINTR', 'spot ECONOMICS:PEINTR', 'spot ECONOMICS:BRFER']

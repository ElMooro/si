# ops 4069 — check and deliver

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-07-29T01:35:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated | junk | real | store |
|---|---|---|---|
| 2026-07-29T01:26:07.973373+00:00 |  |  | 1090 |
|  | 999 | 91 |  |

## Log
- `01:35:36`   DIAG: {"started": 1785288299614, "done": 126, "total": 10319, "sc_ok": 126, "sc_err": 0, "sc2_ok": 126, "sc2_err": 0, "ss_ok": 0, "ss_err": 126, "matched": 100, "first_err": ""}
- `01:35:36`     TVC:HSI: provider/tvc
- `01:35:36`     NASDAQ:VIGI: source/NASDAQ
- `01:35:36`     AMEX:VPL: source/AMEX
- `01:35:36`     LSE:0LMQ: source/LSE
- `01:35:36`     AMEX:IDEV: source/AMEX
- `01:35:36`     AMEX:VEA: source/AMEX
- `01:35:36`     AMEX:EWN: source/AMEX
- `01:35:36`     AMEX:FNDC: source/AMEX
- `01:35:36`     AMEX:ISCF: source/AMEX
- `01:35:36`     AMEX:INTF: source/AMEX
- `01:35:36`     AMEX:AFK: source/AMEX
- `01:35:36`     TVC:NZ02Y: provider/tvc
- `01:35:36` ✗ only 91 real yet — diag names the wall: '' (sc 126/0, sc2 126/0)

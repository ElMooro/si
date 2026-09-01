# ops 5086 -- justhodl-fortress v2.0.0 deploy + daily + weekly backtest, verified

**Status:** failure  
**Duration:** 892.2s  
**Finished:** 2026-09-01T03:44:40+00:00  

## Error

```
SystemExit: 1
```

## Data

| donor | g1 | g2 | g3 | keys | last_update | memory | runtime | schedule | schedule_expr | schedule_state | state | timeout |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| justhodl-equity-research | PASS |  |  | ['FMP_KEY', 'FORTRESS_VERSION', 'POLYGON_API_KEY'] |  |  |  |  |  |  |  |  |
|  |  | PASS |  |  | Successful | 8192 | python3.12 |  |  |  | Active | 900 |
|  |  |  | PASS |  |  |  |  | updated |  |  |  |  |
|  |  |  |  |  |  |  |  |  | cron(30 3 ? * TUE-SAT *) | ENABLED |  |  |

## Log
## G1 key inheritance

## G2 deploy

- `03:29:48`   zip: 143931 bytes
## 1. Lambda

- `03:29:48`   Lambda exists — updating
- `03:29:51` ✅   ✓ updated justhodl-fortress
## G3 schedule (EventBridge Scheduler -- classic rule cap is saturated)

## G4 first run (async, verified on disk)

- `03:29:56` async invoke fired at 2026-09-01T03:29:48+00:00; polling data/fortress.json (prev as_of=2026-09-01T00:44:52+00:00)
- `03:30:59`   poll 60s: not fresh yet
- `03:32:02`   poll 120s: not fresh yet
- `03:33:05`   poll 180s: not fresh yet
- `03:34:08`   poll 240s: not fresh yet
- `03:35:12`   poll 300s: not fresh yet
- `03:36:15`   poll 360s: not fresh yet
- `03:37:18`   poll 420s: not fresh yet
- `03:38:21`   poll 480s: not fresh yet
- `03:39:24`   poll 540s: not fresh yet
- `03:40:27`   poll 600s: not fresh yet
- `03:41:30`   poll 660s: not fresh yet
- `03:42:33`   poll 720s: not fresh yet
- `03:43:36`   poll 780s: not fresh yet
- `03:44:39`   poll 840s: not fresh yet
- `03:44:40`   LOG INIT_START Runtime Version: python:3.12.mainlinev2.v31	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:c1ab740f3656a72d7917665a940f8634df245489445f5a660de5a634d06c5433
- `03:44:40` FAIL G4: no fresh payload within 14 min

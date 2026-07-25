# ops 3841 — portwatch cadence + schedule (feeds a hard leg)

**Status:** success  
**Duration:** 51.1s  
**Finished:** 2026-07-25T02:10:29+00:00  

## Data

| age_h_before | ports | ports_with_yoy | triggers_before |
|---|---|---|---|
| 14.0 | 89 | 88 | 1 |

## Log
## 1. Current freshness

- `02:09:39`   generated_at 2026-07-24T12:10:50.938521+00:00  (age 14.0h)
- `02:09:39`   LastModified 2026-07-24 12:11:50+00:00 · 35,583 bytes
- `02:09:39`   ports=89 chokepoints=28
- `02:09:39` ✅   daily cadence intact
## 2. Existing triggers

- `02:09:39`     Scheduler justhodl-portwatch-daily
- `02:09:39` ✅   1 trigger(s)
## 3. Arm / confirm

- `02:09:40` ✅   Scheduler armed cron(20 11 * * ? *) — daily, pre-US-open
## 4. Confirm the engine still runs and the hard leg survives

- `02:10:29` ✅   invoked clean · ports=89 · with yoy_pct=88
- `02:10:29` ✅   hard-leg field (yoy_pct) intact
- `02:10:29` ✅ PASS — cadence verified, schedule declared and armed

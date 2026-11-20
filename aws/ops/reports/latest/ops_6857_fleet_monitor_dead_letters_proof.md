# ops 6857 - fleet-error-monitor v1.3.0 live proof (dead letters by arrival and by engine)

**Status:** success  
**Duration:** 341.7s  
**Finished:** 2026-11-20T03:30:14+00:00  

## Data

| alert_severity | engines_stamped | failing_dead_letters | failing_engines | held | queue_depth | second_run_raised | second_run_suppressed | sns_sent |
|---|---|---|---|---|---|---|---|---|
| WARNING | 27 | 34 | 27 | {"justhodl-event-coordinator": 262} | 3255 | 0 | 1 | True |

## Log
## 1. the deploy has landed (by content)

- `03:28:00` deployed zip: 357 files, public_brain_projection.py bundled: True
- `03:28:00` after 208 s: landed True | LastModified 2026-11-20T03:27:58.000+0000 | LastUpdateStatus Successful | State Active | CodeSha256 jHgc507/YjrZ... | timeout 180 s mem 512 MB
## 2. independent read: AsyncEventsDropped per function, last 25 h

- `03:28:25` 919 functions scanned; 28 dropped async events: 34 from 27 running engines, 262 refused by 1 held engines
- `03:28:25` held: {"justhodl-event-coordinator": 262}
- `03:28:25` failing: dark-pool 2, earnings-blackout 2, estimate-revisions 2, forensic-screen 2, positioning-analog 2, signal-genealogy 2, stock-screener 2, bagger-engine 1, causality-scanner 1, chart-patterns 1, earnings-pead 1, ecb-derived 1, eps-revision-velocity 1, feed-catalog 1, fmp-ratios 1, hiring-velocity 1, investor-lenses 1, momentum-breakout 1, momentum-leaders 1, port-cargo 1, portfolio-snapshot 1, predictability 1, research-backtest 1, revenue-acceleration 1, share-flows 1, trade-nowcast 1, volatility-squeeze-hunter 1
## 3. quiet invoke (no notification, no history write)

- `03:29:01` quiet: status 200 in 36 s | scanned 920 | n_alerts 1 suppressed 0 | dlq_depth 3255
- `03:29:01` monitor: window 25 h | failing 34 from 27 engines | held {"justhodl-event-coordinator": 262}
- `03:29:01` per-engine counts that differ between the two reads: none
- `03:29:02` report after quiet: version 1.3.0 | detected 1 raised 1 | dlq {'available': True, 'visible': 3255, 'inflight': 0, 'total': 3255} | mode suppressed_for_audit | alert rows [('<DLQ>', 'WARNING', 'DLQ_BACKLOG')]
## 4. one real invoke: the corrected alert, once

- `03:29:02` history before: <DLQ>:CRITICAL 2026-11-20T02:29:41.071839+00:00 | <DLQ>:WARNING None | engines already reported 0
- `03:29:38` real #1: status 200 in 36 s | scanned 920 | n_alerts 1 suppressed 0 | dlq_depth 3255
- `03:29:39` report: detected 1 raised 1 suppressed 0 | sns_sent True telegram_sent False | DLQ row [('WARNING', 'DLQ_BACKLOG', 3255)]
- `03:29:39` history after: <DLQ>:WARNING 2026-11-20T03:29:38.469459+00:00 | <DLQ>:CRITICAL 2026-11-20T02:29:41.071839+00:00 | engines stamped 27 (held engine stamped: False)
## 5. a second real invoke: nothing is sent, the alarm stays visible

- `03:30:14` real #2: status 200 in 35 s | scanned 920 | n_alerts 0 suppressed 1 | dlq_depth 3255
- `03:30:14` report: detected 1 raised 0 suppressed 1 | sns_sent False | DLQ rows 1
- `03:30:14` history keys re-stamped by the second run: none
- `03:30:14` ✅ v1.3.0 live: 34 dead letters from 27 running engines named in one WARNING alert (sent once); {"justhodl-event-coordinator": 262} reported as held, not failures; queue depth 3255 no longer pages by itself

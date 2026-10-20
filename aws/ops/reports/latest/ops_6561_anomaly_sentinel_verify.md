# ops 6561 - governor v1.4.0 live: anomaly sentinel, kill switch, loop layers

**Status:** success  
**Duration:** 20.9s  
**Finished:** 2026-10-20T20:13:55+00:00  

## Data

| anomaly_held | cap | capped | mode | stage | version |
|---|---|---|---|---|---|
| [] | 600.0 | ["justhodl-event-coordinator", "justhodl-miss-calibrator"] | enforce | FREEZE | 1.4.0 |

## Log
- `20:13:36` anomaly_selftest: {"ok": true, "function": "justhodl-spend-governor-canary", "detected": true, "hold_path": "not exercised (fleet frozen; cap_selftest covers the hold/release ledger path)"}
- `20:13:36` ✅ anomaly sentinel detects a synthetic $6 window on the canary (not exercised (fleet frozen; cap_selftest covers the hold/release ledger path))
- `20:13:46` meter: ran=True window 1.2 h reads=2 active=2
- `20:13:46` runaway alarms: 5 of 5 armed and wired to the governor; states {'justhodl-event-breaker-open': 'OK', 'justhodl-runaway-concurrency': 'OK', 'justhodl-runaway-duration': 'OK', 'justhodl-runaway-invocations': 'OK', 'justhodl-s3-live-bucket-size': 'OK'}
- `20:13:46` ✅ the two October loop engines are under CAP (manual release only)
- `20:13:55` event_graph gate on the committed tree: exit 0 -   invoke chain (allowlisted): justhodl-symdir -[self]-> justhodl-symdir
- `20:13:55` ✅ v1.4.0 live: anomaly sentinel armed; loop layers intact

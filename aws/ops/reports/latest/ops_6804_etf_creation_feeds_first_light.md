# ops 6804 - ETF creations desk feeds first light 2026-11-12T18:21+00:00

**Status:** success  
**Duration:** 264.1s  
**Finished:** 2026-11-12T18:26:01+00:00  

## Data

| fails | funds |
|---|---|
| [] | 104 |

## Log
- `18:21:37` function: code last modified 2026-11-12T18:20:33.000+0000, sha erMMQTis5ONO
## 0. the 13:30 UTC invocation question

- `18:21:37`   stream 74f2ee2983a0: 2026-11-12T17:40+00:00 -> 2026-11-12T17:40+00:00
- `18:21:37`   stream c8886df6d75d: 2026-11-12T15:45+00:00 -> 2026-11-12T15:49+00:00
- `18:21:37`   stream 3681849d6846: 2026-11-11T15:45+00:00 -> 2026-11-11T15:48+00:00
- `18:21:37`   stream 7f9ee482beb7: 2026-11-11T06:13+00:00 -> 2026-11-11T06:20+00:00
- `18:21:37`   stream 5e1dc14edb37: 2026-11-10T15:45+00:00 -> 2026-11-10T15:54+00:00
- `18:21:37`   stream 0d47f01cefd9: 2026-11-10T13:30+00:00 -> 2026-11-10T13:40+00:00
- `18:21:37`   stream c0e5f29b8f0c: 2026-11-09T15:45+00:00 -> 2026-11-09T15:54+00:00
- `18:21:37`   stream eb9498f0c214: 2026-11-09T13:30+00:00 -> 2026-11-09T13:40+00:00
- `18:21:37`   stream 66b5194d9125: 2026-11-08T15:45+00:00 -> 2026-11-08T15:54+00:00
- `18:21:37`   stream a6529fcfa9d5: 2026-11-08T13:30+00:00 -> 2026-11-08T13:40+00:00
- `18:21:37`   stream 8e21e15d1d79: 2026-11-08T13:39+00:00 -> 2026-11-08T13:39+00:00
- `18:21:37`   stream 311b8335a4b2: 2026-11-07T15:45+00:00 -> 2026-11-07T15:54+00:00
- `18:21:37`   invocation starts in the last 36 h: ['2026-11-11T15:45+00:00']
- `18:21:37`   justhodl-schedule-liveness (cron 13:30 UTC) re-fires any engine whose data/<name>.json is older than max(8 h, 2.5 x cadence): the ETF packet was stale from 2026-10-12 to 2026-11-11, hence the daily 13:30 revive; it stops by itself now that the engine publishes daily
## 1. invoke once

- `18:26:00` invoke: status 200, function error None, 262s; result {"published": true, "generated_at": "2026-11-12T18:22:46.353878+00:00", "ledgers_moved": 0, "desk_feeds": 105, "coverage": {"issuer_histories": 3, "ledgers_accruing": 20, "reviewed_limits": 24, "snapshot_ledgers": 20, "workbook_histories": 81}}
## 2. the feeds

- `18:26:00` index: 104 funds, generated 2026-11-12T18:22:46.353878+00:00, packet data/etf-true-flows.json
- `18:26:00` funds with a retained history: 104; listed in the index: 104
- `18:26:00`   GLD   feed: 5529 dates 2004-11-18 -> 2026-11-11 | latest flow -147129589.6 5d 77991728.7 20d -1667934329.9 | packet 5d 77991728.7 | history 65681888f6b8c04fd2c
- `18:26:00`   IVV   feed: 6666 dates 2000-05-15 -> 2026-11-11 | latest flow -343378639.35 5d 3687567670.65 20d 7157031972.6 | packet 5d 3687567670.65 | history f3d7304d732efd25261
- `18:26:00`   SPY   feed: 5777 dates 2003-12-01 -> 2026-11-11 | latest flow 2089074660.75 5d -5374330670.05 20d 3298945207.9 | packet 5d -5374330670.05 | history 85b0a09d837a79a33ed
- `18:26:00`   TLT   feed: 6120 dates 2002-07-22 -> 2026-11-11 | latest flow -194085436.8 5d -900561800.4 20d -2860670304.7 | packet 5d -900561800.4 | history 6f85ed252c1d28de7f2
- `18:26:00`   HACK  feed: 39 dates 2026-09-18 -> 2026-11-11 | latest flow 11740960.0 5d 28479205.0 20d 66574090.0 | packet 5d 28479205.0 | history 7508ec9ed10c6024d7b
- `18:26:00`   ASHR  feed: 3273 dates 2013-11-05 -> 2026-11-11 | latest flow 0.0 5d 0.0 20d -7620000.0 | packet 5d 0.0 | history 7840f214d5a3f63b921
- `18:26:00` ledger funds already with a feed (their history exists from the first observed day): ['ARGT', 'BKLN', 'BOTZ', 'CIBR', 'DBA', 'DBC', 'DRIV', 'FINX', 'GDX', 'LIT', 'NLR', 'QQQ', 'ROBO', 'RSP', 'SMH', 'TAN', 'UNG', 'URA', 'USO', 'VNM']
- `18:26:01`   ARGT  ledger feed: 1 dates, latest flow None (None until two observed days)
- `18:26:01`   BKLN  ledger feed: 1 dates, latest flow None (None until two observed days)
- `18:26:01`   BOTZ  ledger feed: 1 dates, latest flow None (None until two observed days)
- `18:26:01` ledger rows after this second invocation of the day: {"ARGT": 1, "BKLN": 1, "BOTZ": 1, "CIBR": 1, "DBA": 1, "DBC": 1, "DRIV": 1, "FINX": 1, "GDX": 1, "LIT": 1, "NLR": 1, "QQQ": 1, "ROBO": 1, "RSP": 1, "SMH": 1, "TAN": 1, "UNG": 1, "URA": 1, "USO": 1, "VNM": 1}
- `18:26:01` GLD feed object: 530108 bytes, cache-control 'public, max-age=300'
- `18:26:01` ✅ 104 issuer-tape feeds live under data/etf-creations/, index written, ledgers idempotent

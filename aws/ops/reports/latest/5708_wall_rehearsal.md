# ops 5708 -- Monday wall rehearsal + schedules

**Status:** failure  
**Duration:** 555.7s  
**Finished:** 2026-09-18T15:34:57+00:00  

## Error

```
SystemExit: 1
```

## Data

| error | last_session | ok | symbols | week |
|---|---|---|---|---|
| None | ['2026-09-18'] | True | {"SPY": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}, "QQQ": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}, "IWM": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}, "TLT": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}, "GLD": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}, "BTC": {"bars": 0, "owned": null, "error": "session_file_missing:2026-09-18"}} | 2026-09-21 |

## Log
## 1. Prepare (real bars, real owned submissions)

## 2. Rehearse the post against Monday 09:31 ET (nothing written)

- `15:26:38` t+ 0 min: entries=0 (owned 0) skipped=6 pending=0
- `15:27:23` t+ 1 min: entries=0 (owned 0) skipped=6 pending=0
- `15:28:09` t+ 2 min: entries=0 (owned 0) skipped=6 pending=0
- `15:28:54` t+ 3 min: entries=0 (owned 0) skipped=6 pending=0
- `15:29:39` t+ 3 min: entries=0 (owned 0) skipped=6 pending=0
- `15:30:25` t+ 4 min: entries=0 (owned 0) skipped=6 pending=0
- `15:31:10` t+ 5 min: entries=0 (owned 0) skipped=6 pending=0
- `15:31:55` t+ 6 min: entries=0 (owned 0) skipped=6 pending=0
- `15:32:40` t+ 6 min: entries=0 (owned 0) skipped=6 pending=0
- `15:33:26` t+ 7 min: entries=0 (owned 0) skipped=6 pending=0
- `15:34:11` t+ 8 min: entries=0 (owned 0) skipped=6 pending=0
- `15:34:56` t+ 9 min: entries=0 (owned 0) skipped=6 pending=0
- `15:34:56` ⚠   skipped both BTC: session_file_missing:2026-09-18
- `15:34:56` ⚠   skipped both GLD: session_file_missing:2026-09-18
- `15:34:56` ⚠   skipped both IWM: session_file_missing:2026-09-18
- `15:34:56` ⚠   skipped both QQQ: session_file_missing:2026-09-18
- `15:34:56` ⚠   skipped both SPY: session_file_missing:2026-09-18
- `15:34:56` ⚠   skipped both TLT: session_file_missing:2026-09-18
- `15:34:56` ✗ 0 entries pass the door in rehearsal
## 3. Schedules (America/New_York)

- `15:34:57` ✅ created justhodl-ai-wall-prepare cron(5 9 ? * MON *)
- `15:34:57` ✅ created justhodl-ai-wall-post cron(31 9 ? * MON *)

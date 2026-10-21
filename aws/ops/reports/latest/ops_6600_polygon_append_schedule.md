# ops 6600 - tv-bars polygon-append: schedule, declaration, first run, bank verification

**Status:** success  
**Duration:** 244.1s  
**Finished:** 2026-10-21T16:44:04+00:00  

## Data

| appended_rows | appended_symbols | calls | failed | gap_older | skipped | step | through |
|---|---|---|---|---|---|---|---|
| 288 | 288 | 11 | 0 | 2 | 2 | polygon-append first run | 2026-10-20 |

## Log
## 1. the deployed function carries v1.2

- `16:40:00` justhodl-tv-bars modified 2026-10-21T16:36:47, timeout 600, memory 1024
- `16:40:01` ✅ deployed function answers mode=polygon-append (dry: {"ok": true, "mode": "polygon-append", "through": "2026-10-20", "symbols": 0, "dates": [], "gap_older_than_window": 2, "note": "every US name inside the window is current"})
## 2. schedule

- `16:40:01` ✅ created justhodl-tv-bars-polygon-append: cron(52 22 ? * MON-FRI *) ENABLED target justhodl-tv-bars role justhodl-scheduler-role
- `16:40:02` ✅ manifest declared ['justhodl-tv-bars-polygon-append'] (backup backups/schedule-manifest/20261021T164001Z/)
## 3. first append now

- `16:40:02` AAPL bank before: key data/warm/tv-bars/universe/US__AAPL.json.gz n 11724 last 2026-10-19 source mixed-bank:unqualified
- `16:44:03` append (241s): {"ok": true, "mode": "polygon-append", "at": "2026-10-21T16:44:02+00:00", "through": "2026-10-20", "dates": ["2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-19", "2026-10-20"], "holidays_or_unavailable": [], "calls": 11, "symbols_needing": 290, "appended_symbols": 288, "appended_rows": 288, "failed": 0, "skipped": 2, "gap_older_than_window": 2, "sample": {"AMEX:SPY": {"appended": 1, "last": "2026-10-20"}, "NASDAQ:AAPL": {"appended": 1, "last": "2026-10-20"}, "US:AAAU": {"appended": 1, "last": "2026-10-20"}, "US:AAPL": {"appended": 1, "last": "2026-10-20"}, "US:ACI": {"appended": 1, "last": "2026-10-20"}}}
- `16:44:03` AAPL bank after: n 11725 last 2026-10-20 source mixed-bank:unqualified; last 3 bars [('2026-10-16T13:30:00+00:00', 343.46), ('2026-10-19T13:30:00+00:00', 342.94), ('2026-10-20T13:30:00+00:00', 339.5)] labels ['polygon-grouped-daily:2026-10-16', 'polygon-grouped-daily:2026-10-19', 'polygon-aggs:daily-adjusted']
- `16:44:03` index last-bar dates now: [["2026-10-20", 288], ["2026-10-02", 11], ["2026-10-04", 3], ["2026-09-30", 1], ["2025-01-08", 1]]; polygon_append record: {"at": "2026-10-21T16:44:02+00:00", "through": "2026-10-20", "dates": ["2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-19", "2026-10-20"], "holidays_or_unavailable": [], "calls": 11, "symbols_needing": 290, "appended_symbols": 288, "appended_rows": 288, "failed": 0, "skipped": 2, "gap_older_than_window": 2}
## 4. the worker serves it

- `16:44:04` worker /ohlc AAPL: source warehouse, 11725 bars, last 2026-10-20T13:30:00+00:00 close 339.5
- `16:44:04` ✅ schedule justhodl-tv-bars-polygon-append live and declared; bank advanced to 2026-10-20 for 288 names

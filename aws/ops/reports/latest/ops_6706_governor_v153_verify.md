# ops 6706 - spend-governor v1.5.3 verify (read-only), 2026-11-01 11:42 UTC

**Status:** success  
**Duration:** 7.3s  
**Finished:** 2026-11-01T11:42:52+00:00  

## Data

| budget_actual | budget_actual_ignored | held | mtd | stage | storage_selftest_ok | version |
|---|---|---|---|---|---|---|
| 1176.941 | True | {"FREEZE/lambda": 904, "CAP/lambda": 5} | 0.0 | FREEZE | True | 1.5.3 |

## Log
## rung 2+3: artifact carries the marker, function ready

- `11:42:46` attempt 0: state Active / Successful, last modified 2026-11-01T11:41:49.000+0000, marker in governor_core.py: True, tick wired: True, VERSION = "1.5.3", 366 files, description: v1.5.3 - $600 cap (budget JustHodl-Monthly-Hard-Cap-600): br
## status (read-only)

- `11:42:52` version 1.5.3 mode enforce stage FREEZE since 2026-11-01T00:07:37.791709+00:00
- `11:42:52` budget actual $1176.941 forecast $1180.02 | MTD used $0.0 | sources {"budget_actual": 1176.941, "ce_mtd": 0.0, "budget_actual_ignored": "budget period not rolled over yet (last month's figure)"}
- `11:42:52` held: {"FREEZE/lambda": 904, "CAP/lambda": 5} | capped: ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api']
- `11:42:52` the stale October figure is now ignored: MTD $0.0 instead of $1176.941
## storage_selftest (read-only; v1.5.2's scenarios, first time on the account)

- `11:42:52` version 1.5.3, ok True, scenarios {"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"}
- `11:42:52` live partial: {} | versioning Enabled
- `11:42:52` ✅ v1.5.3 live; stage FREEZE left exactly as found (release is Khalid's)

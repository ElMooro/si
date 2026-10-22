# ops 6610 - S3 Inventory of the live bucket: old copies per file after the count rule (read-only)

**Status:** success  
**Duration:** 123.5s  
**Finished:** 2026-10-22T15:56:02+00:00  

## Data

| copies_over_cap | current | current_gb | delete_markers | inventory_stamp | keys_over_cap | miss_calibrator_old | old_copies | old_expirable_1d | old_gb | rows | system_events_old |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 7993648 | 3381373 | 1022.93 | 82 | 2026-10-21T01-00Z | 3688 | 7197422 | 8419588 | 8419588 | 1907.44 | 11801043 | 573495 |

## Log
- `15:53:59` inventory manifests found: 2 -> [["inventory/justhodl-dashboard-live/versions-daily/2026-10-20T01-00Z/manifest.json", "2026-10-21 07:58:51"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-21T01-00Z/manifest.json", "2026-10-21 14:38:28"]]
- `15:53:59` newest manifest 2026-10-21T01-00Z (delivered 2026-10-21 14:38:28): source justhodl-dashboard-live, version 2016-11-30, format CSV, 9 data files, schema ['Bucket', 'Key', 'VersionId', 'IsLatest', 'IsDeleteMarker', 'Size', 'LastModifiedDate', 'StorageClass', 'IsMultipartUploaded']
- `15:53:59` file 1/9 read: rows so far 1 (current 1, old copies 0, delete markers 0)
- `15:54:37` file 2/9 read: rows so far 3000001 (current 1, old copies 3000000, delete markers 0)
- `15:54:48` file 3/9 read: rows so far 4000001 (current 256765, old copies 3743236, delete markers 0)
- `15:55:21` file 4/9 read: rows so far 7000001 (current 792313, old copies 6207688, delete markers 0)
- `15:55:34` file 5/9 read: rows so far 8000001 (current 792313, old copies 7207688, delete markers 0)
- `15:55:43` file 6/9 read: rows so far 10036314 (current 2790416, old copies 7245898, delete markers 0)
- `15:55:53` file 7/9 read: rows so far 10890128 (current 2876130, old copies 8013932, delete markers 66)
- `15:56:02` file 8/9 read: rows so far 11800743 (current 3381301, old copies 8419360, delete markers 82)
- `15:56:02` file 9/9 read: rows so far 11801043 (current 3381373, old copies 8419588, delete markers 82)
## totals (inventory 2026-10-21T01-00Z)

- `15:56:02` rows 11801043: current objects 3381373 (1022.9 GB), old copies 8419588 (1907.4 GB) on 152135 keys, delete markers 82 on 76 keys
- `15:56:02` old copies older than 1 day (the rule's age condition): 8419588; oldest old copy 2026-05-22 23:56:28, newest 2026-10-21 02:30:12
- `15:56:02` keys with more than 10 old copies: 3688; copies above the cap in total: 7993648
- `15:56:02` histogram of old copies per key: [["1", 34236], ["101-1k", 56], ["10k-100k", 3], ["11-100", 3609], ["1k-10k", 18], ["2-10", 114211], [">100k", 2]]
## the storm keys

- `15:56:02` data/miss-calibrator-proposals.json: old copies 7197422, delete markers 0
- `15:56:02` system-events/: 1 keys with old copies, 573495 old copies in total; hottest [["system-events/audit/2026-10-12.jsonl", 573495]]
## top 25 keys by old copies

- `15:56:02` 7197422  data/miss-calibrator-proposals.json
- `15:56:02`  573495  system-events/audit/2026-10-12.jsonl
- `15:56:02`   46005  student-state.json
- `15:56:02`   32979  data/warm/worldbank-full/_state/state.json
- `15:56:02`   11144  crypto-intel.json
- `15:56:02`    9994  data/a2a/nudge-ledger.json
- `15:56:02`    9338  factory/salon/board.json
- `15:56:02`    9338  factory/scoreboard.json
- `15:56:02`    3513  repo-data.json
- `15:56:02`    2790  portfolio/risk.json
- `15:56:02`    2781  portfolio/snapshot.json
- `15:56:02`    2699  portfolio/sizing.json
- `15:56:02`    2640  signals/confluence-prev.json
- `15:56:02`    2640  signals/confluence.json
- `15:56:02`    2640  signals/regime-picks.json
- `15:56:02`    2199  errors/recent.json
- `15:56:02`    1839  signals/alert-history.json
- `15:56:02`    1836  screener/alpha-score.json
- `15:56:02`    1797  intelligence-report.json
- `15:56:02`    1771  signals/anomalies.json
- `15:56:02`    1771  signals/anomaly-alert-history.json
- `15:56:02`    1424  portfolio/sizing-alert-history.json
- `15:56:02`    1002  data/warm/sdmx-walker-summary.json
- `15:56:02`     776  screener/ma-latest.json
- `15:56:02`     683  data/warm/boj-full/_state/state.json
## top 15 prefixes by old copies

- `15:56:02`  7245898 old copies,  127.71 GB | current 2790415 objects,  912.55 GB | data/
- `15:56:02`   573495 old copies, 1742.03 GB | current     139 objects,    0.14 GB | system-events/
- `15:56:02`   390631 old copies,    2.51 GB | current  497429 objects,   78.34 GB | audit-private/
- `15:56:02`    46005 old copies,    0.50 GB | current       1 objects,    0.00 GB | student-state.json
- `15:56:02`    43660 old copies,    8.25 GB | current   32323 objects,   10.41 GB | equity-research/
- `15:56:02`    26793 old copies,    3.18 GB | current   23172 objects,    2.67 GB | equity-research-history/
- `15:56:02`    18688 old copies,    0.04 GB | current      87 objects,    0.00 GB | factory/
- `15:56:02`    15721 old copies,    1.02 GB | current     539 objects,    0.61 GB | etf-flows/
- `15:56:02`    13301 old copies,    0.19 GB | current       6 objects,    0.00 GB | signals/
- `15:56:02`    11144 old copies,   13.90 GB | current       1 objects,    0.01 GB | crypto-intel.json
- `15:56:02`    10307 old copies,    0.62 GB | current      16 objects,    0.00 GB | portfolio/
- `15:56:02`     4224 old copies,    1.31 GB | current     167 objects,    0.03 GB | screener/
- `15:56:02`     3915 old copies,    0.02 GB | current    4629 objects,    0.01 GB | edgar-insiders/
- `15:56:02`     3513 old copies,    0.21 GB | current       1 objects,    0.00 GB | repo-data.json
- `15:56:02`     2221 old copies,    0.32 GB | current    2251 objects,    0.00 GB | errors/
- `15:56:02` ⚠ the count rule has not (yet) expired 7993648 old copies above the 10-per-file cap on 3688 keys - a one-time purge of OLD COPIES ONLY is the proposal for Khalid; nothing deleted here
- `15:56:02` ✅ read-only; nothing written

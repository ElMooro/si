# ops 6610 - S3 Inventory of the live bucket: old copies per file after the count rule (read-only)

**Status:** success  
**Duration:** 117.2s  
**Finished:** 2026-10-24T00:02:41+00:00  

## Data

| copies_over_cap | current | current_gb | delete_markers | inventory_stamp | keys_over_cap | miss_calibrator_old | old_copies | old_expirable_1d | old_gb | rows | system_events_old |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 7539766 | 3459439 | 1033.08 | 82 | 2026-10-23T01-00Z | 571 | 6804553 | 8033694 | 8031941 | 1893.44 | 11493215 | 568955 |

## Log
- `00:00:44` inventory manifests found: 4 -> [["inventory/justhodl-dashboard-live/versions-daily/2026-10-20T01-00Z/manifest.json", "2026-10-21 07:58:51"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-21T01-00Z/manifest.json", "2026-10-21 14:38:28"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-22T01-00Z/manifest.json", "2026-10-22 19:58:50"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-23T01-00Z/manifest.json", "2026-10-23 19:58:00"]]
- `00:00:44` newest manifest 2026-10-23T01-00Z (delivered 2026-10-23 19:58:00): source justhodl-dashboard-live, version 2016-11-30, format CSV, 9 data files, schema ['Bucket', 'Key', 'VersionId', 'IsLatest', 'IsDeleteMarker', 'Size', 'LastModifiedDate', 'StorageClass', 'IsMultipartUploaded']
- `00:00:44` file 1/9 read: rows so far 1 (current 1, old copies 0, delete markers 0)
- `00:00:52` file 2/9 read: rows so far 936119 (current 544246, old copies 391857, delete markers 16)
- `00:01:02` file 3/9 read: rows so far 1776801 (current 634717, old copies 1142006, delete markers 78)
- `00:01:11` file 4/9 read: rows so far 3492949 (current 2289411, old copies 1203460, delete markers 78)
- `00:01:11` file 5/9 read: rows so far 3493215 (current 2289483, old copies 1203654, delete markers 78)
- `00:01:48` file 6/9 read: rows so far 6493215 (current 2289483, old copies 4203654, delete markers 78)
- `00:01:55` file 7/9 read: rows so far 7493215 (current 2901462, old copies 4591675, delete markers 78)
- `00:02:28` file 8/9 read: rows so far 10493215 (current 3459439, old copies 7033694, delete markers 82)
- `00:02:41` file 9/9 read: rows so far 11493215 (current 3459439, old copies 8033694, delete markers 82)
## totals (inventory 2026-10-23T01-00Z)

- `00:02:41` rows 11493215: current objects 3459439 (1033.1 GB), old copies 8033694 (1893.4 GB) on 186564 keys, delete markers 82 on 77 keys
- `00:02:41` old copies older than 1 day (the rule's age condition): 8031941; oldest old copy 2026-05-22 23:56:28, newest 2026-10-23 03:37:05
- `00:02:41` keys with more than 10 old copies: 571; copies above the cap in total: 7539766
- `00:02:41` histogram of old copies per key: [["1", 48731], ["101-1k", 37], ["10k-100k", 3], ["11-100", 512], ["1k-10k", 17], ["2-10", 137262], [">100k", 2]]
## the storm keys

- `00:02:41` data/miss-calibrator-proposals.json: old copies 6804553, delete markers 0
- `00:02:41` system-events/: 1 keys with old copies, 568955 old copies in total; hottest [["system-events/audit/2026-10-12.jsonl", 568955]]
## top 25 keys by old copies

- `00:02:41` 6804553  data/miss-calibrator-proposals.json
- `00:02:41`  568955  system-events/audit/2026-10-12.jsonl
- `00:02:41`   45551  student-state.json
- `00:02:41`   32571  data/warm/worldbank-full/_state/state.json
- `00:02:41`   11132  crypto-intel.json
- `00:02:41`    9920  data/a2a/nudge-ledger.json
- `00:02:41`    9142  factory/salon/board.json
- `00:02:41`    8925  factory/scoreboard.json
- `00:02:41`    3515  repo-data.json
- `00:02:41`    2713  portfolio/snapshot.json
- `00:02:41`    2701  portfolio/sizing.json
- `00:02:41`    2677  portfolio/risk.json
- `00:02:41`    2642  signals/confluence.json
- `00:02:41`    2632  signals/confluence-prev.json
- `00:02:41`    2555  signals/regime-picks.json
- `00:02:41`    2199  errors/recent.json
- `00:02:41`    1743  signals/anomalies.json
- `00:02:41`    1730  signals/alert-history.json
- `00:02:41`    1711  signals/anomaly-alert-history.json
- `00:02:41`    1617  screener/alpha-score.json
- `00:02:41`    1262  intelligence-report.json
- `00:02:41`    1237  portfolio/sizing-alert-history.json
- `00:02:41`     749  screener/ma-latest.json
- `00:02:41`     670  data/warm/sdmx-walker-summary.json
- `00:02:41`     493  data/warm/boj-full/_state/state.json
## top 15 prefixes by old copies

- `00:02:41`  6891494 old copies,  131.75 GB | current 2824650 objects,  919.04 GB | data/
- `00:02:41`   568955 old copies, 1728.45 GB | current     139 objects,    0.14 GB | system-events/
- `00:02:41`   379052 old copies,    1.20 GB | current  536499 objects,   81.23 GB | audit-private/
- `00:02:41`    45551 old copies,    0.50 GB | current       1 objects,    0.00 GB | student-state.json
- `00:02:41`    45275 old copies,    8.89 GB | current   34186 objects,   10.83 GB | equity-research/
- `00:02:41`    28313 old copies,    3.38 GB | current   25735 objects,    3.00 GB | equity-research-history/
- `00:02:41`    18079 old copies,    0.04 GB | current      87 objects,    0.00 GB | factory/
- `00:02:41`    13013 old copies,    0.18 GB | current       6 objects,    0.00 GB | signals/
- `00:02:41`    11132 old copies,   13.83 GB | current       1 objects,    0.01 GB | crypto-intel.json
- `00:02:41`     9459 old copies,    0.55 GB | current      16 objects,    0.00 GB | portfolio/
- `00:02:41`     5443 old copies,    0.66 GB | current     539 objects,    0.61 GB | etf-flows/
- `00:02:41`     3552 old copies,    1.00 GB | current     169 objects,    0.03 GB | screener/
- `00:02:41`     3515 old copies,    0.21 GB | current       1 objects,    0.00 GB | repo-data.json
- `00:02:41`     2507 old copies,    0.01 GB | current    4924 objects,    0.01 GB | edgar-insiders/
- `00:02:41`     2221 old copies,    0.32 GB | current    2251 objects,    0.00 GB | errors/
- `00:02:41` ⚠ the count rule has not (yet) expired 7539766 old copies above the 10-per-file cap on 571 keys - a one-time purge of OLD COPIES ONLY is the proposal for Khalid; nothing deleted here
- `00:02:41` ✅ read-only; nothing written

# ops 6610 - S3 Inventory of the live bucket: old copies per file after the count rule (read-only)

**Status:** success  
**Duration:** 119.4s  
**Finished:** 2026-10-23T15:33:20+00:00  

## Data

| copies_over_cap | current | current_gb | delete_markers | inventory_stamp | keys_over_cap | miss_calibrator_old | old_copies | old_expirable_1d | old_gb | rows | system_events_old |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 7575589 | 3420242 | 1028.29 | 82 | 2026-10-22T01-00Z | 2271 | 6808388 | 8040791 | 8040791 | 1898.34 | 11461115 | 571385 |

## Log
- `15:31:21` inventory manifests found: 3 -> [["inventory/justhodl-dashboard-live/versions-daily/2026-10-20T01-00Z/manifest.json", "2026-10-21 07:58:51"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-21T01-00Z/manifest.json", "2026-10-21 14:38:28"], ["inventory/justhodl-dashboard-live/versions-daily/2026-10-22T01-00Z/manifest.json", "2026-10-22 19:58:50"]]
- `15:31:21` newest manifest 2026-10-22T01-00Z (delivered 2026-10-22 19:58:50): source justhodl-dashboard-live, version 2016-11-30, format CSV, 9 data files, schema ['Bucket', 'Key', 'VersionId', 'IsLatest', 'IsDeleteMarker', 'Size', 'LastModifiedDate', 'StorageClass', 'IsMultipartUploaded']
- `15:31:30` file 1/9 read: rows so far 928436 (current 525506, old copies 402914, delete markers 16)
- `15:31:38` file 2/9 read: rows so far 2616701 (current 2161389, old copies 455296, delete markers 16)
- `15:32:11` file 3/9 read: rows so far 5616701 (current 2708323, old copies 2908360, delete markers 18)
- `15:32:24` file 4/9 read: rows so far 6616701 (current 2708323, old copies 3908360, delete markers 18)
- `15:32:24` file 5/9 read: rows so far 6616702 (current 2708324, old copies 3908360, delete markers 18)
- `15:32:24` file 6/9 read: rows so far 6616966 (current 2708396, old copies 3908552, delete markers 18)
- `15:32:34` file 7/9 read: rows so far 7461115 (current 2795938, old copies 4665095, delete markers 82)
- `15:33:12` file 8/9 read: rows so far 10461115 (current 2795938, old copies 7665095, delete markers 82)
- `15:33:20` file 9/9 read: rows so far 11461115 (current 3420242, old copies 8040791, delete markers 82)
## totals (inventory 2026-10-22T01-00Z)

- `15:33:20` rows 11461115: current objects 3420242 (1028.3 GB), old copies 8040791 (1898.3 GB) on 176103 keys, delete markers 82 on 77 keys
- `15:33:20` old copies older than 1 day (the rule's age condition): 8040791; oldest old copy 2026-05-22 23:56:28, newest 2026-10-22 03:29:27
- `15:33:20` keys with more than 10 old copies: 2271; copies above the cap in total: 7575589
- `15:33:20` histogram of old copies per key: [["1", 48401], ["101-1k", 51], ["10k-100k", 3], ["11-100", 2198], ["1k-10k", 17], ["2-10", 125431], [">100k", 2]]
## the storm keys

- `15:33:20` data/miss-calibrator-proposals.json: old copies 6808388, delete markers 0
- `15:33:20` system-events/: 1 keys with old copies, 571385 old copies in total; hottest [["system-events/audit/2026-10-12.jsonl", 571385]]
## top 25 keys by old copies

- `15:33:20` 6808388  data/miss-calibrator-proposals.json
- `15:33:20`  571385  system-events/audit/2026-10-12.jsonl
- `15:33:20`   45614  student-state.json
- `15:33:20`   32571  data/warm/worldbank-full/_state/state.json
- `15:33:20`   11126  crypto-intel.json
- `15:33:20`    9846  data/a2a/nudge-ledger.json
- `15:33:20`    9288  factory/scoreboard.json
- `15:33:20`    9148  factory/salon/board.json
- `15:33:20`    3514  repo-data.json
- `15:33:20`    2724  portfolio/risk.json
- `15:33:20`    2713  portfolio/snapshot.json
- `15:33:20`    2700  portfolio/sizing.json
- `15:33:20`    2641  signals/confluence-prev.json
- `15:33:20`    2641  signals/confluence.json
- `15:33:20`    2641  signals/regime-picks.json
- `15:33:20`    2199  errors/recent.json
- `15:33:20`    1798  intelligence-report.json
- `15:33:20`    1772  signals/anomalies.json
- `15:33:20`    1772  signals/anomaly-alert-history.json
- `15:33:20`    1729  signals/alert-history.json
- `15:33:20`    1616  screener/alpha-score.json
- `15:33:20`    1331  portfolio/sizing-alert-history.json
- `15:33:20`     815  data/warm/sdmx-walker-summary.json
- `15:33:20`     747  screener/ma-latest.json
- `15:33:20`     672  _health/fleet.json
## top 15 prefixes by old copies

- `15:33:20`  6881142 old copies,  128.81 GB | current 2807121 objects,  916.10 GB | data/
- `15:33:20`   571385 old copies, 1734.10 GB | current     139 objects,    0.14 GB | system-events/
- `15:33:20`   389042 old copies,    2.31 GB | current  517762 objects,   79.85 GB | audit-private/
- `15:33:20`    45614 old copies,    0.50 GB | current       1 objects,    0.00 GB | student-state.json
- `15:33:20`    43287 old copies,    8.44 GB | current   32951 objects,   10.58 GB | equity-research/
- `15:33:20`    27276 old copies,    3.24 GB | current   24187 objects,    2.80 GB | equity-research-history/
- `15:33:20`    18448 old copies,    0.04 GB | current      87 objects,    0.00 GB | factory/
- `15:33:20`    13196 old copies,    0.19 GB | current       6 objects,    0.00 GB | signals/
- `15:33:20`    11126 old copies,   13.79 GB | current       1 objects,    0.01 GB | crypto-intel.json
- `15:33:20`    10050 old copies,    0.56 GB | current      16 objects,    0.00 GB | portfolio/
- `15:33:20`     8280 old copies,    0.99 GB | current     539 objects,    0.61 GB | etf-flows/
- `15:33:20`     3967 old copies,    0.02 GB | current    4795 objects,    0.01 GB | edgar-insiders/
- `15:33:20`     3537 old copies,    1.00 GB | current     168 objects,    0.03 GB | screener/
- `15:33:20`     3514 old copies,    0.21 GB | current       1 objects,    0.00 GB | repo-data.json
- `15:33:20`     2221 old copies,    0.32 GB | current    2251 objects,    0.00 GB | errors/
- `15:33:20` ⚠ the count rule has not (yet) expired 7575589 old copies above the 10-per-file cap on 2271 keys - a one-time purge of OLD COPIES ONLY is the proposal for Khalid; nothing deleted here
- `15:33:20` ✅ read-only; nothing written

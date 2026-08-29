## P0 ground truth of the extraction

**Status:** failure  
**Duration:** 250.6s  
**Finished:** 2026-08-29T18:51:27+00:00  

## Error

```
SystemExit: 1
```

## Log
- `18:47:20`   n_pages BEFORE count: 949124  flows_done=5545  updated_at=2026-08-29T18:34:36+00:00
- `18:51:26`   objects counted     : 966466 (269.3 GB)
- `18:51:26`   n_pages AFTER count : 964888
- `18:51:26`   bracket 949124 <= 966466 <= 964888 : *** GAP ***
- `18:51:26`   flows 5554 / 8147 (68.17%)   series 482444000   269.3 GB
- `18:51:26`   holes=0 failed_flows=0 write_errors_last_run=0
- `18:51:27`   series-manifest: flows_total=8147 flows_parsed=5545 series_extracted=478294500 n_pages=956589 updated_at=2026-08-29T18:34:36+00:00
## P1 the IMPORT DEGRADED banner + sentinel incidents

- `18:51:27`   status=None updated_at=None
- `18:51:27`   incidents: 5
- `18:51:27`    1) {"at": "2026-08-10T08:25:04+00:00", "pipeline": "fred", "kind": "expansion", "detail": "scoped COMPLETE \u2192 full catalog started"}
- `18:51:27`    2) {"at": "2026-08-10T04:15:04+00:00", "pipeline": "fred", "kind": "auto_heal", "detail": "stalled \u2014 async kick queued"}
- `18:51:27`    3) {"at": "2026-08-10T04:05:04+00:00", "pipeline": "fred", "kind": "auto_heal", "detail": "stalled \u2014 async kick queued"}
- `18:51:27`    4) {"at": "2026-08-10T03:58:10+00:00", "pipeline": "fred", "kind": "auto_heal", "detail": "stalled \u2014 async kick queued"}
- `18:51:27`    5) {"at": "2026-08-10T03:57:48+00:00", "pipeline": "fred", "kind": "auto_heal", "detail": "stalled \u2014 async kick queued"}
- `18:51:27`   non-OK providers: {}
## P2 the eurostat card's actual inputs

- `18:51:27`   eurostat card: {"slug": "eurostat", "name": "Eurostat", "api": "ec.europa.eu/eurostat SDMX", "datasets": 8191, "datasets_target": 8152, "coverage_pct": 100.0, "coverage_note": "at_or_above_target", "coverage_basis": "keys/keys (walker n_total \u2014 same unit)", "denied_source_side": null, "unit": "keys", "n_keys": 8191, "total_mb": 8892.72, "hot_feeds": 43, "series_count": 8152, "catalog_note": null, "freshest_h": 0.9}
- `18:51:27`   does the catalog mention 'data/providers/eurostat/series/' anywhere? False
- `18:51:27`   -> the card counts data/warm/eurostat/ only; the extraction lives in data/providers/ and is not scanned
- `18:51:27`   totals on the page: S3 KEYS and WARM+HOT GB therefore EXCLUDE every page written today
## P3 the other two flags on the page

- `18:51:27`   census-us    data/warm/census-us/state.json -> NoSuchKey
- `18:51:27`   census-us alt data/_state/census-us.json -> NoSuchKey
- `18:51:27`   fred         data/_state/fred-import.json -> NoSuchKey
- `18:51:27`   fred alt     data/warm/fred/state.json -> NoSuchKey
## P4 what a correct eurostat card would cost

- `18:51:27`   adding data/providers/eurostat/series/ to the catalog's
- `18:51:27`   prefixes would make it LIST 966466 objects (~967 requests) on every run -- pennies, but it would also move the page's S3 KEYS from ~776k to ~1.7M and WARM+HOT from 214.9 GB to ~484 GB
- `18:51:27`   the cheaper honest option: point series_from at the series-manifest's series_extracted, which needs _series_list() to accept an int as well as a list (it currently requires a list of ids and counts len())
- `18:51:27`   NOT APPLIED HERE -- the renderer decides which of those is right, and a display change belongs in its own op
- `18:51:27`   -> data/ops/dashboard-reconcile.json
- `18:51:27` ops 5038 RED: P0:gap

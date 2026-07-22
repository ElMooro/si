# ops 3722 — in-Lambda probe of fetch_calendar

**Status:** success  
**Duration:** 8.5s  
**Finished:** 2026-07-22T19:37:14+00:00  

## Data

| probe | value |
|---|---|
| module | {"path": "/var/task/benzinga.py", "has_fmp_calendar": true, "has_degrade": true, "bytes": 14062} |
| env | {"FMP_KEY_set": true, "FMP_API_KEY_set": false, "MASSIVE_API_KEY_set": false, "benzinga_key_resolves": true} |
| massive_raw | {"is_none": false, "type": "dict", "keys": ["next_url", "request_id", "results", "status"], "n_results": 50, "head": "{\"status\": \"OK\", \"request_id\": \"269632fb356e4bc382dbae298249dc40\", \"results\": [{\"currency\": \"USD\", \"date_status\": \"projected\", \"estimated_eps\": 0.28, \"previous_e |
| fmp_shim | {"n": 4000, "n_with_eps": 2879, "sample": {"ticker": "0006.HK", "company": null, "date": "2026-08-05", "time": null, "session": "\u2014", "importance": 3, "fiscal_period": null, "fiscal_year": null, "estimated_eps": null, "estimated_revenue": null, "actual_eps": null, "actual_revenue": null, "_sourc |
| fetch_calendar_minimp0 | {"n": 1000, "n_with_eps": 822, "sources": ["benzinga"], "sample": {"ticker": "WSR", "company": "Whitestone REIT", "date": "2026-08-05", "session": "AMC", "date_status": "projected", "estimated_eps": 0.28, "previous_eps": 0.26, "estimated_revenue": 42450800.0, "importance": 2, "fiscal_period": "Q2",  |
| fetch_calendar_minimp2 | {"n": 735, "n_with_eps": 687, "sources": ["benzinga"], "sample": {"ticker": "WSR", "company": "Whitestone REIT", "date": "2026-08-05", "session": "AMC", "date_status": "projected", "estimated_eps": 0.28, "previous_eps": 0.26, "estimated_revenue": 42450800.0, "importance": 2, "fiscal_period": "Q2", " |

## Log
- `19:37:06` zip_bytes: 84207
- `19:37:11` probe_created: active
- `19:37:14` function_error: None
## PROBE RESULT

- `19:37:14` VERDICT_SIGNALS: module_has_shim=True fmp_shim_rows=4000 fetch_calendar(min_imp=0)=1000 fetch_calendar(min_imp=2)=735 massive_none=False
- `19:37:14` probe_deleted: True
- `19:37:14` VERDICT: DIAGNOSIS COMPLETE

# ops 3958 — Tankan close (runner-exec + hardened)

**Status:** failure  
**Duration:** 42.9s  
**Finished:** 2026-07-27T03:50:01+00:00  

## Error

```
SystemExit: 1
```

## Data

| boj_ok | n_live | n_series_pulled |
|---|---|---|
| 1 | 8 | 58 |

## Log
## runner-exec p_boj (ground truth)

- `03:49:20`   {'name': 'Bank lending YoY (JPLG)', 'value': 7.07, 'unit': '%', 'asof': '202605', 'ok': True}
- `03:49:20`   {'name': 'Tankan DI Large Mfg (actual)', 'value': None, 'unit': 'pts', 'asof': None, 'ok': False, 'note': 'HTTPError: HTTP Error 400: Bad Request'}
## settle v2.0.1

- `03:49:20` ✅   settled attempt 1
- `03:50:01` ✅   artifact ~40s
- `03:50:01`   boj probe: {'name': 'Bank lending YoY (JPLG)', 'value': 7.07, 'unit': '%', 'asof': '202605', 'ok': True}
- `03:50:01`   boj probe: {'name': 'Tankan DI Large Mfg (actual)', 'value': None, 'unit': 'pts', 'asof': None, 'ok': False, 'note': 'HTTPError: HTTP Error 400: Bad Request'}
- `03:50:01` ✅   runner-exec ran
- `03:50:01` ✅   v2.0.1 settled
- `03:50:01` ✅   artifact written
- `03:50:01` ✗   boj >= 2 ok probes
- `03:50:01` ✅   n_series_pulled >= 46
- `03:50:01` ✗ FAILED: ['boj >= 2 ok probes']

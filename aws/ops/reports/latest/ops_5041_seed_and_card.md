## P0 state + manifest, printed unconditionally

**Status:** success  
**Duration:** 369.2s  
**Finished:** 2026-08-29T20:08:21+00:00  

## Data

| derived | n_keys | series |
|---|---|---|
| True | 1124529 | 558169101 |

## Log
- `20:02:14`   flows_done=6977 n_pages=1116368 series_count=558184000 updated_at=2026-08-29T19:59:03+00:00
- `20:02:14`   pages_seeded    = '2026-08-29T19:45:01+00:00'
- `20:02:14`   pages_seed_error= None
- `20:02:14`   pages_objects   = 1116368
- `20:02:14`   pages_bytes     = 309714685756
- `20:02:14`   holes=0 failed_flows=0 write_errors_last_run=0
- `20:02:15`   BEFORE hub totals: keys=776,663 gb=214.86 datasets=800963
## P1 wait for the seed

- `20:02:17`   SEEDED: pages=1,116,338 pages_bytes=309.7 GB (flows_parsed=6843, series=558,169,101)
## P2 run the catalog asynchronously

- `20:02:17`   Event invoke accepted status=202
- `20:08:20`   hub rewritten after 360s (as_of 2026-08-29T19:52:46+00:00 -> 2026-08-29T20:02:17+00:00)
## P3 the card

- `20:08:20`   series.count=558,169,101 counted=True
- `20:08:20`   n_keys=1,124,529 total_mb=318599.72
- `20:08:20`   derived={"source": "data/providers/eurostat/series-manifest.json", "objects": 1116338, "bytes": 309707005982, "mb": 309707.01, "freshest_h": 0.3}
- `20:08:20`   note=None
- `20:08:21`   hub totals keys 776,663 -> 1,893,001
- `20:08:21`   hub totals gb   214.86 -> 524.57
- `20:08:21`   hub datasets    800963 -> 800963 (must NOT absorb series)
- `20:08:21`   hub eurostat row: series_count=558,169,101 n_keys=1,124,529 coverage_pct=100.0 datasets=8191
## P4 regression

- `20:08:21`   data/providers/eurostat.json = 0.02 MB, per-key rows=100
- `20:08:21`   other providers: [('statcan', None), ('fred', 277453), ('oecd', 1546), ('bis', 29)]
- `20:08:21`   -> data/ops/eurostat-card-fix.json
- `20:08:21` ops 5041 GREEN -- card truthful and totals complete

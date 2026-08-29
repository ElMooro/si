## P0/P1 track the ECB lane

**Status:** success  
**Duration:** 1564.5s  
**Finished:** 2026-08-29T21:13:20+00:00  

## Data

| complete | flows | objects | series | total |
|---|---|---|---|---|
| True | 207 | 6481 | 3240500 | 207 |

## Log
- `20:47:16`   t+ 0min flows=51/207 (24.6%) pages=2075 series=1037500 stopped_early=None
- `20:51:16`   t+ 4min flows=74/207 (35.7%) pages=2636 series=1318000 stopped_early=True
- `20:51:16`     in-flight IVF            slice 4/8 attempts=1
- `20:55:16`   t+ 8min flows=154/207 (74.4%) pages=3750 series=1875000 stopped_early=True
- `20:59:16`   t+12min flows=189/207 (91.3%) pages=5183 series=2591500 stopped_early=True
- `21:03:16`   t+16min flows=205/207 (99.0%) pages=6477 series=3238500 stopped_early=True
- `21:07:17`   t+20min flows=207/207 (100.0%) pages=6481 series=3240500 stopped_early=False
- `21:07:17`   ALL ECB FLOWS INDEXED
- `21:07:17`   failed_flows=0  errors=0  holes=0
## P2 integrity on the bracket predicate

- `21:07:18`   bracket 6481 <= 6481 <= 6481 : CLEAN
- `21:07:18`   6481 objects, 2.33 GB, avg page 351 KB
- `21:07:18`   manifest: series_extracted=3,240,832 n_pages=6481 pages=6,481 pages_bytes=2.33 GB flows_parsed=207/574
## P3 the card

- `21:07:18`   BEFORE {"series": 1318290, "n_keys": 3392, "total_mb": 3664.99}
- `21:07:18`   catalog code fresh (2026-08-29T20:47:24.000+0000)
- `21:07:18`   catalog Event invoke sent (sync invokes drop the connection on a run this long)
- `21:13:20`   hub rewritten after 360s
- `21:13:20`   AFTER series.count=3,240,832 counted=True
- `21:13:20`   n_keys=7,237 total_mb=4930.34
- `21:13:20`   derived={"source": "data/providers/ecb/series-manifest.json", "objects": 6481, "bytes": 2330628602, "mb": 2330.63, "freshest_h": -0.1}
- `21:13:20`   note=None
## P4 hub totals + coverage

- `21:13:20`   keys 1,907,743 -> 1,911,590
- `21:13:20`   gb   528.86 -> 530.12
- `21:13:20`   datasets 800999 -> 801001 (must NOT absorb series)
- `21:13:20`   ecb       series_count=3,240,832 n_keys=7,237 coverage_pct=None
- `21:13:20`   eurostat  series_count=564,204,235 n_keys=1,136,599 coverage_pct=100.0
- `21:13:20`   -> data/ops/ecb-series-lane.json
- `21:13:20` ops 5045 GREEN -- ECB indexed and its card tells the truth

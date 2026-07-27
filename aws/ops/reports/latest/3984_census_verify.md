# ops 3984 — pure verifier: is the census finally whole?

**Status:** failure  
**Duration:** 363.0s  
**Finished:** 2026-07-27T21:25:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | artifacts | artifacts_fresh_48h | artifacts_stale | artifacts_truncated_by_time_budget | conflicts | cron | elapsed_s | families | fred_us10y | fully_attributed | gap_candidates | generated_at | keyed_paths | marker | metric_directory_n | mislabels | page_bytes | page_markers | parse_errors | scalar_paths | schedule_state | source_families |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 839.7 |  |  |  |  | 2026-07-27T18:29:04.828087+00:00 |  | data-census v1.1 ops3978 4mb-cap |  |  |  |  |  |  |  |  |
| 170.4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 10022 | 394 | 1 | 30375 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | 729243 |  |  |
|  |  |  |  |  |  |  |  |  |  | 0 |  |  | False |  | 0 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {} |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 |
|  |  |  |  |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 0 |  |  |  |  |  | 0 |  |  |  |  | 0 |  |  |  |  |  |  |
|  |  |  |  |  |  | cron(45 12 * * ? *) |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ENABLED |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14181 | 5/5 |  |  |  |  |

## Log
## A. the artifact (short poll for the v1.6 write)

- `21:19:27`   [0] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:19:57`   [1] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:20:27`   [2] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:20:58`   [3] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:21:28`   [4] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:21:58`   [5] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:22:28`   [6] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:22:59`   [7] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:23:29`   [8] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:23:59`   [9] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:24:29`   [10] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:24:59`   [11] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
## B. totals + directory

## C. by_source — the locating scheme

## D. detectors (first run with a fed keyed walk)

## E. schedule + page

- `21:25:30` ✗   artifact is v1.5
- `21:25:30` ✗   written after the v1.6 invoke
- `21:25:30` ✅   >=100 artifacts walked
- `21:25:30` ✅   >=2000 scalar paths
- `21:25:30` ✗   metric directory >=50
- `21:25:30` ✗   >=50 fully attributed (name+source+engine)
- `21:25:30` ✗   keyed-list walk landed
- `21:25:30` ✗   by_source >=5 families
- `21:25:30` ✗   FRED >=40 metrics
- `21:25:30` ✗   US 10Y locatable under FRED
- `21:25:30` ✅   detectors present
- `21:25:30` ✅   schedule ENABLED
- `21:25:30` ✅   page v3 live at edge
- `21:25:30` ✗ FAILED: ['artifact is v1.5', 'written after the v1.6 invoke', 'metric directory >=50', '>=50 fully attributed (name+source+engine)', 'keyed-list walk landed', 'by_source >=5 families', 'FRED >=40 metrics', 'US 10Y locatable under FRED']

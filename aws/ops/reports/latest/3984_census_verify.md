# ops 3984 — pure verifier: is the census finally whole?

**Status:** failure  
**Duration:** 1.5s  
**Finished:** 2026-07-27T21:13:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | artifacts | artifacts_fresh_48h | artifacts_stale | artifacts_truncated_by_time_budget | conflicts | cron | elapsed_s | families | fred_us10y | fully_attributed | gap_candidates | generated_at | keyed_paths | marker | metric_directory_n | mislabels | page_bytes | page_markers | parse_errors | scalar_paths | schedule_state | source_families |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 839.7 |  |  |  |  | 2026-07-27T18:29:04.828087+00:00 |  | data-census v1.1 ops3978 4mb-cap |  |  |  |  |  |  |  |  |
| 164.4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 10022 | 394 | 1 | 30375 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | 729243 |  |  |
|  |  |  |  |  |  |  |  |  |  | 0 |  |  | False |  | 0 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {} |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 |
|  |  |  |  |  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 0 |  |  |  |  |  | 0 |  |  |  |  | 0 |  |  |  |  |  |  |
|  |  |  |  |  |  | cron(45 12 * * ? *) |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ENABLED |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14181 | 5/5 |  |  |  |  |

## Log
## A. the artifact

## B. totals + directory

## C. by_source — the locating scheme

## D. detectors (first run with a fed keyed walk)

## E. schedule + page

- `21:13:29` ✗   artifact is v1.5
- `21:13:29` ✗   written after the 20:33 enforced invoke
- `21:13:29` ✅   >=100 artifacts walked
- `21:13:29` ✅   >=2000 scalar paths
- `21:13:29` ✗   metric directory >=50
- `21:13:29` ✗   >=50 fully attributed (name+source+engine)
- `21:13:29` ✗   keyed-list walk landed
- `21:13:29` ✗   by_source >=5 families
- `21:13:29` ✗   FRED >=40 metrics
- `21:13:29` ✗   US 10Y locatable under FRED
- `21:13:29` ✅   detectors present
- `21:13:29` ✅   schedule ENABLED
- `21:13:29` ✅   page v3 live at edge
- `21:13:29` ✗ FAILED: ['artifact is v1.5', 'written after the 20:33 enforced invoke', 'metric directory >=50', '>=50 fully attributed (name+source+engine)', 'keyed-list walk landed', 'by_source >=5 families', 'FRED >=40 metrics', 'US 10Y locatable under FRED']

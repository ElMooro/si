# ops 5225 -- Release C2 gate: freshness monitor v2 + outcome-checker v4

**Status:** success  
**Duration:** 238.5s  
**Finished:** 2026-09-09T01:22:32+00:00  

## Data

| fresh | graded | invalid_empty | missing | pending | provenance | source_stale | stale | step | unscoreable | validated |
|---|---|---|---|---|---|---|---|---|---|---|
| 54 |  | 0 | 0 |  |  | 6 | 142 | freshness |  | 47 |
|  | 228 |  |  | 154 | {"s3": 220, "yahoo": 8} |  |  | outcome-checker | 0 |  |

## Log
## A. freshness monitor

- `01:18:33` LastModified 2026-09-09T00:27:55.000+0000
- `01:18:42` ✅ monitor invoke ok (FunctionError=None) b'{"statusCode": 200, "body": "{\\"n_tracked\\": 196, \\"stale\\": 142, \\"fresh\\": 54, \\"alerts\\": 0, \\"suppressed\\": 142, \\"elapsed_s\\": 7.94}"}'
- `01:18:42` ✅ state version 2.0.0 (got 2.0.0)
- `01:18:42` ✅ coverage block complete: {'keys_enumerated': 10000, 'bodies_validated': 47, 'expected_keys_checked': 1126, 'missing': 0, 'declared_absent_never_seen': 905, 'truncated_rules': ['data/']}
- `01:18:42` ✅ new counters present: invalid/empty=0 source_stale=6 missing=0
- `01:18:42` ✅ bodies were validated (47)
- `01:18:42` stale=142 fresh=54 invalid/empty=0 source_stale=6 missing=0 | truncated rules ['data/']
- `01:18:42`    SOURCE_STALE data/alpha-triage.json: artifact 842.95h, source 1273.73h (generated_at)
- `01:18:42`    SOURCE_STALE data/alpha-atlas.json: artifact 892.14h, source 893.91h (generated_at)
- `01:18:42`    SOURCE_STALE data/ai-council.json: artifact 843.43h, source 843.43h (generated_at)
- `01:18:42`    SOURCE_STALE data/activist-13d.json: artifact 699.28h, source 699.29h (as_of)
- `01:18:42`    SOURCE_STALE data/13f-clone-alpha.json: artifact 40.8h, source 40.8h (as_of)
- `01:18:42`    SOURCE_STALE data/apex-fusion.json: artifact 12.95h, source 12.95h (generated_at)
## B. outcome-checker

- `01:18:42` LastModified 2026-09-09T00:28:20.000+0000
- `01:21:25` ✅ checker invoke ok (FunctionError=None) b'{"statusCode": 200, "body": "{\\"processed\\": 90, \\"timestamp\\": \\"2026-09-09T01:21:25.709121+00:00\\"}"}'
- `01:22:32` scanned 140151 signals | graded this run 228 | pending 154 | unscoreable 0 | provenance {'s3': 220, 'yahoo': 8}
- `01:22:32` ✅ every window graded this run carries provenance and no zero-grade finalisation (0 violations) []
## verdict

- `01:22:32` ✅ GREEN -- freshness is judged on content and expected outputs; outcomes are graded at their own session with provenance

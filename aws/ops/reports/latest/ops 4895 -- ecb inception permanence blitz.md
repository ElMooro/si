# ops 4895 — ECB inception + permanence + blitz

**Status:** success  
**Duration:** 1620.3s  
**Finished:** 2026-08-18T18:18:54+00:00  

## Data

| clean | data_objects | ephemeral | fail_sample | flow | gz_mb | marker | max_year | memory | min_year | n_done | n_failures | n_failures_after | n_rules | n_statements | n_total | note | offenders | per_env | raw_bytes | remaining | remaining_flows | retried_ok | rounds | stage | status | verified |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 2048 |  |  |  | True |  | 2048 |  |  |  |  |  |  |  |  |  | 2 |  |  |  |  |  | zip-settle |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 26 |  | extended+2 |  |  |  |  |  |  |  | deny-delete |  | True |
| True |  |  |  |  |  |  |  |  |  |  |  |  | 7 |  |  |  | [] |  |  |  |  |  |  | lifecycle |  |  |
|  |  |  | {"CSEC": "TimeoutError: The read operation timed out", "DD": "HTTPError: HTTP Error 404: Not Found"} |  |  |  |  |  |  | 104 | 2 |  |  |  | 104 |  |  |  |  |  |  |  | 2 | blitz | COMPLETE |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 1 |  |  |  |  |  |  |  |  |  | 1 |  | fail-retry |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 31 | BSI,EXR,ICP,MIR,STS,BLS,BSP,CBD,CBD2,DWA,ESA,FVC |  | 3 | truncation |  |  |
|  |  |  |  | EXR |  |  | 2026 |  | 1920 |  |  |  |  |  |  |  |  |  | 473956352 |  |  |  |  | inception |  |  |
|  |  |  |  | BSI |  |  | 2099 |  | 1900 |  |  |  |  |  |  |  |  |  | 473956352 |  |  |  |  | inception |  |  |
|  |  |  |  | ICP |  |  | 2025 |  | 1985 |  |  |  |  |  |  |  |  |  | 473956352 |  |  |  |  | inception |  |  |
|  |  |  |  | MIR |  |  | 2099 |  | 1900 |  |  |  |  |  |  |  |  |  | 473956352 |  |  |  |  | inception |  |  |
|  |  |  |  | CISS |  |  | 2026 |  | 1973 |  |  |  |  |  |  |  |  |  | 83504090 |  |  |  |  | inception |  |  |
|  |  |  |  | FM |  |  | 2087 |  | 1900 |  |  |  |  |  |  |  |  |  | 33663205 |  |  |  |  | inception |  |  |
|  | 103 |  |  |  | 195.6 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | footprint |  |  |

## Log
- `17:51:55` blitz round 1: done 14/104, truncated 10
- `18:06:09` blitz round 2: done 14/104, truncated 10
- `18:14:56` trunc round 1: 33 flows (BOP,BSI,EXR,ICP,MIR,STS,BKN,BLS) at cap 450MB
- `18:16:12` trunc round 2: 32 flows (BSI,EXR,ICP,MIR,STS,BKN,BLS,BSP) at cap 450MB
- `18:17:27` trunc round 3: 31 flows (BSI,EXR,ICP,MIR,STS,BLS,BSP,CBD) at cap 450MB
- `18:18:54` VERDICT: PASS_WITH_PENDING · {"walker_deployed": "PASS", "deny_delete_policy": "PASS", "lifecycle_clean": "PASS", "walk_complete": "PASS", "truncation_zero": "PENDING", "inception_depth": "PASS"}
- `18:18:54` report written: aws/ops/reports/4895.json

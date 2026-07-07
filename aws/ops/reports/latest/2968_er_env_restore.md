## 0. Facts: surviving env + donor bundle

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-07-07T18:25:56+00:00  

## Data

| donor_keys_pulled | env_keys_after | env_n_after | surviving_keys | surviving_n |
|---|---|---|---|---|
|  |  |  | ['ANTHROPIC_API_KEY', 'FMP_KEY'] | 2 |
| ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY'] |  |  |  |  |
|  | ['ANTHROPIC_API_KEY', 'FMP_KEY', 'FRED_KEY', 'POLYGON_KEY'] | 4 |  |  |

## Log
## 1. Fill-gaps merge + config update + re-read assert

- `18:25:56` ✗ bundle still thin after restore: 4 vars
- `18:25:56` FAILS=1 WARNS=0
- `18:25:56` report written: /home/runner/work/si/si/aws/ops/reports/2968.json

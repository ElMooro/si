## 0. Context-feed probes from the runner

**Status:** success  
**Duration:** 18.9s  
**Finished:** 2026-07-07T18:52:35+00:00  

## Data

| body | candidates_n | compass_universe_n | cron | crypto_cycle_present | dropped_n | env_keys | env_n | finviz_industries | history | invoke_seconds | llm_status | memory_mb | month | month_read | rule | rule_state | state | status | timeout_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 18 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 144 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | ['ANTHROPIC_API_KEY', 'FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] | 5 |  |  |  |  | 256 |  |  |  |  | Active |  | 180 |
|  |  |  | cron(30 6 1 * ? *) |  |  |  |  |  |  |  |  |  |  |  | asset-discovery-monthly | ENABLED |  |  |  |
| {"ok": true, "month": "2026-07", "llm_status": "GATED_OR_DOWN", "candidates": 0} |  |  |  |  |  |  |  |  |  | 2.0 |  |  |  |  |  |  |  | 200 |  |
|  | 0 |  |  |  | 0 |  |  |  |  |  | GATED_OR_DOWN |  | 2026-07 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | discovery/history/2026-07.json |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. Wait for deploy-lambdas: function + ANTHROPIC key

## 2. Synchronous first run (this month's record)

## 3. Hard verify data/asset-discovery.json

- `18:52:35` ✅ discovery live: 2026-07 llm=GATED_OR_DOWN candidates=0 (none) -- ''
- `18:52:35` FAILS=0 WARNS=1
- `18:52:35` report written: /home/runner/work/si/si/aws/ops/reports/2969.json

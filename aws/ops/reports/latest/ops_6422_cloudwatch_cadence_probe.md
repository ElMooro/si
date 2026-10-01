
**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-10-01T18:03:41+00:00  

## Data

| api_calls | aws_writes | billing_reads | binding | cadence | code_sha256 | completed | exact_function_match | exact_function_matches | exists | function | kind | log_reads | max_event_age_seconds | max_retries | memory_mb | metric_queries | pagination_incomplete | runtime | scope | state | target_count | targets_complete | timeout_seconds | timezone |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | dj1flp3431234xiju18UnXRP6gUx0Uut+vmyB39z2Rg= |  |  |  | True | justhodl-fleet-error-monitor |  |  |  |  | 512 |  |  | python3.12 |  | Active |  |  | 180 |  |
|  |  |  |  |  | StIvK0wTNu7J4Hfa/c7BnSpP6X6uo03982nWfJrKMK8= |  |  |  | True | justhodl-health-monitor |  |  |  |  | 256 |  |  | python3.12 |  | Active |  |  | 300 |  |
|  |  |  |  |  | yXruD4uZ8ptFSxwc1G7QjLkGoUy89iH1zPN+8CGrzFg= |  |  |  | True | justhodl-cost-anomaly |  |  |  |  | 512 |  |  | python3.12 |  | Active |  |  | 300 |  |
|  |  |  |  |  | g6iL9OHe9pRUu7w4FyVcmXQvpvc5B4clbEIdP0zNYvc= |  |  |  | True | justhodl-fleet-integrity |  |  |  |  | 1024 |  |  | python3.12 |  | Active |  |  | 900 |  |
|  |  |  | cost-anomaly-daily | cron(0 9 * * ? *) |  |  |  | ['justhodl-cost-anomaly'] |  |  | events |  |  |  |  |  |  |  |  | ENABLED | 1 | True |  |  |
|  |  |  | fleet-error-monitor-5min | rate(5 minutes) |  |  |  | ['justhodl-fleet-error-monitor'] |  |  | events |  |  |  |  |  |  |  |  | ENABLED | 1 | True |  |  |
|  |  |  | justhodl-d1-scan-daily | cron(0 5 * * ? *) |  |  |  | ['justhodl-fleet-integrity'] |  |  | events |  |  |  |  |  |  |  |  | ENABLED | 1 | True |  |  |
|  |  |  | justhodl-fleet-error-6h | rate(1 hour) |  |  |  | ['justhodl-fleet-error-monitor'] |  |  | events |  |  |  |  |  |  |  |  | DISABLED | 1 | True |  |  |
|  |  |  | justhodl-fleet-integrity-weekly | cron(0 8 ? * MON *) |  |  |  | ['justhodl-fleet-integrity'] |  |  | events |  |  |  |  |  |  |  |  | ENABLED | 1 | True |  |  |
|  |  |  | fleet-error-monitor-sched | rate(24 hours) |  |  | justhodl-fleet-error-monitor |  | True |  | scheduler |  | 86400 | 185 |  |  |  |  |  | ENABLED |  |  |  | UTC |
| 19 | 0 | 0 |  |  |  | True |  |  |  |  |  | 0 |  |  |  | 0 | False |  | Four named functions; default-bus exact unqualified targets; one recorded default-group Scheduler. Alias/custom-bus/arbitrary-name bindings unsearched. |  |  |  |  |  |

## Log


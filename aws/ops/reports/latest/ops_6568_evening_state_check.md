# ops 6568 - evening state check (read-only)

**Status:** success  
**Duration:** 13.9s  
**Finished:** 2026-10-21T01:14:12+00:00  

## Data

| budget_actual | functions | governor_version | stage |
|---|---|---|---|
| 1037.103 | 904 | 1.5.1 | NORMAL |

## Log
- `01:14:00` governor: version 1.5.1, mode enforce, stage NORMAL, budget actual $1037.103 (forecast $1205.092), held {"CAP/lambda": 5}, capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api'], anomaly held []
- `01:14:01` justhodl-insider-cluster-scanner: reserved concurrency None; ledger stage None (before None)
- `01:14:01` justhodl-warroom-weights: reserved concurrency None; ledger stage None (before None)
- `01:14:01` justhodl-miss-calibrator: reserved concurrency 0; ledger stage CAP (before 1)
- `01:14:01` justhodl-event-coordinator: reserved concurrency 0; ledger stage CAP (before 10)
- `01:14:01` macro-financial-intelligence: reserved concurrency 0; ledger stage CAP (before None)
- `01:14:01` ofrapi: reserved concurrency 0; ledger stage CAP (before None)
- `01:14:01` treasury-api: reserved concurrency 0; ledger stage CAP (before None)
- `01:14:02` justhodl-insider-cluster-scanner-daily: cron(50 13 * * ? *) ENABLED -> justhodl-insider-cluster-scanner
- `01:14:02` alert relay marker: {"last_key": "spend-governor/events/20261021T002925.519278Z-restore.json", "updated": "2026-10-21T01:09:56.090524Z", "issues_opened": 1}
- `01:14:12` ✅ checked

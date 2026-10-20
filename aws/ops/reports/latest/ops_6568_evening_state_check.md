# ops 6568 - evening state check (read-only)

**Status:** success  
**Duration:** 12.2s  
**Finished:** 2026-10-20T21:44:32+00:00  

## Data

| budget_actual | functions | governor_version | stage |
|---|---|---|---|
| 1037.103 | 904 | 1.4.2 | FREEZE |

## Log
- `21:44:21` governor: version 1.4.2, mode enforce, stage FREEZE, budget actual $1037.103 (forecast $1205.092), held {"FREEZE/lambda": 911, "CAP/lambda": 5, "FREEZE/smasync": 1}, capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api'], anomaly held []
- `21:44:21` justhodl-insider-cluster-scanner: reserved concurrency None; ledger stage FREEZE (before None)
- `21:44:21` justhodl-warroom-weights: reserved concurrency None; ledger stage FREEZE (before None)
- `21:44:22` justhodl-miss-calibrator: reserved concurrency 0; ledger stage CAP (before 1)
- `21:44:22` justhodl-event-coordinator: reserved concurrency 0; ledger stage CAP (before 10)
- `21:44:22` macro-financial-intelligence: reserved concurrency 0; ledger stage CAP (before None)
- `21:44:22` ofrapi: reserved concurrency 0; ledger stage CAP (before None)
- `21:44:22` treasury-api: reserved concurrency 0; ledger stage CAP (before None)
- `21:44:22` justhodl-insider-cluster-scanner-daily: cron(50 13 * * ? *) ENABLED -> justhodl-insider-cluster-scanner
- `21:44:22` alert relay marker: {"last_key": "spend-governor/events/20261020T205554Z-operator-hold.json", "updated": "2026-10-20T21:32:30.882740Z", "issues_opened": 2}
- `21:44:32` ✅ checked

# ops 6568 - evening state check (read-only)

**Status:** success  
**Duration:** 13.2s  
**Finished:** 2026-10-20T22:12:13+00:00  

## Data

| budget_actual | functions | governor_version | stage |
|---|---|---|---|
| 1037.103 | 904 | 1.4.2 | FREEZE |

## Log
- `22:12:00` governor: version 1.4.2, mode enforce, stage FREEZE, budget actual $1037.103 (forecast $1205.092), held {"FREEZE/lambda": 911, "CAP/lambda": 5, "FREEZE/smasync": 1}, capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api'], anomaly held []
- `22:12:01` justhodl-insider-cluster-scanner: reserved concurrency 0; ledger stage FREEZE (before None)
- `22:12:01` justhodl-warroom-weights: reserved concurrency 0; ledger stage FREEZE (before None)
- `22:12:01` justhodl-miss-calibrator: reserved concurrency 0; ledger stage CAP (before 1)
- `22:12:01` justhodl-event-coordinator: reserved concurrency 0; ledger stage CAP (before 10)
- `22:12:01` macro-financial-intelligence: reserved concurrency 0; ledger stage CAP (before None)
- `22:12:01` ofrapi: reserved concurrency 0; ledger stage CAP (before None)
- `22:12:02` treasury-api: reserved concurrency 0; ledger stage CAP (before None)
- `22:12:02` justhodl-insider-cluster-scanner-daily: cron(50 13 * * ? *) ENABLED -> justhodl-insider-cluster-scanner
- `22:12:02` alert relay marker: {"last_key": "spend-governor/events/20261020T205554Z-operator-hold.json", "updated": "2026-10-20T21:32:30.882740Z", "issues_opened": 2}
- `22:12:13` ✅ checked

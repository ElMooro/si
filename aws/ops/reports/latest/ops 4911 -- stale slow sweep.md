# ops 4911 — deep unfreeze · eurostat autopsy · nyfed · midas union

**Status:** success  
**Duration:** 1015.7s  
**Finished:** 2026-08-19T17:36:37+00:00  

## Data

| deep_v14 | failures | have | inventory_n | lanes | ledger | midas_v11 | missing | mode | n_complete | newest_age_min | pending_hist | rearmed | recoverable_class | stage | still_missing | top | walker_euro |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True |  |  |  |  |  | True |  |  |  |  |  |  |  | settle |  |  | True |
|  |  |  |  |  |  |  |  |  | 30 |  | [["pending", 195], ["done", 34], ["slow_month", 4], ["err:HTTP502", 1]] |  |  | deep-before |  |  |  |
|  |  |  |  |  |  |  |  | backfill | 30 |  |  | null |  | deep-after |  |  |  |
|  |  |  |  |  | 6 |  |  |  |  |  |  |  | 0 | eurostat-autopsy |  | [["RemoteDisconnected: Remote end closed connec", 3], ["HTTPError: HTTP Error 401: Unauthorized", 2], ["ValueError: tiny 173b", 1]] |  |
|  |  |  |  |  |  |  |  |  |  | 0.5 |  |  |  | nyfed-markets |  |  |  |
|  |  | 34 | 50 |  |  |  | 16 |  |  |  |  |  |  | midas-union |  |  |  |
|  |  |  |  | {"dera": 69, "edgar": 80, "eiopa": 67} |  |  |  |  |  |  |  |  |  | hist-banker | 55 |  |  |
|  | 834 |  |  |  |  |  |  |  |  |  |  |  |  | oecd-drain |  |  |  |

## Log
- `17:36:37` VERDICT: PASS_WITH_PENDING · {"patches_deployed": "PASS", "deep_unfrozen": "PENDING", "eurostat_handled": "PASS", "nyfed_markets_fresh": "PASS", "midas_inventory_grew": "PASS", "snapshots": "PASS"}
- `17:36:37` report written: aws/ops/reports/4911.json

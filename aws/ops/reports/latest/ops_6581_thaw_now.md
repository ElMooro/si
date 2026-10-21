# ops 6581 - thaw now under the full guardrails (suppressed until the Nov 1 rollover)

**Status:** success  
**Duration:** 82.7s  
**Finished:** 2026-10-21T00:30:45+00:00  

## Data

| capped | remaining_in_ledger | restored | stage | suppress_until |
|---|---|---|---|---|
| 5 | 5 | {"restored": 894, "released": 17, "skipped_gone": 1} | NORMAL | 2026-11-01T00:29:25.519278+00:00 |

## Log
- `00:29:24` before: version 1.5.1, stage FREEZE, held {"FREEZE/lambda": 911, "CAP/lambda": 5, "FREEZE/smasync": 1}, capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api']
- `00:29:25` preconditions: governor 1.5.1, endpoints 0, runner sealed True -> True
- `00:30:33` restore: {"restored": {"restored": 894, "released": 17, "skipped_gone": 1}, "remaining_in_ledger": 5, "stage": "NORMAL", "suppress_until": "2026-11-01T00:29:25.519278+00:00"}
- `00:30:44` after: stage NORMAL, suppress_until 2026-11-01T00:29:25.519278+00:00, held {"CAP/lambda": 5}, capped ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'macro-financial-intelligence', 'ofrapi', 'treasury-api']
- `00:30:45` reserved concurrency after the thaw: {"justhodl-market-tape": 2, "justhodl-fundamental-census": null, "justhodl-share-flows": null, "justhodl-backlog": null, "justhodl-miss-calibrator": 0, "justhodl-event-coordinator": 0, "justhodl-insider-cluster-scanner": null, "justhodl-symdir": 2}
- `00:30:45` ✅ fleet thawed; CAP holds kept; brakes live

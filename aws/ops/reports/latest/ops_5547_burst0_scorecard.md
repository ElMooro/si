# ops 5547 -- burst 0 scorecard: job, verifier summary, kept rows, pass rate at K, cost per pass

**Status:** success  
**Duration:** 25.1s  
**Finished:** 2026-09-14T01:22:56+00:00  

## Data

| cost_spot_usd | kept_rows | pass_rate | passed | seen |
|---|---|---|---|---|
| 0.2155 | 399 | 0.6601 | 1165 | 1765 |

## Log
- `01:22:31` ✅ job jh-burst-gen0-20260914-005825 status=Completed training_s=596 billable_s=512 spot_savings=14.1% failure=
- `01:22:31` ✅ verifier summary: {"run_id": "34795385750", "rows_written": 399, "rows_existing": 0, "verdicts": 1161}
- `01:22:31` ✅ candidates seen=1765 passed=1165 failed=600 timeouts=1 malformed=0
- `01:22:56` ✅ rows: self_trace kept=399 of 863 verified rows total; tasks=464 K=4 candidates=1765
- `01:22:56` ✅ PASS RATE at K=4: 0.6601 (1165/1765); cost on-demand basis $0.2508, spot-billed basis $0.2155 -> cost per verified pass $0.00018 (spot) / $0.00022 (on-demand)
- `01:22:56` ✅ GREEN -- scorecard recorded

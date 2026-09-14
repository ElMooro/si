# ops 5550 -- burst 1 scorecard: job, verifier summary, kept rows, pass rate at K, cost per pass

**Status:** success  
**Duration:** 138.7s  
**Finished:** 2026-09-14T03:00:16+00:00  

## Data

| cost_spot_usd | kept_rows | pass_rate | passed | seen |
|---|---|---|---|---|
| 0.2453 | 754 | None | 0 | 0 |

## Log
- `02:57:57` ✅ job jh-burst-gen0b1-20260914-022049 status=Completed training_s=802 billable_s=583 spot_savings=27.3% failure=
- `02:57:58` ✅ verifier summary: {"run_id": null, "rows_written": null, "rows_existing": null, "verdicts": null}
- `02:57:58` ✅ candidates seen=None passed=None failed=None timeouts=None malformed=None
- `03:00:15` ✅ rows: self_trace kept=754 of 1324 verified rows total; tasks=570 K=4 candidates=0
- `03:00:15` ✅ PASS RATE at K=4: None (0/0); cost on-demand basis $0.3375, spot-billed basis $0.2453 -> cost per verified pass $None (spot) / $None (on-demand)
- `03:00:16` ✅ Gear B jobs: 0; latest: none yet
- `03:00:16` ✅ rows vs floor: 1324 verified rows total (self_trace 754) against min_sft_rows=1500 -> below floor by 176
- `03:00:16` ✅ GREEN -- scorecard recorded

# ops 4896 — ecb-deep launch + weekly rewalk

**Status:** success  
**Duration:** 903.1s  
**Finished:** 2026-08-18T18:39:38+00:00  

## Data

| action | first_periods | flows_touched | live | mode | n_complete | n_flows | name | parts_after | parts_before | rewalk_marker | stage | via |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | True | walker-settle |  |
|  |  |  | True |  |  |  |  |  |  |  | deep-fn | workflow |
| created |  |  |  |  |  |  | justhodl-ecb-deep-10min |  |  |  | schedule |  |
| created |  |  |  |  |  |  | justhodl-ecb-rewalk-weekly |  |  |  | schedule |  |
|  | {"EXR": "1982", "BSI": "1980-02"} | 2 |  | backfill | 2 | 31 |  | 20 | 0 |  | deep-backfill |  |

## Log
- `18:39:38` deep round 1 settled
- `18:39:38` VERDICT: PASS · {"walker_rewalk_deployed": "PASS", "deep_fn_live": "PASS", "schedulers_ensured": "PASS", "deep_backfill_started": "PASS"}
- `18:39:38` report written: aws/ops/reports/4896.json

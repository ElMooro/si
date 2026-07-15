## PRE-STATE

**Status:** success  
**Duration:** 5.1s  
**Finished:** 2026-07-15T01:16:52+00:00  

## Data

| RESULT | age_seconds | counts | engine | feed_after | feed_before | fn_state | fresh | function_error | generated_at | has_massive_env | invoke_status | last_update | log_tail | mem | n_most_bullish | n_top_picks | response | rule | schedule | state | timeout | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | {'exists': True, 'size': 1068, 'modified': '2026-07-14T13:45:12+00:00'} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | Active |  |  |  | False |  | Successful |  | 512 |  |  |  |  |  |  | 120 |  |  |
|  |  |  |  |  |  |  |  | None |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {'statusCode': 200, 'body': '{"upgrades": 0, "downgrades": 0, "pt_raises": 0, "guidance_raises": 0, "guidance_cuts": 0, "n_picks": 0, "top_pick": null, "elapsed_s": 0.4}'} |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | [analyst] ratings=0 guidance=0 insights=0
REPORT RequestId: 1e5436ae-5283-47a3-b606-6a092637d0cc	Duration: 515.17 ms	Billed Duration: 1010 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 493.88 ms	 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | {'exists': True, 'size': 1068, 'modified': '2026-07-15T01:16:50+00:00'} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | {'ratings_7d': 0, 'guidance_21d': 0, 'insights_7d': 0, 'upgrades': 0, 'downgrades': 0, 'pt_raises': 0, 'pt_cuts': 0, 'guidance_raises': 0, 'guidance_cuts': 0} | justhodl-analyst-actions |  |  |  |  |  | 2026-07-15T01:16:49.188453+00:00 |  |  |  |  |  | 0 | 0 |  |  |  |  |  | 1.0.0 |  |
|  | 3 |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | justhodl-analyst-actions-daily | cron(45 13 * * ? *) | ENABLED |  |  |  |
| PASS |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1 |

## Log
## FORCE INVOKE

## VERIFY FEED

- `01:16:52` ⚠ harvest empty (0 ratings/guidance/insights)
## SCHEDULE

- `01:16:52` ✅ daily schedule asserted: cron(45 13 * * ? *)
## VERDICT

- `01:16:52` ⚠ Benzinga harvest returned 0 across all feeds — check Massive entitlement (page still renders empty)
- `01:16:52` ✅ analyst-actions feed live + fresh; page will render on next load

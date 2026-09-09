# ops 5222 -- Release B1 gate: risk-sizer v2.0 capital authority / single-name cap / drawdown brake

**Status:** success  
**Duration:** 3.1s  
**Finished:** 2026-09-09T01:18:19+00:00  

## Data

| authority | authority_cap | entries_allowed | n | status | step | total |
|---|---|---|---|---|---|---|
| FRESH | 50.0 | True | 30 | OK | risk-sizer | 49.98 |

## Log
- `01:18:17` justhodl-risk-sizer LastModified 2026-09-09T00:26:21.000+0000 (push 2026-09-08T23:29:19+00:00)
- `01:18:19` invoke -> {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"regime\": \"NEUTRAL\", \"max_gross_exposure_pct\": 50.0, \"current_drawdown_pct\": 0.0, \"entries_allowed\": true, \"drawdown_multiplier\": 1.0, \"n_ideas\": 30, \"n_clusters\": 11, \"total_size_pct\": 49.98, \"n_warnin
- `01:18:19` ✅ invoke succeeded (FunctionError=None)
- `01:18:19` ✅ artifact v=2.0 (got 2.0)
- `01:18:19` ✅ status OK
- `01:18:19` ✅ data/risk-sizer.json mirror is the same run
- `01:18:19` authority FRESH mode=SELECTIVE cap=50.0 allows=True age=0.96h | book gross=0 available=50.0 | dd=0.0 (OK) | entries_allowed=True | hold=[]
- `01:18:19` ✅ every size <= 8.0% (max 2.25)
- `01:18:19` ✅ every cluster <= 25% (max 9.41)
- `01:18:19` ✅ total 49.98 <= available gross 50.00
- `01:18:19` ✅ final_constraint_check all ok: {'single_name_ok': True, 'cluster_ok': True, 'gross_ok': True, 'clamped': []}
- `01:18:19` ✅ entries blocked whenever the authority is not fresh / forbids entries / drawdown unknown
- `01:18:19` ✅ risk.html carries the authority strip at the edge
## verdict

- `01:18:19` ✅ GREEN -- risk-sizer obeys the binding authority, the published single-name cap and the drawdown brake

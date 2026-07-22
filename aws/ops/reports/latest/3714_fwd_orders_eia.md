# ops 3714 — forward-orders sidecar diagnosis + EIA key revival

**Status:** failure  
**Duration:** 23.5s  
**Finished:** 2026-07-22T17:53:44+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| data/forward-orders.json age=20.0h | A0_artifact_exists | True |
| reader-compatible keys present: NONE — shape mismatch (reader tries ('by_ticker', 'results', 'rankings', 'top_picks', 'all', 'scored')) | A1_reader_shape_hits | False |
| emulated readthrough join -> 0 tickers (sample=[]) | A2_join_nonempty | False |
| age=20.0h (schedule is cron(0 11 * * ? *) daily; >48h means the schedule is dead again, cf. ops 3642) | A3_artifact_fresh | True |
| had_key_before=True FunctionError=None key_present=False still_asking_for_key=False resp={"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Contr | B1_eia_key_set | False |

## Log
## A — forward-orders sidecar

- `17:53:21` A0_artifact_exists True
- `17:53:21` top-level keys: ['schema_version', 'method', 'generated_at', 'duration_s', 'n_universe', 'n_with_rpo', 'weights', 'top_25_by_score', 'all_results', 'notes']
- `17:53:21` A1_reader_shape_hits False
- `17:53:21` A2_join_nonempty False
- `17:53:21` A3_artifact_fresh True
- `17:53:21` ignored containers: ['weights', 'top_25_by_score', 'all_results']
## B — EIA key revival

- `17:53:44` B1_eia_key_set False
- `17:53:44` VERDICT: GAPS: A1_reader_shape_hits,A2_join_nonempty,B1_eia_key_set

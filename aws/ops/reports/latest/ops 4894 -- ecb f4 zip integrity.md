# ops 4894 — F4-zip integrity + arc non-regression

**Status:** success  
**Duration:** 2.8s  
**Finished:** 2026-08-18T17:39:07+00:00  

## Data

| accept_winner | detail | failures | n_dataflows | n_done | n_files | n_total | ok | progress_pct | raw_snapshot_in_zip | raw_snapshot_key | sample | snapshot_headable | stage | state_present | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 32 |  |  |  | True |  | _fred_shim.py,_sentry_lite.py,anthropic_shim.py,api_auth.py,benzinga.py,calibration.py,capital_flow.py,census_lib.py |  | zip-settle |  |  |
| no-accept |  |  | 104 |  |  |  | True |  |  | data/raw/ecb/2026-08-18/41711916d3ec.json.gz |  | True | catalog-invoke |  |  |
|  |  | 0 |  | 8 |  | 104 |  | 7.69 |  |  |  |  | walker-progress | True | converging |
|  | converging — 8/104 |  |  |  |  |  |  |  |  |  |  |  | sentinel |  | RUNNING |

## Log
- `17:39:07` VERDICT: PASS · gates={"shared_zip_restored": "PASS", "f4_snapshot_restored": "PASS", "walker_nonregression": "PASS", "sentinel_nonregression": "PASS"}
- `17:39:07` report written: aws/ops/reports/4894.json

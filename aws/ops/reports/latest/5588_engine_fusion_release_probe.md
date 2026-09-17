# ops 5588 -- engine-fusion release probe (read-only)

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-09-17T13:56:37+00:00  

## Data

| as_of | best_setups_evidence_level | coverage_ratio | freshness | freshness_basis | generated_at | independent_root_evidence | live_code_sha256 | live_last_modified | receipt_actor | receipt_code_sha256 | receipt_commit | receipt_deployed_at | receipt_run_id | receipt_workflow | settlement_max_age_hours | sf_active | sf_age_h | sf_as_of | sf_error | sf_freshness | sf_max_age_h | state | status | timestamp_paths | treasury_as_of | treasury_regime | treasury_scope | update_status | zip_sha256 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | Tf9pmmMRe6wPPd2BFkskUqPOkSkX3jOT3y6rVozdIzA= | 2026-09-17T02:22:24.000+0000 |  |  |  |  |  |  |  |  |  |  |  |  |  | Active |  |  |  |  |  | Successful |  |
|  |  |  |  |  |  |  |  |  | ElMooro | Tf9pmmMRe6wPPd2BFkskUqPOkSkX3jOT3y6rVozdIzA= | 002bbd1e1a38fee90ab9f6fe923bb10cba14b57f | 2026-09-17T02:22:14Z | 35174108077 | deploy-lambdas.yml |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | L3 |  |  | weekly_observation |  |  |  |  |  |  |  |  |  |  | 336 |  |  |  |  |  |  |  |  | ['generated_at', 'as_of', 'treasury.as_of'] |  |  |  |  | 4dff699a63117bac |
|  |  | 1.0 | {"fresh": 12, "stale": 0, "missing": 0, "invalid": 0, "unknown": 0} |  | 2026-09-17T13:15:10.763638+00:00 | 1 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | OK |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | 15.74 | 2026-09-16T21:30:32.172833+00:00 | None | FRESH | 336 |  |  |  |  |  |  |  |  |
| 2026-09-02 |  |  |  |  | 2026-09-16T21:30:32.172833+00:00 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-02 | CALM | US_TREASURY_INCLUDING_TIPS |  |  |

## Log
## 1. Live function vs release receipt

- `13:56:36` ✅ receipt CodeSha256 == live CodeSha256 -- AWS runs the receipted commit 002bbd1e1a38
## 2. Registry row inside the live zip

- `13:56:37` ✅ live bundle carries max_age_hours=336 for settlement_fails_treasury
## 3. data/engine-fusion.json -- the effect

- `13:56:37` ✅ settlement_fails_treasury is FRESH (age 15.74h vs SLA 336h)
- `13:56:37` non-FRESH sources now: none
## 4. data/settlement-fails.json timestamps

- `13:56:37` ✅ probe complete (no writes, no invokes)

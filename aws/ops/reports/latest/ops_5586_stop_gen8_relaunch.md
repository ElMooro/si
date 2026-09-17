# ops 5586 -- stop the doomed gen8 job (old bundle) and relaunch generation 1 on the planned-steps + time-budget bundle

**Status:** success  
**Duration:** 393.3s  
**Finished:** 2026-09-17T01:13:58+00:00  

## Data

| bundle | head | train_job |
|---|---|---|
|  | 921edf7be4 |  |
| train-23b4c7807e7682f8.tar.gz |  | jh-gearb-gen9-20260917-011357 |

## Log
- `01:07:25` ✅ pinned bundle: train-23b4c7807e7682f8.tar.gz (epochs=3); InProgress gearb jobs: ['jh-gearb-gen8-20260917-000739']
- `01:07:25` ✅ jh-gearb-gen8-20260917-000739: secondary=Starting started=None bundle=ng/bundles/train-0dbdf02a4a83d435.tar.gz time_budget_s=None -> OLD recipe, cannot finish inside the cap
- `01:10:06` ✅ jh-gearb-gen8-20260917-000739 -> Stopped (billable so far: None s)
- `01:10:06` ✅ gearb control: max_jobs_per_day=4 daily=$20.0 season=$600.0 (unchanged by this op)
- `01:13:58` ✅ tick status=200: {"refusal": null, "built": null, "launched": null, "polled": null, "error": null, "reason": null}
- `01:13:58` ✅ tick result: refusal=None built={"ok": true, "generation": 9, "kept": 570, "floor": 500, "missing_rows": null, "reason": null}
- `01:13:58` ✅ job jh-gearb-gen9-20260917-011357: status=InProgress hyperparameters epochs=1 max_steps(ceiling)=400 time_budget_s=9600 bundle=ng/bundles/train-23b4c7807e7682f8.tar.gz
- `01:13:58` ✅ GEN-1 TRAINING RELAUNCHED: {"schema_version": "gearb-job.v1", "job_name": "jh-gearb-gen9-20260917-011357", "kind": "sft", "generation": 9, "model_id": "qwen2-5-coder-7b-instruct", "instance_type": "ml.g5.2xlarge", "spot": true, "max_runtime_s": 10800, "usd_per_hour": 1.515, "cap_usd": 4.545, "price_source": "aws-price-list", "training_uri": "s3://justhodl-ai-857687956942/factory/gearb/datasets/gen-9/", "out_uri": "s3://justhodl-ai-857687956942/factory/champions/gen-9/", "eligibility_digest": "5295d3293faeba53f456c01c020855531270e74e72fa5b0b1752c78500a1400a", "train_sha256": "38f7eecedf107995b3266dca60337a5433f4420ca6685
- `01:13:58` ✅ GREEN -- training inside the refusal chain with a plan that fits the cap; next: ops 5584 extracts the adapter and launches the frozen exam

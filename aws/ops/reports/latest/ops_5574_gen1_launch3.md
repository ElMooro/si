# ops 5574 -- generation 1 (bundle re-pinned with the dataset contract): feasible family share for two families, then the Gear B tick (launch=true) via the owner route

**Status:** success  
**Duration:** 191.9s  
**Finished:** 2026-09-15T03:12:43+00:00  

## Data

| head |
|---|
| 1a02961bd4 |

## Log
- `03:09:31` ✅ bundle re-pinned: train-00432192e1b2b157.tar.gz -> train-0dbdf02a4a83d435.tar.gz; trainer accepts instruction/context/response=True; image kept=True
- `03:09:31` ✅ gearb control: {"max_family_share": 0.85, "min_sft_rows": 500, "max_jobs_per_day": 3, "daily_budget_usd": 20.0, "season_cap_usd": 600.0} -> max_family_share 0.85, min_sft_rows 500, max_jobs_per_day 3 (dated copy kept)
- `03:12:43` ✅ tick status=200: {"refusal": null, "built": null, "launched": null, "polled": null, "error": null, "reason": null}
- `03:12:43` ✅ tick result: refusal=None built={"ok": true, "generation": 3, "kept": 570, "floor": 500, "missing_rows": null, "reason": null}
- `03:12:43` ✅ GEN-1 TRAINING LAUNCHED: {"schema_version": "gearb-job.v1", "job_name": "jh-gearb-gen3-20260915-031242", "kind": "sft", "generation": 3, "model_id": "qwen2-5-coder-7b-instruct", "instance_type": "ml.g5.2xlarge", "spot": true, "max_runtime_s": 10800, "usd_per_hour": 1.515, "cap_usd": 4.545, "price_source": "aws-price-list", "training_uri": "s3://justhodl-ai-857687956942/factory/gearb/datasets/gen-3/", "out_uri": "s3://justhodl-ai-857687956942/factory/champions/gen-3/", "eligibility_digest": "6efdf8fbc6ab152c71077876b1e96c1ed93b0800f84a5f5eefdaa8172eaa8b8d", "train_sha256": "38f7eecedf107995b3266dca60337a5433f4420ca6685
- `03:12:43` ✅ GREEN -- generation 1 is training inside the refusal chain; next: adapter -> exam -> promotion by contract

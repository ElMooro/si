# ops 5965 -- the market model; the DPO relaunch

**Status:** success  
**Duration:** 24.7s  
**Finished:** 2026-09-23T15:37:04+00:00  

## Log
## 1. Market model (FMP history since 2005, crises embargoed, graded on the frozen holdout drills)

- `15:37:03` {"model_id": "quant-20260923T153702Z", "training": {"windows": 22995, "per_symbol": {"SPY": 4599, "QQQ": 4599, "IWM": 4599, "TLT": 4599, "GLD": 4599}, "crisis_windows": 1394, "first": "2006-11-06", "last": "2026-08-19", "holdout_drills": 55}, "holdout": {"model": {"n": 55, "score": 0.4273, "direction_acc": 0.4182, "regime_acc": 0.3273, "crisis_acc": 0.6, "crisis_brier": 0.2886}, "prior": {"n": 55, "score": 0.5127, "direction_acc": 0.5273, "regime_acc": 0.4182, "crisis_acc": 0.6182, "crisis_brier": 0.236}, "momentum": {"n": 55, "score": 0.4873, "direction_acc": 0.5091, "regime_acc": 0.3636, "crisis_acc": 0.6182, "crisis_brier": 0.236}}, "beats_prior": false}
- `15:37:03` ✅ market model rc=0 
## 2. Gear B since 15:24

- `15:37:03` 15:07 jh-gearb-gen27-20260923-150753 Stopped/Stopped spot=True mode=dpo 
- `15:37:04` last_tick: {"at": "2026-09-23T15:07:48.360289Z", "built": {"floor": 500, "generation": 27, "kept": 2010, "missing_rows": null, "ok": true, "reason": null}, "launched": {"cap_usd": 4.545, "eligibility_digest": "f0fbf61dd764adc93f1fb7a69c414f71ec908452dae01b1a7ba0494449699bd4", "generation": 27, "holdout_digest": "b926ab5e305c6ce5764adfcbc93ea5f89bc596424f31e26a4cdea54066781fd2", "instance_type": "ml.g5.2xlarge", "job_name": "jh-gearb-gen27-20260923-150753", "kind": "dpo", "launched_at": "2026-09-23T15:07:53.161050Z", "max_runtime_s": 10800, "model_id": "qwen2-5-coder-7b-instruct", "out_uri": "s3://justhodl-ai-857687956942/factory/champions/gen-27/", "price_source": "aws-price-list", "schema_version": "gearb-job.v1", "spot": true, "status": "launching", "train_sha256": "daa94969f4f1da9db9d15fc5e208166032b0aa841af26362b2f341b755914ff7", "training_uri": "s3://justhodl-ai-857687956942/factory/gearb/datasets/gen-27/", "usd_per_hour": 1.515, "version": "gear-b.1"}, "refusal": null, "capacity_stops": null, "merged": null}

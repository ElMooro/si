# ops 5303 -- AI relaunch v1.2.1: SageMaker-safe names (5302 config name ended on a dash), real metals/crypto keys, on-demand LLM with a bounded direct fallback (5302 read came back empty)

**Status:** failure  
**Duration:** 860.5s  
**Finished:** 2026-09-09T20:19:48+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. IAM

- `20:05:28`    justhodl-sagemaker-execution-role exists
- `20:05:28` ✅    AmazonSageMakerFullAccess attached to justhodl-sagemaker-execution-role
- `20:05:28` ✅    inline bucket/ecr/logs policy on justhodl-sagemaker-execution-role
- `20:05:28` ✅    control policy justhodl-ai-sagemaker-control on lambda-execution-role
## 2. private ML bucket

- `20:05:29`    justhodl-ai-857687956942 exists
- `20:05:29` ✅    public access block ok
- `20:05:29` ✅    default encryption ok
- `20:05:29` ✅    lifecycle ok
- `20:05:29` ✅    tags ok
## 3. Lambda justhodl-ai

- `20:05:30`   Lambda exists — updating
- `20:05:33` ✅   ✓ updated justhodl-ai
- `20:05:39`    state Active/Successful 3008MB/900s url https://kedjcxg4bsjdpe3ebib6gnj3ee0yphhm.lambda-url.us-east-1.on.aws/
- `20:05:39` ✅    control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)
- `20:05:40` ✅    unauthenticated POST to the Function URL -> HTTP 401 (gate holds)
## 4. schedule (EventBridge Scheduler)

- `20:05:40` ✅    justhodl-ai-inventory updated cron(7 * * * ? *)
- `20:05:40` ✅    justhodl-ai-market-read updated cron(45 5 * * ? *)
## 5. inventory (async + poll data/ai.json)

- `20:05:56`    v1.2.1 in 1.6s | domains 2 apps 0 endpoints 0 models 6 jobs 0 notebooks 0 feature_groups 0 clusters 0
- `20:05:56`       domain d-yxi0afwkz879 QuickSetupDomain-20250528T180672 status InService
- `20:05:56`       domain d-60hhwdwn03rz QuickSetupDomain-20250528T094736 status InService
- `20:05:56`    catalog 37 cards; article RoBERTa-SEC present ['mxnet-tcembedding-robertafin-base-uncased', 'mxnet-tcembedding-robertafin-base-wiki-uncased', 'mxnet-tcembedding-robertafin-large-uncased', 'mxnet-tcembedding-robertafin-large-wiki-uncased'] missing []; errors [] refresh_error None
- `20:05:56`    run-rate 0.0 | MTD 0.15 | ttl ledger []
- `20:05:56`    policy {"version": 1, "daily_budget_usd": 5.0, "endpoint_ttl_hours": 3.0, "idle_hours": 2.0, "training_max_runtime_s": 3600, "training_spot": true, "allowed_inference_instances": ["ml.t2.medium", "ml.t2.large", "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge
## 6. learn from the Brain (dataset -> embedding endpoint -> embed -> classifier -> serve -> infer)

- `20:05:57`    dataset 20260909T200556804256Z: 13051 rows (10456 train / 2592 validation) from 13051 notes; by_label {"philosophy": 9641, "thesis": 3407, "lesson": 1, "macro": 2}; dropped {"short": 0, "no_cat": 0, "dup": 0}
- `20:05:57`    embedding card: mxnet-tcembedding-robertafin-base-uncased
- `20:05:59`    deployed serverless: {"endpoint": "jh-ai-mxnet-tcembedding-robertafin-base-uncased", "artifact_how": "prefix-tar-cached", "artifact_probes": [{"candidate": "artifact", "uri": "s3://jumpstart-cache-prod-us-east-1/mxnet-tcembedding/mxnet-tcembedding-robertafin-base-uncased/artifacts/inference-prepack/v1.0.0/", "exists": true}], "model_data": "s3://justhodl-ai-857687956942/ai/models/repacked/mxnet-tcembedding-robertafin-base-uncased/model.tar.gz"}
- `20:19:08`    endpoint jh-ai-mxnet-tcembedding-robertafin-base-uncased -> Failed after 789s Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1
## 6b. market read

- `20:19:14`    read 20260909T201913Z in 2.3s | playbook False | sources {"fusion": "FRESH", "risk_gate": "FRESH", "khalid_risk": "FRESH", "katlin": "FRESH", "bottom": "FRESH", "fortress": "FRESH", "bonds": "FRESH", "crisis": "FRESH", "gbc": "FRESH", "regime_composite": "FRESH", "metals": "FRESH", "crypto": "FRESH", "btc_cycle": "FRESH", "scorecard": "FRESH", "brain": "FRESH"}
- `20:19:14`    stances: {"stocks": null, "bonds": null, "metals": null, "crypto": null}
- `20:19:14`    overall: 
- `20:19:14`    opportunities 0: 
- `20:19:14`    calls logged: []
## 7. page

- `20:19:15`    ai.html carries marker AI_DESK_V1 at the edge: True
- `20:19:38`    1440px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `20:19:48`     390px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `20:19:48` ⚠    dataset thin (13051 rows, smallest class 1) -- the classifier will be weak until the Brain has more labelled notes
- `20:19:48` ✗    brain pipeline: embedding endpoint not InService: Failed Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1
- `20:19:48` ✗    market read: LLM answer did not parse as JSON

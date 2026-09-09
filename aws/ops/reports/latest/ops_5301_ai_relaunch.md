# ops 5301 -- AI relaunch after the artifact-resolution fix (ops 5300 reached the hub deploy and hit a 404 on HostingArtifactUri)

**Status:** failure  
**Duration:** 1530.9s  
**Finished:** 2026-09-09T19:04:08+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. IAM

- `18:38:37`    justhodl-sagemaker-execution-role exists
- `18:38:37` ✅    AmazonSageMakerFullAccess attached to justhodl-sagemaker-execution-role
- `18:38:37` ✅    inline bucket/ecr/logs policy on justhodl-sagemaker-execution-role
- `18:38:37` ✅    control policy justhodl-ai-sagemaker-control on lambda-execution-role
## 2. private ML bucket

- `18:38:38`    justhodl-ai-857687956942 exists
- `18:38:38` ✅    public access block ok
- `18:38:38` ✅    default encryption ok
- `18:38:38` ✅    lifecycle ok
- `18:38:38` ✅    tags ok
## 3. Lambda justhodl-ai

- `18:38:39`   Lambda exists — updating
- `18:38:45` ✅   ✓ updated justhodl-ai
- `18:38:51`    state Active/Successful 3008MB/900s url https://kedjcxg4bsjdpe3ebib6gnj3ee0yphhm.lambda-url.us-east-1.on.aws/
- `18:38:51` ✅    control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)
- `18:38:52` ✅    unauthenticated POST to the Function URL -> HTTP 401 (gate holds)
## 4. schedule (EventBridge Scheduler)

- `18:38:52` ✅    justhodl-ai-inventory updated cron(7 * * * ? *)
## 5. inventory (async + poll data/ai.json)

- `18:39:08`    v1.0.1 in 1.6s | domains 2 apps 0 endpoints 0 models 2 jobs 0 notebooks 0 feature_groups 0 clusters 0
- `18:39:08`       domain d-yxi0afwkz879 QuickSetupDomain-20250528T180672 status InService
- `18:39:08`       domain d-60hhwdwn03rz QuickSetupDomain-20250528T094736 status InService
- `18:39:08`    catalog 37 cards; article RoBERTa-SEC present ['mxnet-tcembedding-robertafin-base-uncased', 'mxnet-tcembedding-robertafin-base-wiki-uncased', 'mxnet-tcembedding-robertafin-large-uncased', 'mxnet-tcembedding-robertafin-large-wiki-uncased'] missing []; errors [] refresh_error None
- `18:39:08`    run-rate 0.0 | MTD 0.15 | ttl ledger []
- `18:39:08`    policy {"version": 1, "daily_budget_usd": 5.0, "endpoint_ttl_hours": 3.0, "idle_hours": 2.0, "training_max_runtime_s": 3600, "training_spot": true, "allowed_inference_instances": ["ml.t2.medium", "ml.t2.large", "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge
## 6. learn from the Brain (dataset -> embedding endpoint -> embed -> classifier -> serve -> infer)

- `18:39:10`    dataset 20260909T183908955783Z: 13051 rows (10456 train / 2592 validation) from 13051 notes; by_label {"philosophy": 9641, "thesis": 3407, "lesson": 1, "macro": 2}; dropped {"short": 0, "no_cat": 0, "dup": 0}
- `18:39:10`    embedding card: mxnet-tcembedding-robertafin-base-uncased
- `18:39:14`    deployed real-time: {"endpoint": "jh-ai-mxnet-tcembedding-robertafin-base-uncased", "artifact_how": "artifact-prefix", "artifact_probes": [{"candidate": "artifact", "uri": "s3://jumpstart-cache-prod-us-east-1/mxnet-tcembedding/mxnet-tcembedding-robertafin-base-uncased/artifacts/inference-prepack/v1.0.0/", "exists": true}], "model_data": "s3://jumpstart-cache-prod-us-east-1/mxnet-tcembedding/mxnet-tcembedding-robertafin-base-uncased/artifacts/inference-prepack/v1.0.0/", "instance_type": "ml.m5.xlarge"}
- `19:03:46`    endpoint jh-ai-mxnet-tcembedding-robertafin-base-uncased -> Failed after 1472s The primary container for production variant AllTraffic did not pass the ping health check. Please check CloudWatch logs for this endpoint.
- `19:03:47` ✅    real-time embedding endpoint jh-ai-mxnet-tcembedding-robertafin-base-uncased deleted (no hourly bill left behind); the classifier endpoint is serverless
## 7. page

- `19:03:48`    ai.html carries marker AI_DESK_V1 at the edge: True
- `19:03:58`    1440px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 11, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `19:04:07`     390px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 11, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `19:04:08` ⚠    dataset thin (13051 rows, smallest class 1) -- the classifier will be weak until the Brain has more labelled notes
- `19:04:08` ⚠    serverless deploy of mxnet-tcembedding-robertafin-base-uncased failed (deploy: {"errorMessage": "An error occurred (ValidationException) when calling the CreateEndpointConfig operation: Model with containers that use ModelDataSourc) -- falling back to real-time ml.m5.xlarge with a 2h TTL
- `19:04:08` ✗    brain pipeline: embedding endpoint not InService: Failed The primary container for production variant AllTraffic did not pass the ping health check. Please check CloudWatch logs for this endpoint.

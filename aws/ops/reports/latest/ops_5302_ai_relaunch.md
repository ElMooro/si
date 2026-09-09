# ops 5302 -- AI relaunch v1.2.0: prepacked-prefix hosting fixed (script env + serverless tarball), ledger/LLM permissions, first market read, verdict on the page

**Status:** failure  
**Duration:** 84.3s  
**Finished:** 2026-09-09T20:00:02+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. IAM

- `19:58:38`    justhodl-sagemaker-execution-role exists
- `19:58:38` ✅    AmazonSageMakerFullAccess attached to justhodl-sagemaker-execution-role
- `19:58:38` ✅    inline bucket/ecr/logs policy on justhodl-sagemaker-execution-role
- `19:58:38` ✅    control policy justhodl-ai-sagemaker-control on lambda-execution-role
## 2. private ML bucket

- `19:58:39`    justhodl-ai-857687956942 exists
- `19:58:39` ✅    public access block ok
- `19:58:39` ✅    default encryption ok
- `19:58:39` ✅    lifecycle ok
- `19:58:39` ✅    tags ok
## 3. Lambda justhodl-ai

- `19:58:40`   Lambda exists — updating
- `19:58:43` ✅   ✓ updated justhodl-ai
- `19:58:49`    state Active/Successful 3008MB/900s url https://kedjcxg4bsjdpe3ebib6gnj3ee0yphhm.lambda-url.us-east-1.on.aws/
- `19:58:49` ✅    control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)
- `19:58:50` ✅    unauthenticated POST to the Function URL -> HTTP 401 (gate holds)
## 4. schedule (EventBridge Scheduler)

- `19:58:50` ✅    justhodl-ai-inventory updated cron(7 * * * ? *)
- `19:58:50` ✅    justhodl-ai-market-read created cron(45 5 * * ? *)
## 5. inventory (async + poll data/ai.json)

- `19:59:06`    v1.2.0 in 1.6s | domains 2 apps 0 endpoints 0 models 4 jobs 0 notebooks 0 feature_groups 0 clusters 0
- `19:59:06`       domain d-yxi0afwkz879 QuickSetupDomain-20250528T180672 status InService
- `19:59:06`       domain d-60hhwdwn03rz QuickSetupDomain-20250528T094736 status InService
- `19:59:06`    catalog 37 cards; article RoBERTa-SEC present ['mxnet-tcembedding-robertafin-base-uncased', 'mxnet-tcembedding-robertafin-base-wiki-uncased', 'mxnet-tcembedding-robertafin-large-uncased', 'mxnet-tcembedding-robertafin-large-wiki-uncased'] missing []; errors [] refresh_error None
- `19:59:06`    run-rate 0.0 | MTD 0.15 | ttl ledger []
- `19:59:06`    policy {"version": 1, "daily_budget_usd": 5.0, "endpoint_ttl_hours": 3.0, "idle_hours": 2.0, "training_max_runtime_s": 3600, "training_spot": true, "allowed_inference_instances": ["ml.t2.medium", "ml.t2.large", "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge
## 6. learn from the Brain (dataset -> embedding endpoint -> embed -> classifier -> serve -> infer)

- `19:59:07`    dataset 20260909T195906684125Z: 13051 rows (10456 train / 2592 validation) from 13051 notes; by_label {"philosophy": 9641, "thesis": 3407, "lesson": 1, "macro": 2}; dropped {"short": 0, "no_cat": 0, "dup": 0}
- `19:59:07`    embedding card: mxnet-tcembedding-robertafin-base-uncased
## 6b. market read

- `19:59:30`    read 20260909T195929Z in 1.6s | playbook False | sources {"fusion": "FRESH", "risk_gate": "FRESH", "khalid_risk": "FRESH", "katlin": "FRESH", "bottom": "FRESH", "fortress": "FRESH", "bonds": "FRESH", "crisis": "FRESH", "gbc": "FRESH", "regime_composite": "FRESH", "metals": "MISSING", "crypto": "MISSING", "btc_cycle": "MISSING", "scorecard": "FRESH", "brain": "FRESH"}
- `19:59:30`    stances: {"stocks": null, "bonds": null, "metals": null, "crypto": null}
- `19:59:30`    overall: 
- `19:59:30`    opportunities 0: 
- `19:59:30`    calls logged: []
## 7. page

- `19:59:30`    ai.html carries marker AI_DESK_V1 at the edge: True
- `19:59:52`    1440px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `20:00:02`     390px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `20:00:02` ⚠    dataset thin (13051 rows, smallest class 1) -- the classifier will be weak until the Brain has more labelled notes
- `20:00:02` ⚠    serverless deploy of mxnet-tcembedding-robertafin-base-uncased failed (deploy: {"errorMessage": "An error occurred (ValidationException) when calling the CreateEndpointConfig operation: 1 validation error detected: Value 'jh-ai-mxn) -- falling back to real-time ml.m5.xlarge with a 2h TTL
- `20:00:02` ⚠    market read board: ['metals', 'crypto', 'btc_cycle'] not FRESH (named in the read's data gaps)
- `20:00:02` ✗    brain pipeline: deploy: {"errorMessage": "An error occurred (ValidationException) when calling the CreateEndpointConfig operation: 1 validation error detected: Value 'jh-ai-mxnet-tcembedding-robertafin-base-uncased-cfg-8983967612-' at 'endpointConfigName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9](-*[a-zA-Z0-9]){0,62}", "errorType": "ClientError", "requestId": "5cc11
- `20:00:02` ✗    market read: LLM answer did not parse as JSON

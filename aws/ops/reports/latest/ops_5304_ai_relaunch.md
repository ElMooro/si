# ops 5304 -- AI relaunch v1.2.2: paged endpoint log tails, embedding-card fallback ladder (RoBERTa-SEC -> BGE -> MiniLM), LLM path diagnostics

**Status:** failure  
**Duration:** 2414.5s  
**Finished:** 2026-09-09T21:06:02+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. IAM

- `20:25:48`    justhodl-sagemaker-execution-role exists
- `20:25:48` ✅    AmazonSageMakerFullAccess attached to justhodl-sagemaker-execution-role
- `20:25:48` ✅    inline bucket/ecr/logs policy on justhodl-sagemaker-execution-role
- `20:25:48` ✅    control policy justhodl-ai-sagemaker-control on lambda-execution-role
## 2. private ML bucket

- `20:25:48`    justhodl-ai-857687956942 exists
- `20:25:49` ✅    public access block ok
- `20:25:49` ✅    default encryption ok
- `20:25:49` ✅    lifecycle ok
- `20:25:49` ✅    tags ok
## 3. Lambda justhodl-ai

- `20:25:50`   Lambda exists — updating
- `20:25:53` ✅   ✓ updated justhodl-ai
- `20:25:59`    state Active/Successful 3008MB/900s url https://kedjcxg4bsjdpe3ebib6gnj3ee0yphhm.lambda-url.us-east-1.on.aws/
- `20:26:00` ✅    control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)
- `20:26:00` ✅    unauthenticated POST to the Function URL -> HTTP 401 (gate holds)
## 4. schedule (EventBridge Scheduler)

- `20:26:01` ✅    justhodl-ai-inventory updated cron(7 * * * ? *)
- `20:26:01` ✅    justhodl-ai-market-read updated cron(45 5 * * ? *)
## 5. inventory (async + poll data/ai.json)

- `20:26:16`    v1.2.1 in 1.8s | domains 2 apps 0 endpoints 1 models 7 jobs 0 notebooks 0 feature_groups 0 clusters 0
- `20:26:16`       domain d-yxi0afwkz879 QuickSetupDomain-20250528T180672 status InService
- `20:26:16`       domain d-60hhwdwn03rz QuickSetupDomain-20250528T094736 status InService
- `20:26:16`       endpoint jh-ai-mxnet-tcembedding-robertafin-base-uncased Failed {} tags {"justhodl-ai-managed": "true", "justhodl-ai-purpose": "hub:mxnet-tcembedding-robertafin-base-uncased", "justhodl": "ai"
- `20:26:16`    catalog 37 cards; article RoBERTa-SEC present ['mxnet-tcembedding-robertafin-base-uncased', 'mxnet-tcembedding-robertafin-base-wiki-uncased', 'mxnet-tcembedding-robertafin-large-uncased', 'mxnet-tcembedding-robertafin-large-wiki-uncased'] missing []; errors [] refresh_error None
- `20:26:16`    run-rate 0.0 | MTD 0.15 | ttl ledger [{"endpoint": "jh-ai-mxnet-tcembedding-robertafin-base-uncased", "managed": true, "action": "keep", "reason": "", "age_hours": 0.33, "ttl_hours": 3.0, "invocations_window": 0.0}]
- `20:26:16`    policy {"version": 1, "daily_budget_usd": 5.0, "endpoint_ttl_hours": 3.0, "idle_hours": 2.0, "training_max_runtime_s": 3600, "training_spot": true, "allowed_inference_instances": ["ml.t2.medium", "ml.t2.large", "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge
## 6. learn from the Brain (dataset -> embedding endpoint -> embed -> classifier -> serve -> infer)

- `20:26:18`    dataset 20260909T202617516695Z: 13051 rows (10456 train / 2592 validation) from 13051 notes; by_label {"philosophy": 9641, "thesis": 3407, "lesson": 1, "macro": 2}; dropped {"short": 0, "no_cat": 0, "dup": 0}
- `20:26:18`    embedding card ladder: ['mxnet-tcembedding-robertafin-base-uncased', 'huggingface-textembedding-bge-base-en-v1-5', 'huggingface-textembedding-all-MiniLM-L6-v2', 'huggingface-sentencesimilarity-bge-base-en-v1-5']
- `20:26:18`    mxnet-tcembedding-robertafin-base-uncased: image 763104351884.dkr.ecr.us-east-1.amazonaws.com/mxnet-inference:1.9.0-gpu-py38 | env {"ENDPOINT_SERVER_TIMEOUT": "", "MODEL_CACHE_ROOT": "", "SAGEMAKER_CONTAINER_LOG_LEVEL": "", "SAGEMAKER_ENV": "", "SAGEMAKER_MODEL_SERVER_TIMEOUT": "", "SAGEMAKER_MODEL_SERVER_WORKERS": "", "SAGEMAKER_PROGRAM": "", "SAGEMAKER_SUBMIT_DIRECTORY": ""} | default instance ml.p3.2xlarge
- `20:26:25`    huggingface-textembedding-bge-base-en-v1-5: image 683313688378.dkr.ecr.us-east-1.amazonaws.com/tei:2.0.1-tei1.4.0-gpu-py310-cu122-ubuntu22.04 | env {"ENDPOINT_SERVER_TIMEOUT": "", "HF_MODEL_ID": "", "MODEL_CACHE_ROOT": "", "SAGEMAKER_CONTAINER_LOG_LEVEL": "", "SAGEMAKER_ENV": "", "SAGEMAKER_MODEL_SERVER_TIMEOUT": "", "SAGEMAKER_MODEL_SERVER_WORKERS": "", "SAGEMAKER_PROGRAM": "", "SAGEMAKER_SUBMIT_DIRECTORY": ""} | default instance ml.g5.xlarge
- `20:38:07`    deployed serverless: {"endpoint": "jh-ai-huggingface-textembedding-bge-base-en-v1-5", "artifact_how": "prefix-tar-repacked", "model_data": "s3://justhodl-ai-857687956942/ai/models/repacked/huggingface-textembedding-bge-base-en-v1-5/model.tar.gz"}
- `20:48:55`    endpoint jh-ai-huggingface-textembedding-bge-base-en-v1-5 -> Failed after 648s Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws/sagemaker/End
- `20:48:55`       log: == stream 7c809c9-a8542f67eb684177b0b7fa7303e52c65 (5 events, 0 flagged)
- `20:48:55`       log: CUDA compat package requires Nvidia driver ≤535.183.06
- `20:48:55`       log: cat: /proc/driver/nvidia/version: No such file or directory
- `20:48:55`       log: Current installed Nvidia driver version is
- `20:48:55`       log: Skip CUDA compat libs setup as newer Nvidia driver is installed
- `20:48:55`       log: HF_MODEL_ID must be set
- `20:48:56`    failed endpoint jh-ai-huggingface-textembedding-bge-base-en-v1-5 deleted
- `20:48:57`    huggingface-textembedding-all-MiniLM-L6-v2: image 683313688378.dkr.ecr.us-east-1.amazonaws.com/tei:2.0.1-tei1.4.0-gpu-py310-cu122-ubuntu22.04 | env {"ENDPOINT_SERVER_TIMEOUT": "", "HF_MODEL_ID": "", "MODEL_CACHE_ROOT": "", "SAGEMAKER_CONTAINER_LOG_LEVEL": "", "SAGEMAKER_ENV": "", "SAGEMAKER_MODEL_SERVER_TIMEOUT": "", "SAGEMAKER_MODEL_SERVER_WORKERS": "", "SAGEMAKER_PROGRAM": "", "SAGEMAKER_SUBMIT_DIRECTORY": ""} | default instance ml.g5.xlarge
- `20:49:15`    deployed serverless: {"endpoint": "jh-ai-huggingface-textembedding-all-minilm-l6-v2", "artifact_how": "prefix-tar-repacked", "model_data": "s3://justhodl-ai-857687956942/ai/models/repacked/huggingface-textembedding-all-MiniLM-L6-v2/model.tar.gz"}
- `20:50:56`    endpoint jh-ai-huggingface-textembedding-all-minilm-l6-v2 -> Failed after 101s Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws/sagemaker/End
- `20:50:56`       log: == stream 325e66b-b757e305267744cd91494b4a9f9440a0 (5 events, 0 flagged)
- `20:50:56`       log: CUDA compat package requires Nvidia driver ≤535.183.06
- `20:50:56`       log: cat: /proc/driver/nvidia/version: No such file or directory
- `20:50:56`       log: Current installed Nvidia driver version is
- `20:50:56`       log: Skip CUDA compat libs setup as newer Nvidia driver is installed
- `20:50:56`       log: HF_MODEL_ID must be set
- `20:50:57`    failed endpoint jh-ai-huggingface-textembedding-all-minilm-l6-v2 deleted
- `20:50:57`    huggingface-sentencesimilarity-bge-base-en-v1-5: image 763104351884.dkr.ecr.us-east-1.amazonaws.com/huggingface-pytorch-inference:1.13.1-transformers4.26.0-gpu-py39-cu117-ubuntu20.04 | env {"ENDPOINT_SERVER_TIMEOUT": "", "MODEL_CACHE_ROOT": "", "SAGEMAKER_CONTAINER_LOG_LEVEL": "", "SAGEMAKER_ENV": "", "SAGEMAKER_MODEL_SERVER_TIMEOUT": "", "SAGEMAKER_MODEL_SERVER_WORKERS": "", "SAGEMAKER_PROGRAM": "", "SAGEMAKER_SUBMIT_DIRECTORY": ""} | default instance ml.g5.2xlarge
- `20:54:59`    deployed serverless: {"endpoint": "jh-ai-huggingface-sentencesimilarity-bge-base-en-v1-5", "artifact_how": "prefix-tar-repacked", "model_data": "s3://justhodl-ai-857687956942/ai/models/repacked/huggingface-sentencesimilarity-bge-base-en-v1-5/model.tar.gz"}
- `21:05:26`    endpoint jh-ai-huggingface-sentencesimilarity-bge-base-en-v1-5 -> Failed after 628s Image size 13127985835 is greater than supported size 10737418240
- `21:05:26`       log: log tail unavailable: An error occurred (ResourceNotFoundException) when calling the DescribeLogStreams operation: The specified log group does not exist.
- `21:05:27`    failed endpoint jh-ai-huggingface-sentencesimilarity-bge-base-en-v1-5 deleted
## 6b. market read

- `21:05:31`    read 20260909T210529Z in 2.2s | playbook False | sources {"fusion": "FRESH", "risk_gate": "FRESH", "khalid_risk": "FRESH", "katlin": "FRESH", "bottom": "FRESH", "fortress": "FRESH", "bonds": "FRESH", "crisis": "FRESH", "gbc": "FRESH", "regime_composite": "FRESH", "metals": "FRESH", "crypto": "FRESH", "btc_cycle": "FRESH", "scorecard": "FRESH", "brain": "FRESH"}
- `21:05:31`    stances: {"stocks": null, "bonds": null, "metals": null, "crypto": null}
- `21:05:31`    llm_path direct-failed:HTTP Error 400: Bad Request | empty True | raw 
- `21:05:31`    overall: 
- `21:05:31`    opportunities 0: 
- `21:05:31`    calls logged: []
## 7. page

- `21:05:31`    ai.html carries marker AI_DESK_V1 at the edge: True
- `21:05:52`    1440px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `21:06:02`     390px: {"headline": "0 endpoints InService \u00b7 0 jobs running \u00b7 run-rate $0/day of a $5 budget", "legs": 6, "steps": 5, "done": 1, "tabs": 7, "rows": 1, "cards": 37, "helps": 14, "tiers": 4, "defs": 4, "pol": 10, "overflow": 0, "err": ""} errors=[]
- `21:06:02` ⚠    dataset thin (13051 rows, smallest class 1) -- the classifier will be weak until the Brain has more labelled notes
- `21:06:02` ⚠    serverless deploy of mxnet-tcembedding-robertafin-base-uncased failed (deploy: {"errorMessage": "An error occurred (ValidationException) when calling the CreateEndpoint operation: Cannot create already existing endpoint \"arn:aws:s) -- trying real-time ml.m5.xlarge with a 2h TTL
- `21:06:02` ⚠    real-time deploy of mxnet-tcembedding-robertafin-base-uncased failed too: deploy: {"errorMessage": "An error occurred (ValidationException) when calling the CreateEndpoint operation: Cannot create already existing endpoint \"arn:aws:s
- `21:06:02` ⚠    huggingface-textembedding-bge-base-en-v1-5 endpoint Failed: Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.ama
- `21:06:02` ⚠    huggingface-textembedding-all-MiniLM-L6-v2 endpoint Failed: Received server error (0) from model with message "An error occurred while handling request as the model process exited.". See https://us-east-1.console.aws.ama
- `21:06:02` ⚠    huggingface-sentencesimilarity-bge-base-en-v1-5 endpoint Failed: Image size 13127985835 is greater than supported size 10737418240
- `21:06:02` ✗    brain pipeline: no embedding card reached InService (ladder ['mxnet-tcembedding-robertafin-base-uncased', 'huggingface-textembedding-bge-base-en-v1-5', 'huggingface-textembedding-all-MiniLM-L6-v2', 'huggingface-sentencesimilarity-bge-base-en-v1-5']) -- log tails above
- `21:06:02` ✗    market read: LLM answer did not parse as JSON

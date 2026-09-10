# ops 5308 -- AI v2.1.0 (Perplexity governance modules + Claude pipeline/read/lessons) deployed in review mode on the live role; GET /inventory for the page; RoBERTa-SEC on GPU + CPU retrieval cards; learn-from-mistakes loop

**Status:** failure  
**Duration:** 1366.5s  
**Finished:** 2026-09-10T17:11:42+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. IAM

- `16:48:56`    justhodl-sagemaker-execution-role exists
- `16:48:56` ✅    AmazonSageMakerFullAccess attached to justhodl-sagemaker-execution-role
- `16:48:56` ✅    inline bucket/ecr/logs policy on justhodl-sagemaker-execution-role
- `16:48:56` ✅    control policy justhodl-ai-sagemaker-control on lambda-execution-role
## 2. private ML bucket

- `16:48:56`    justhodl-ai-857687956942 exists
- `16:48:56` ✅    public access block ok
- `16:48:57` ✅    default encryption ok
- `16:48:57` ✅    lifecycle ok
- `16:48:57` ✅    tags ok
## 3. Lambda justhodl-ai

- `16:48:57`   Lambda exists — updating
- `16:49:03` ✅   ✓ updated justhodl-ai
- `16:49:08`    state Active/Successful 3008MB/900s url https://kedjcxg4bsjdpe3ebib6gnj3ee0yphhm.lambda-url.us-east-1.on.aws/
- `16:49:08` ✅    control pointers written (private ai/control.json, public data/ai/control.json for the worker bridge)
- `16:49:09` ✅    unauthenticated POST to the Function URL -> HTTP 401 (gate holds)
## 4. schedule (EventBridge Scheduler)

- `16:49:10` ✅    justhodl-ai-inventory updated cron(7 * * * ? *)
- `16:49:10` ✅    justhodl-ai-market-read updated cron(45 5 * * ? *)
- `16:49:10` ✅    justhodl-ai-pipeline updated rate(10 minutes)
## 5. inventory (async + poll data/ai.json)

- `16:49:25`    v2.1.0 in 5.7s | domains 0 apps 0 endpoints 0 models 0 jobs 0 notebooks 0 feature_groups 0 clusters 0
- `16:49:25`    catalog 37 cards; article RoBERTa-SEC present ['mxnet-tcembedding-robertafin-base-uncased', 'mxnet-tcembedding-robertafin-base-wiki-uncased', 'mxnet-tcembedding-robertafin-large-uncased', 'mxnet-tcembedding-robertafin-large-wiki-uncased'] missing []; errors None refresh_error None
- `16:49:25`    run-rate 0.0 | MTD 1.03 | ttl ledger null
- `16:49:25`    policy {"daily_budget_usd": 5.0, "endpoint_ttl_hours": 3.0, "idle_hours": 2.0, "serverless_default": true}
## 6. Brain pipeline -- start + observe (the engine finishes it on its own 10-minute ticks)

- `16:49:29`    started 20260910T164926Z: stage wait_embedding | ladder ['mxnet-tcembedding-robertafin-base-uncased', 'tensorflow-tcembedding-bert-en-uncased-L-12-H-768-A-12-2', 'huggingface-sentencesimilarity-all-MiniLM-L6-v2']
- `16:55:30`    wait_embedding -> train | ERROR train: TRAINING_INPUT_BUCKET is not configured
- `17:01:31`    train -> failed | stage train failed 3 times: TRAINING_INPUT_BUCKET is not configured
- `17:01:31`    after 12 min: status failed stage failed | endpoint jh-ai-mxnet-tcembedding-robertafin-base-uncased | classifier None | retrieval None
- `17:01:31`    error: train: TRAINING_INPUT_BUCKET is not configured
- `17:01:31`    error: train: TRAINING_INPUT_BUCKET is not configured
- `17:01:31`    error: train: TRAINING_INPUT_BUCKET is not configured
## 7. page

- `17:11:42`    ai.html carries marker AI_DESK_V1 at the edge: False
- `17:11:42` ✗    GET /inventory through the Function URL: HTTP Error 400: Bad Request
- `17:11:42` ✗    pipeline failed at failed: stage train failed 3 times: TRAINING_INPUT_BUCKET is not configured
- `17:11:42` ✗    page deploy not observed at the edge

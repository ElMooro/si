# ops 6578 - SageMaker to zero; sealed deny policies against rogue compute

**Status:** failure  
**Duration:** 59.7s  
**Finished:** 2026-10-21T00:13:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| domains | endpoints_deleted | feature_groups | jobs_stopped | notebooks | roles_sealed | simulations_ok |
|---|---|---|---|---|---|---|
| 2 | 3 | 0 | 0 | 0 | 14 | False |

## Log
- `00:12:57` ✅ endpoint jh-owned-coder-async (InService) deleted; configuration saved
- `00:12:58` ✅ endpoint jh-ai-tensorflow-tcembedding-bert-en-uncased-l-12-h-768 (InService) deleted; configuration saved
- `00:12:59` ✅ endpoint jh-ai-clf-jh-ai-brain-clf-20260910-180837-297 (InService) deleted; configuration saved
- `00:12:59` endpoint config jh-owned-coder-cfg-16k-20260918200057 deleted
- `00:12:59` endpoint config jh-owned-coder-cfg-20260914-224817 deleted
- `00:13:00` endpoint config jh-owned-coder-cfg-20260914-172407 deleted
- `00:13:01` endpoint config jh-owned-coder-cfg-20260914-131255 deleted
- `00:13:01` endpoint config jh-owned-coder-cfg-20260914-125722 deleted
- `00:13:02` endpoint config jh-owned-coder-cfg-20260914-052158 deleted
- `00:13:03` endpoint config jh-owned-coder-cfg-20260914-032906 deleted
- `00:13:04` endpoint config jh-ai-tensorflow-tcembedding-bert-en-uncased-l-cfg-9065653439-2: ClientError
- `00:13:04` endpoint config jh-ai-clf-jh-ai-brain-clf-20260910-180837-297-cfg-9064841312-2: ClientError
- `00:13:04` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-9062450846-2: ClientError
- `00:13:07` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-9060733530-2: ClientError
- `00:13:09` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-9058967813-2: ClientError
- `00:13:09` endpoint config jh-ai-clf-jh-ai-brain-clf-20260910-032814-500-cfg-9011609846-2: ClientError
- `00:13:09` endpoint config jh-ai-huggingface-sentencesimilarity-all-minil-cfg-9008836461-4: ClientError
- `00:13:10` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-9007384418-2: ClientError
- `00:13:11` endpoint config jh-ai-huggingface-sentencesimilarity-bge-base-cfg-8992631668-4: ClientError
- `00:13:11` endpoint config jh-ai-huggingface-textembedding-all-minilm-l6-cfg-8991237602-2: ClientError
- `00:13:12` endpoint config jh-ai-huggingface-textembedding-bge-base-en-v1-cfg-8989862964-2: ClientError
- `00:13:12` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-8988367360-2: ClientError
- `00:13:17` endpoint config jh-ai-huggingface-sentencesimilarity-bge-base-cfg-8987297893-4: ClientError
- `00:13:17` endpoint config jh-ai-huggingface-textembedding-all-minilm-l6-cfg-8986953991-2: ClientError
- `00:13:17` endpoint config jh-ai-huggingface-textembedding-bge-base-en-v1-cfg-8986286051-6: ClientError
- `00:13:19` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-8985583596-4: ClientError
- `00:13:19` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-8985579623-2: ClientError
- `00:13:19` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncase-cfg-8984358816-2: ClientError
- `00:13:19` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncased-cfg-1788979152: ClientError
- `00:13:20` endpoint config jh-ai-mxnet-tcembedding-robertafin-base-uncased-cfg-1788977619: ClientError
- `00:13:20` in-flight jobs stopped: none
- `00:13:21` remaining: 0 notebook instances ([]), 2 studio domains, 0 feature groups (online store: []), 0 mlflow servers
- `00:13:21` ✅ AI engine switches: {"factory/control/gearb.json": {"enabled": false, "daily_budget_usd": 20.0}, "factory/control/inference.json": {"enabled": false}, "ai/policy.json": {"daily_budget_usd": 0}}
- `00:13:21` ✅ policy justhodl-deny-rogue-compute created (2621 chars)
- `00:13:21` ✅ policy justhodl-deny-fleet-self-modify created (1408 chars)
- `00:13:22` ✅ policy justhodl-deny-runner-escape created (861 chars)
- `00:13:22` ✅ runner group justhodl-runner-ecr: + justhodl-deny-rogue-compute
- `00:13:22` runner group attach justhodl-deny-runner-escape: LimitExceededException
- `00:13:35` Lambda execution roles (14): {"MacroFinancialIntelligenceRole": ["new", "new"], "bls-api-role": ["new", "new"], "chatgpt-agent-self-modify": ["new", "new"], "daily-liquidity-role": ["new", "new"], "economyapi-lambda-role": ["new", "new"], "justhodl-factory-grader-role": ["new", "new"], "justhodl-spend-governor-role": ["new", "skipped (governor)"], "justhodl-student-rsi-role": ["new", "new"], "lambda-execution-role": ["new", "new"], "lambda-role": ["new", "new"], "liquidity-agent-role-v2": ["new", "new"], "ofr-lambda-role": ["new", "new"], "openbb-lambda-role": ["new", "new"], "scrapeMacroData-role-9pweui52": ["new", "new"]}
- `00:13:55` github-actions-justhodl: denied ['explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'implicitDeny', 'allowed', 'explicitDeny'] / allowed-still ['allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed']  UNEXPECTED: [('sts:AssumeRole', 'implicitDeny'), ('iam:AddUserToGroup', 'allowed')]
- `00:13:55` lambda-execution-role: denied ['explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny', 'explicitDeny'] / allowed-still ['allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'allowed', 'implicitDeny', 'allowed', 'allowed', 'implicitDeny']
- `00:13:56` justhodl-spend-governor-role: denied ['explicitDeny', 'explicitDeny'] / allowed-still ['allowed', 'allowed', 'allowed', 'implicitDeny', 'allowed', 'implicitDeny', 'allowed']
- `00:13:56` ✗ a simulation disagreed with the design - review before relying on the seal

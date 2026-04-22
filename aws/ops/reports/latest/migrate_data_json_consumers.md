# Migrate 7 stale consumers: data.json → data/report.json

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-04-22T23:15:45+00:00  

## Data

| lambda_name | status | substitutions |
|---|---|---|
| justhodl-ai-chat | modified | 1 |
| justhodl-bloomberg-v8 | modified | 1 |
| justhodl-chat-api | modified | 1 |
| justhodl-crypto-intel | modified | 1 |
| justhodl-investor-agents | modified | 1 |
| justhodl-morning-intelligence | modified | 1 |
| justhodl-signal-logger | modified | 1 |

## Log
## justhodl-ai-chat

- `23:15:43`   ✓ aws/lambdas/justhodl-ai-chat/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-ai-chat: 1 substitution(s)
## justhodl-bloomberg-v8

- `23:15:43`   ✓ aws/lambdas/justhodl-bloomberg-v8/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-bloomberg-v8: 1 substitution(s)
## justhodl-chat-api

- `23:15:43`   ✓ aws/lambdas/justhodl-chat-api/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-chat-api: 1 substitution(s)
## justhodl-crypto-intel

- `23:15:43`   ✓ aws/lambdas/justhodl-crypto-intel/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-crypto-intel: 1 substitution(s)
## justhodl-investor-agents

- `23:15:43`   ✓ aws/lambdas/justhodl-investor-agents/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-investor-agents: 1 substitution(s)
## justhodl-morning-intelligence

- `23:15:43`   ✓ aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-morning-intelligence: 1 substitution(s)
## justhodl-signal-logger

- `23:15:43`   ✓ aws/lambdas/justhodl-signal-logger/source/lambda_function.py: 1 substitution(s)
- `23:15:43` ✅ justhodl-signal-logger: 1 substitution(s)
## Self-commit & push (no [skip-deploy] — deploys will trigger)

- `23:15:43`   Rebase: rc=128
- `23:15:43`   Staged diff:
- `23:15:43`     .../justhodl-ai-chat/source/lambda_function.py     |  472 +-
- `23:15:43`      .../source/lambda_function.py                      |    2 +-
- `23:15:43`      .../justhodl-chat-api/source/lambda_function.py    |    2 +-
- `23:15:43`      .../source/lambda_function.py                      | 7458 ++++++++++----------
- `23:15:43`      .../source/lambda_function.py                      |  390 +-
- `23:15:43`      .../source/lambda_function.py                      |    2 +-
- `23:15:43`      .../source/lambda_function.py                      |  344 +-
- `23:15:43`      7 files changed, 4335 insertions(+), 4335 deletions(-)
- `23:15:45` ✅   Pushed. deploy-lambdas.yml will redeploy 7 Lambda(s)
- `23:15:45` 
- `23:15:45` Next steps (automatic):
- `23:15:45`   - deploy-lambdas workflow detects changes in 7 source/ dirs
- `23:15:45`   - Each Lambda gets re-zipped and redeployed (~30s each)
- `23:15:45`   - After ~3 min: ai-chat and morning-intel stop showing [REGIME]/[DATA]
- `23:15:45` Done

# Liquidity agent source and publication proof

**Status:** failure  
**Duration:** 9.9s  
**Finished:** 2026-09-17T16:35:14+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5623_liquidity_agent_refresh.py", line 45, in main
    raise RuntimeError("Lambda refresh failed; inspect its CloudWatch log")
RuntimeError: Lambda refresh failed; inspect its CloudWatch log

```

## Data

| code_sha256 | commit | function |
|---|---|---|
| pFpJEiwSKluBxZ4xvb7g/rLr3Mz77CnSWTFlDO5ngoo= | 87c9822ae5e2107f690f2750ec073aab0d220b86 | justhodl-liquidity-agent |

## Log


## 1. Gate on CODE truth (LastModified), settle updates

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-07-12T02:37:52+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3134_bls_desk_land.py", line 119, in main
    rep.row(check="runner BLS key valid on v2", ok=key_ok, value=key_msg)
    ^^^^^^^
AttributeError: 'Report' object has no attribute 'row'. Did you mean: 'rows'?

```

## Data

| code_sha | handler | last_modified | memory | runtime | timeout |
|---|---|---|---|---|---|
| pB9bw4QY+bBX | lambda_bls_agent.lambda_handler | 2026-07-12T02:37:47.000+0000 | 512 | python3.9 | 300 |

## Log
- `02:37:51` code landed via deploy-lambdas -- gate PASS
## 2. Key probe + env/description/runtime sync


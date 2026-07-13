# ops 3179 — fused fleet verification

**Status:** failure  
**Duration:** 10.8s  
**Finished:** 2026-07-13T00:00:43+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3179_fusion_verify.py", line 67, in <module>
    for t in themes[:9]:
             ~~~~~~^^^^
KeyError: slice(None, 9, None)

```

## Data

| engines | firing | proven | themes |
|---|---|---|---|
| None | 20 | 0 | 10 |

## Log
## 1. Fusion bus

- `00:00:43` ── THEME PRESSURE (his 96 engines, pooled):

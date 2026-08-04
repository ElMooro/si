# ops 4356 — restore crypto-intel schedule + prove natural fire

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-08-04T03:56:58+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4356_crypto_schedule_restore.py", line 95, in <module>
    a = ((sname.get("target") or {}).get("arn")) or json.dumps(sname)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'str' object has no attribute 'get'

```

## Data

| expr | rule | state | target_ok | targets |
|---|---|---|---|---|
| rate(15 minutes) | justhodl-crypto-15min | ENABLED | True | 1 |

## Log
## 1. rule -> target -> permission

- `03:56:58` ✅ permission added
## 2. fleet triage: declared-but-missing rules (no mutations)


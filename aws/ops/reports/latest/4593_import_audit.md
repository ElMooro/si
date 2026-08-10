# ops 4593 — FRED import audit

**Status:** failure  
**Duration:** 0.3s  
**Finished:** 2026-08-10T23:37:04+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4593_import_audit.py", line 58, in main
    for name, p in sorted(pipes.items()):
                          ^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'items'

```

## Log
## 1. import-health — the badge and the five incidents

- `23:37:04` ✅   [health] import-health readable (ok)
- `23:37:04`   overall=ACTION_REQUIRED  sweep=2026-08-10T23:35:04+00:00

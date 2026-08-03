# ops 4334 -- the fingerprint becomes a tier

**Status:** failure  
**Duration:** 3.0s  
**Finished:** 2026-08-03T21:03:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4334_prime_convergence.py", line 57, in <module>
    or (d.get("compound") or {}).get("ranked")
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'list' object has no attribute 'get'

```

## Log
- `21:03:22` root: {"statusCode": 200, "body": "{\"n_compound\": 133, \"n_3_plus\": 41, \"n_alerts\": 26, \"duration_s\": 1.69}"}

# ops 5127 -- every symbol from the warehouse: daily bars since inception, aliases, native page

**Status:** failure  
**Duration:** 283.4s  
**Finished:** 2026-09-02T14:59:02+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5127_every_symbol_from_s3.py", line 117, in main
    s3.delete_object(Bucket=B, Key=o["Key"])
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.AccessDenied: An error occurred (AccessDenied) when calling the DeleteObject operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: s3:DeleteObject on resource: "arn:aws:s3:::justhodl-dashboard-live/data/warm/tv-bars/universe/AMEX__SPY.json.gz" with an explicit deny in a resource-based policy

```

## Data

| step | symdir_version |
|---|---|
| S1 | 1.2.0 |

## Log
## S1 deploy tv-bars v1.1 + symdir v1.2.0

- `14:54:18`   zip: 107422 bytes
## 1. Lambda

- `14:54:19`   Lambda exists — updating
- `14:54:22` ✅   ✓ updated justhodl-tv-bars
- `14:54:25`   zip: 132808 bytes
## 1. Lambda

- `14:54:25`   Lambda exists — updating
- `14:54:28` ✅   ✓ updated justhodl-symdir
- `14:58:54` ✅   rebuilt docs=1,374,434 elapsed=242.1s

# ops 5580 -- CDN: invalidate the harvest + chart assets on the justhodl.ai distribution; confirm the origin object

**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-09-15T04:29:57+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5580_fmp_harvest_cdn.py", line 28, in main
    dists = cf.list_distributions().get("DistributionList", {}).get("Items", []) or []
            ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.AccessDenied: An error occurred (AccessDenied) when calling the ListDistributions operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: cloudfront:ListDistributions because no identity-based policy allows the cloudfront:ListDistributions action

```

## Log
- `04:29:56` ✅ origin s3://justhodl-dashboard-live/data/fmp-ratios.json: 181618 bytes, content-type=application/json, last-modified=2026-09-15 04:25:42+00:00


**Status:** failure  
**Duration:** 0.3s  
**Finished:** 2026-09-19T19:44:07+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5869_etf_original_research_acceptance.py", line 33, in main
    receipt=read('data/ops/releases/'+fn+'.json')
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5869_etf_original_research_acceptance.py", line 24, in read
    def read(key):return json.loads(raw(key))
                                    ^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-etf-true-flows/source/etf_store.py", line 27, in read
    raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.

```

## Log


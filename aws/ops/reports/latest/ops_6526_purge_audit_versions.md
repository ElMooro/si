# ops 6526 - delete the loop's dead audit-log versions

**Status:** failure  
**Duration:** 51.4s  
**Finished:** 2026-10-16T23:08:20+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_6526_purge_audit_versions.py", line 132, in main
    results = list(listers.map(lambda ks: purge_key(ks[0], ks[1], deleter), keys))
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/concurrent/futures/_base.py", line 619, in result_iterator
    yield _result_or_cancel(fs.pop())
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/concurrent/futures/_base.py", line 317, in _result_or_cancel
    return fut.result(timeout)
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/concurrent/futures/_base.py", line 456, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/concurrent/futures/thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_6526_purge_audit_versions.py", line 132, in <lambda>
    results = list(listers.map(lambda ks: purge_key(ks[0], ks[1], deleter), keys))
                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_6526_purge_audit_versions.py", line 71, in purge_key
    page = s3.list_object_versions(**kw)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (SlowDown) when calling the ListObjectVersions operation: Please reduce your request rate.

```

## Log
- `23:07:29` BucketSizeBytes(Standard) TB by day: [('10-11', 0.92), ('10-12', 24.65), ('10-13', 61.15), ('10-14', 105.87), ('10-15', 145.39)]
- `23:07:29` day-logs in scope: [('2026-10-10.jsonl', 0.0), ('2026-10-11.jsonl', 0.0), ('2026-10-12.jsonl', 22.0), ('2026-10-13.jsonl', 33.2), ('2026-10-14.jsonl', 30.7), ('2026-10-15.jsonl', 30.8), ('2026-10-16.jsonl', 26.0)]

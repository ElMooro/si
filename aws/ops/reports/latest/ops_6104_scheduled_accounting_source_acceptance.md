
**Status:** failure  
**Duration:** 845.4s  
**Finished:** 2026-09-25T16:47:19+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6104_scheduled_accounting_source_acceptance.py", line 56, in main
    for _ in pool.map(verify_public,sorted(public)):pass
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 619, in result_iterator
    yield _result_or_cancel(fs.pop())
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 317, in _result_or_cancel
    return fut.result(timeout)
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6104_scheduled_accounting_source_acceptance.py", line 54, in verify_public
    assert store.bounded(response)==store.bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Log


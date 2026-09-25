
**Status:** failure  
**Duration:** 8.2s  
**Finished:** 2026-09-25T01:43:43+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6036_offexchange_whole_source_preflight.py", line 136, in main
    captures[jobs[job]]=job.result();status(s3,{'request_id':REQUEST,'status':'capturing','parents':parents,'captures':captures})
                        ^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/concurrent/futures/thread.py", line 59, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6036_offexchange_whole_source_preflight.py", line 100, in fetch
    response=opener.open(req,timeout=max(1,min(40,deadline-time.monotonic())));code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in HEADERS};raw=bounded(response,SOURCE_MAX)
                                                                                                                                                                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6036_offexchange_whole_source_preflight.py", line 33, in bounded
    if not 0<len(raw)<=limit:raise ValueError('Whole source byte bound')
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Whole source byte bound

```

## Log


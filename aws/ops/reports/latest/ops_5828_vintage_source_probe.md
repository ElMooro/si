
**Status:** failure  
**Duration:** 1.3s  
**Finished:** 2026-09-19T04:05:30+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5828_vintage_source_probe.py", line 32, in main
    with urllib.request.urlopen(req,timeout=30) as response:raw=response.read(16*1024*1024+1)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 215, in urlopen
    return opener.open(url, data, timeout)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 521, in open
    response = meth(req, response)
               ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 630, in http_response
    response = self.parent.error(
               ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 559, in error
    return self._call_chain(*args)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 492, in _call_chain
    result = func(*args)
             ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 639, in http_error_default
    raise HTTPError(req.full_url, code, msg, hdrs, fp)
urllib.error.HTTPError: HTTP Error 400: Bad Request

```

## Log


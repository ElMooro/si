# ops 3158 — TV pipeline v3 (notes + WATCHLISTS)

**Status:** failure  
**Duration:** 7.0s  
**Finished:** 2026-07-12T18:39:15+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3158_tv_pipeline_v3.py", line 99, in <module>
    with urllib.request.urlopen(req, timeout=30) as r:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 215, in urlopen
    return opener.open(url, data, timeout)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 521, in open
    response = meth(req, response)
               ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 630, in http_response
    response = self.parent.error(
               ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 559, in error
    return self._call_chain(*args)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 492, in _call_chain
    result = func(*args)
             ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 639, in http_error_default
    raise HTTPError(req.full_url, code, msg, hdrs, fp)
urllib.error.HTTPError: HTTP Error 502: Bad Gateway

```

## Data

| function_url |
|---|
| https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/ |

## Log
## 1. Deploy ingest lambda

- `18:39:09`   zip: 55246 bytes
## 1. Lambda

- `18:39:09`   Lambda exists — updating
- `18:39:14` ✅   ✓ updated justhodl-tv-notes-ingest
## 2. E2E: notes + watchlists through the real pipe


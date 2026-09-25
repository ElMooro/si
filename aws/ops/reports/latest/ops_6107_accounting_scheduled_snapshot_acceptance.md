
**Status:** failure  
**Duration:** 232.5s  
**Finished:** 2026-09-25T17:10:35+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6107_accounting_scheduled_snapshot_acceptance.py", line 85, in main
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+producer.CURRENT,headers={'Cache-Control':'no-cache'}),timeout=30) as response:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
urllib.error.HTTPError: HTTP Error 403: Forbidden

```

## Data

| diagnostic | replay | source_replay_seconds | whole_ready_document | whole_source_journal |
|---|---|---|---|---|
| {'key': 'audit-private/20260909-originals/financial-statement-research/87fa5b5ab4620101a936c1bc26b3b925c2b31b978c357cb8dd0f0bcdaa34ddb6.bin', 'sha256': '87fa5b5ab4620101a936c1bc26b3b925c2b31b978c357cb8dd0f0bcdaa34ddb6', 'bytes': 347052} | {'manifest_key': 'data/statement-research/runs/d45b617f4df8e2d2eab788dd97f89bf29cac7838684827efce521fdf1e97376a.json', 'output_sha256': '37612b97e1abaebe97d9fbef9466c569ef67267bab9e4cce9e345fbd44f458e9'} |  | {'bytes': 1501, 'key': 'audit-private/20260909-originals/financial-statement-research/6da08b694316d13eea20427c3acc721f967189962669c65fab9c8d6b9e4a2793.bin', 'sha256': '6da08b694316d13eea20427c3acc721f967189962669c65fab9c8d6b9e4a2793'} | {'bytes': 983814, 'key': 'audit-private/20260909-originals/financial-statement-research/385206a36b9f3247c7df62f26a8df8b73ef4dc1a5601ba6cd24ef1ad01aa0c42.bin', 'sha256': '385206a36b9f3247c7df62f26a8df8b73ef4dc1a5601ba6cd24ef1ad01aa0c42'} |
|  |  | 148.951 |  |  |

## Log


# ops 6929 - year over year on every chart of the held outright desk, real browser on the live site (read-only), 2026-11-23T16:30+00:00

**Status:** failure  
**Duration:** 293.7s  
**Finished:** 2026-11-23T16:35:51+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/http/client.py", line 627, in _read_chunked
    value.append(self._safe_read(chunk_left))
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/http/client.py", line 671, in _safe_read
    raise IncompleteRead(data, amt - len(data))
http.client.IncompleteRead: IncompleteRead(65920 bytes read, 61965 more expected)

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6929_held_outright_yoy_live_proof.py", line 217, in main
    js = fetch("%s/assets/jh-workspaces.js?nocache=%d" % (BASE, stamp + attempt))
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6929_held_outright_yoy_live_proof.py", line 74, in fetch
    return resp.read().decode("utf-8", "replace")
           ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/http/client.py", line 488, in read
    return self._read_chunked(amt)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/http/client.py", line 633, in _read_chunked
    raise IncompleteRead(b''.join(value)) from exc
http.client.IncompleteRead: IncompleteRead(0 bytes read)

```

## Log
## 0. The live site serves the new build


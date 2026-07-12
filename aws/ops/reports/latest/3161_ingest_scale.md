# ops 3161 — ingest at harvest scale

**Status:** failure  
**Duration:** 74.1s  
**Finished:** 2026-07-12T20:06:42+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3161_ingest_scale.py", line 139, in <module>
    post(furl, {"token": token, "delete_ids": ids}, timeout=60)
  File "/home/runner/work/si/si/aws/ops/pending/ops_3161_ingest_scale.py", line 57, in post
    with urllib.request.urlopen(req, timeout=timeout) as r:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 215, in urlopen
    return opener.open(url, data, timeout)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 515, in open
    response = self._open(req, data)
               ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 532, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 492, in _call_chain
    result = func(*args)
             ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 1392, in https_open
    return self.do_open(http.client.HTTPSConnection, req,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/urllib/request.py", line 1348, in do_open
    r = h.getresponse()
        ^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/http/client.py", line 1450, in getresponse
    response.begin()
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/http/client.py", line 336, in begin
    version, status, reason = self._read_status()
                              ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/http/client.py", line 297, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/socket.py", line 720, in readinto
    return self._sock.recv_into(b)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/ssl.py", line 1251, in recv_into
    return self.read(nbytes, buffer)
           ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/ssl.py", line 1103, in read
    return self._sslobj.read(len, buffer)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TimeoutError: The read operation timed out

```

## Data

| brain_error | brain_failed | brain_upserted | burst_secs | burst_status | memory | mirror_added | projected_1983_notes_secs | timeout | wl_only_status | wl_saved | wl_secs |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 1024 |  |  | 300 |  |  |  |
|  |  |  |  |  |  |  |  |  | 200 | 2 | 1.4 |
| None | 0 | 300 | 4.3 | 200 |  | 300 |  |  |  |  |  |
|  |  |  |  |  |  |  | 28.4 |  |  |  |  |

## Log
## 1. Deploy hardened ingest (1024MB / 300s / parallel)

- `20:05:29`   zip: 56091 bytes
## 1. Lambda

- `20:05:29`   Lambda exists — updating
- `20:05:34` ✅   ✓ updated justhodl-tv-notes-ingest
## 2. Watchlists-first request (the new order)

- `20:05:37` ✅ watchlists land independently of notes (the fix that saves them when a note chunk dies)
## 3. 300-note burst — the scale that broke it

- `20:05:41` ✅ 300 notes accepted in 4.3s (brain 300, mirror 300)
## 4. Cleanup e2e artifacts


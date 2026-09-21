
**Status:** failure  
**Duration:** 199.3s  
**Finished:** 2026-09-21T14:58:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 1344, in do_open
    h.request(req.get_method(), req.selector, req.data, headers,
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1386, in request
    self._send_request(method, url, body, headers, encode_chunked)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1432, in _send_request
    self.endheaders(body, encode_chunked=encode_chunked)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1381, in endheaders
    self._send_output(message_body, encode_chunked=encode_chunked)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1141, in _send_output
    self.send(msg)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1085, in send
    self.connect()
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1527, in connect
    self.sock = self._context.wrap_socket(self.sock,
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/ssl.py", line 455, in wrap_socket
    return self.sslsocket_class._create(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/ssl.py", line 1041, in _create
    self.do_handshake()
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/ssl.py", line 1319, in do_handshake
    self._sslobj.do_handshake()
ConnectionResetError: [Errno 104] Connection reset by peer

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5996_option_population_qualification.py", line 132, in main
    for _ in pool.map(check,sorted(protected)):pass
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
  File "/home/runner/work/si/si/aws/ops/staged/ops_5996_option_population_qualification.py", line 130, in check
    def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_5975_etf_constituent_source_preflight.py", line 47, in denied
    with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 215, in urlopen
    return opener.open(url, data, timeout)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 515, in open
    response = self._open(req, data)
               ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 532, in _open
    result = self._call_chain(self.handle_open, protocol, protocol +
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 492, in _call_chain
    result = func(*args)
             ^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 1392, in https_open
    return self.do_open(http.client.HTTPSConnection, req,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/urllib/request.py", line 1347, in do_open
    raise URLError(err)
urllib.error.URLError: <urlopen error [Errno 104] Connection reset by peer>

```

## Log


## Deploy carry-surface v1.3.1

**Status:** failure  
**Duration:** 319.5s  
**Finished:** 2026-07-15T16:19:41+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 534, in _make_request
    response = conn.getresponse()
               ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connection.py", line 571, in getresponse
    httplib_response = super().getresponse()
                       ^^^^^^^^^^^^^^^^^^^^^
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

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/httpsession.py", line 477, in send
    urllib_response = conn.urlopen(
                      ^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 842, in urlopen
    retries = retries.increment(
              ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/util/retry.py", line 473, in increment
    raise reraise(type(error), error, _stacktrace)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/util/util.py", line 39, in reraise
    raise value
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 788, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 536, in _make_request
    self._raise_timeout(err=e, url=url, timeout_value=read_timeout)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 367, in _raise_timeout
    raise ReadTimeoutError(
urllib3.exceptions.ReadTimeoutError: AWSHTTPSConnectionPool(host='lambda.us-east-1.amazonaws.com', port=443): Read timed out. (read timeout=60)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/3345b_unwind_regime_join_rerun.py", line 30, in <module>
    deploy_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 277, in deploy_lambda
    return smoke_test(report=report, function_name=function_name)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 214, in smoke_test
    resp = _lam.invoke(
           ^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1076, in _make_api_call
    http, parsed_response = self._make_request(
                            ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1100, in _make_request
    return self._endpoint.make_request(operation_model, request_dict)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 119, in make_request
    return self._send_request(request_dict, operation_model)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 200, in _send_request
    while self._needs_retry(
          ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 360, in _needs_retry
    responses = self._event_emitter.emit(
                ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/hooks.py", line 412, in emit
    return self._emitter.emit(aliased_event_name, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/hooks.py", line 256, in emit
    return self._emit(event_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/hooks.py", line 239, in _emit
    response = handler(**kwargs)
               ^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 207, in __call__
    if self._checker(**checker_kwargs):
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 284, in __call__
    should_retry = self._should_retry(
                   ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 320, in _should_retry
    return self._checker(attempt_number, response, caught_exception)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 363, in __call__
    checker_response = checker(
                       ^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 247, in __call__
    return self._check_caught_exception(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 416, in _check_caught_exception
    raise caught_exception
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 279, in _do_get_response
    http_response = self._send(request)
                    ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 383, in _send
    return self.http_session.send(request)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/httpsession.py", line 514, in send
    raise ReadTimeoutError(endpoint_url=request.url, error=e)
botocore.exceptions.ReadTimeoutError: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-carry-surface/invocations"

```

## Log
- `16:14:22`   zip: 90171 bytes
## 1. Lambda

- `16:14:22`   Lambda exists — updating
- `16:14:27` ✅   ✓ updated justhodl-carry-surface
## 2. EB rule + permissions

- `16:14:28`   rule already correct: carry-surface-4h (rate(4 hours))
- `16:14:28` ✅   ✓ target → justhodl-carry-surface
- `16:14:28` ✅   ✓ added invoke permission
## 3. Smoke test

- `16:14:28`   invoking justhodl-carry-surface…

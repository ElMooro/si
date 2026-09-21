# ops 5871 -- Gear B: the real refusal, then a launch on the new supply

**Status:** failure  
**Duration:** 312.3s  
**Finished:** 2026-09-21T18:03:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 540, in _make_request
    response = conn.getresponse()
               ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connection.py", line 638, in getresponse
    httplib_response = super().getresponse()
                       ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 1478, in getresponse
    response.begin()
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 343, in begin
    version, status, reason = self._read_status()
                              ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 304, in _read_status
    line = str(self.fp.readline(_MAXLINE + 1), "iso-8859-1")
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/socket.py", line 720, in readinto
    return self._sock.recv_into(b)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/ssl.py", line 1251, in recv_into
    return self.read(nbytes, buffer)
           ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/ssl.py", line 1103, in read
    return self._sslobj.read(len, buffer)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TimeoutError: The read operation timed out

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/httpsession.py", line 509, in send
    urllib_response = conn.urlopen(
                      ^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 847, in urlopen
    retries = retries.increment(
              ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/util/retry.py", line 485, in increment
    raise reraise(type(error), error, _stacktrace)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/util/util.py", line 39, in reraise
    raise value
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 793, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 542, in _make_request
    self._raise_timeout(err=e, url=url, timeout_value=read_timeout)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 373, in _raise_timeout
    raise ReadTimeoutError(
urllib3.exceptions.ReadTimeoutError: AWSHTTPSConnectionPool(host='lambda.us-east-1.amazonaws.com', port=443): Read timed out. (read timeout=60)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5871_gearb_unblock.py", line 58, in main
    resp = lam.invoke(FunctionName="justhodl-ai", InvocationType="RequestResponse", Payload=json.dumps({"mode": "gearb", "launch": True, "body": {"launch": True}}).encode())
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1076, in _make_api_call
    http, parsed_response = self._make_request(
                            ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1100, in _make_request
    return self._endpoint.make_request(operation_model, request_dict)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 119, in make_request
    return self._send_request(request_dict, operation_model)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 202, in _send_request
    while self._needs_retry(
          ^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 362, in _needs_retry
    responses = self._event_emitter.emit(
                ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/hooks.py", line 412, in emit
    return self._emitter.emit(aliased_event_name, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/hooks.py", line 256, in emit
    return self._emit(event_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/hooks.py", line 239, in _emit
    response = handler(**kwargs)
               ^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 207, in __call__
    if self._checker(**checker_kwargs):
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 284, in __call__
    should_retry = self._should_retry(
                   ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 320, in _should_retry
    return self._checker(attempt_number, response, caught_exception)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 363, in __call__
    checker_response = checker(
                       ^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 247, in __call__
    return self._check_caught_exception(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/retryhandler.py", line 416, in _check_caught_exception
    raise caught_exception
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 281, in _do_get_response
    http_response = self._send(request)
                    ^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/endpoint.py", line 385, in _send
    return self.http_session.send(request)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/httpsession.py", line 547, in send
    raise ReadTimeoutError(endpoint_url=request.url, error=e)
botocore.exceptions.ReadTimeoutError: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-ai/invocations"

```

## Data

| approved_at | daily_budget | enabled | launch | max_jobs_per_day | records | season_cap | season_committed_usd | season_jobs | today_jobs |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-13T19:39:40.169947Z | 20.0 | True | True | 4 | 19 | 600.0 | 86.36 | 19 | 0 |

## Log
## 1. Control and spend

## 2. Supersede the stale gen-21 manifest

- `17:57:50` ✅ gen-21 marked superseded (zero-cost record; nothing on SageMaker)
## 2b. launch flag

- `17:57:50` launch already true
## 3. One real tick

- `17:57:51` schedule lookup: An error occurred (ResourceNotFoundException) when calling the GetSchedule operation: Schedule justhodl-ai-gearb does no

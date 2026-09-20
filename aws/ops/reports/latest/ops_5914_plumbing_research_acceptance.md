
**Status:** failure  
**Duration:** 285.6s  
**Finished:** 2026-09-20T11:51:28+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 793, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
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
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 312, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
http.client.RemoteDisconnected: Remote end closed connection without response

During handling of the above exception, another exception occurred:

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
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/util/util.py", line 38, in reraise
    raise value.with_traceback(tb)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/urllib3/connectionpool.py", line 793, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
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
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/http/client.py", line 312, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
urllib3.exceptions.ProtocolError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5914_plumbing_research_acceptance.py", line 108, in main
    acquisition,source_result=controlled(lam,'justhodl-daily-report-v3',{'action':'research_measurements'},r)
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5914_plumbing_research_acceptance.py", line 62, in controlled
    response,rejections=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(payload).encode()),wait_seconds=90)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/acceptance_invoke.py", line 9, in invoke_when_available
    try:return client.invoke(**kwargs), rejected
               ^^^^^^^^^^^^^^^^^^^^^^^
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
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/httpsession.py", line 549, in send
    raise ConnectionClosedError(
botocore.exceptions.ConnectionClosedError: Connection was closed before we received a valid response from endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-daily-report-v3/invocations".

```

## Data

| acceptance_consumer_invocations | exact_commit | linux_fixture_replay | notifications_sent | packaged_files_checked | pages_commit | paid_ai_calls | portable_reference_digest | portfolio_writes | private_account_reads | runtimes | schedule | verified_function | whole_preceding_product |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | exact_match |  |  |  |  | aa275f9916ddb722527e5b0290c483fab7659a2ee108a8b100e09442f6775b36 |  |  |  |  |  |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 8 |  |  |  |  |  |  |  | justhodl-ai-brief-router |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 7 |  |  |  |  |  |  |  | justhodl-bond-warroom |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 2 |  |  |  |  |  |  |  | justhodl-calibration-snapshotter |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 5 |  |  |  |  |  |  |  | justhodl-calibrator |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 12 |  |  |  |  |  |  |  | justhodl-crisis-plumbing |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 22 |  |  |  |  |  |  |  | justhodl-daily-report-v3 |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 13 |  |  |  |  |  |  |  | justhodl-master-ranker |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 16 |  |  |  |  |  |  |  | justhodl-morning-intelligence |  |
|  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  | 5 |  |  |  |  |  |  |  | justhodl-signal-logger |  |
| 0 |  |  | 0 |  |  | 0 |  | 0 | 0 | {'justhodl-ai-brief-router': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': '6s4KUJQyQbuvRDG873afEKTCx5gdNj0NCjh17OPBLXI=', 'packaged_files_checked': 8, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-bond-warroom': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'ihCEBE/HALHa7KtOMj30WRfTR7Cau4A+Vm+IjXZNumA=', 'packaged_files_checked': 7, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-calibration-snapshotter': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'eg8unyALYyReE+vpCfbDAl/ftvgZr3qn/t7Jddat51Y=', 'packaged_files_checked': 2, 'all_packaged_sources_match': True, 'alias': {'name': 'live', 'version': '3', 'weighted_secondary_versions': 0}}, 'justhodl-calibrator': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'eyO/r5tRIxNU1yjL3Mn/515PpBye/ngoCpZ154jpXgI=', 'packaged_files_checked': 5, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-crisis-plumbing': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'FQesppZXt3/EQzNfPQNhtS51K0A09PaBiU4x0URAvCU=', 'packaged_files_checked': 12, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-daily-report-v3': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'iVmsdSVY/PlLkZ5nRBsg9M0nsLcx+fvOTACAnSd5kAk=', 'packaged_files_checked': 22, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-master-ranker': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': '7NjyW8YhoOjB1wVjBdxfh5gaD0to5boLQnOl4WYMuww=', 'packaged_files_checked': 13, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-morning-intelligence': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'K3Ock9Wr/MdElks4zBXo+jW92NT9VAMj87Gg5L7AlSg=', 'packaged_files_checked': 16, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-signal-logger': {'commit': '3a35ad51c049cbf57b5e43fa732a8c308e8aad5c', 'code_sha256': 'tezvw+Hw91omZkpu5UB6pg1reBlnKJS+mgQOMSL3WiY=', 'packaged_files_checked': 5, 'all_packaged_sources_match': True, 'alias': None}} |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | {'sha256': 'c9ba6e12508eae1431e9fde1c031491374b72026ab960f428832801aef6b72a0', 'bytes': 168362, 'anonymous_denied': True, 'legacy_source_sha256': '1c579c47862866421f7e809d1afce60a325ede88be941fd1ab56a44ae07b3a83'} |
|  |  |  |  |  | 3a35ad51c049cbf57b5e43fa732a8c308e8aad5c |  |  |  |  |  | {'enabled': True, 'expression': 'cron(54 14 * * ? *)', 'bound_targets': 1, 'natural_native_publication_observed': False} |  |  |

## Log


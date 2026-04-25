# Broken Lambdas — Investigation & Recommendations

**Generated:** 2026-04-25T01:49:47.994194+00:00
**Scope:** 7 Lambdas with high error rates surfaced by health monitor 2026-04-25

---

## Summary table

| Lambda | EB rules | Source in repo | Recommendation |
|---|---|---|---|
| `news-sentiment-agent` | news-sentiment-update | yes | _(see below)_ |
| `global-liquidity-agent-v2` | DailyLiquidityReportRule, khalid-daily-report | yes | _(see below)_ |
| `fmp-stock-picks-agent` | fmp-movers-hourly, fmp-stock-picks-daily | yes | _(see below)_ |
| `daily-liquidity-report` | daily-liquidity-7am | yes | _(see below)_ |
| `ecb-data-daily-updater` | ecb-daily-update-rule | yes | _(see below)_ |
| `treasury-auto-updater` | treasury-monday-update, treasury-thursday-update | yes | _(see below)_ |
| `justhodl-data-collector` | justhodl-hourly-collection | yes | _(see below)_ |

## Per-Lambda findings

### `news-sentiment-agent`

- **Runtime:** `python3.9`
- **Memory:** 512MB
- **Timeout:** 30s
- **Last modified:** 2025-09-21T17:02:16.000+0000
- **EB rules:**
  - `news-sentiment-update` — `rate(30 minutes)` (ENABLED)
- **Source:** [`aws/lambdas/news-sentiment-agent/source/lambda_function.py`](aws/lambdas/news-sentiment-agent/source/lambda_function.py) (7 LOC)

**Error signature:**
```
END RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25
REPORT RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25	Duration: 95.78 ms	Billed Duration: 96 ms	Memory Size: 512 MB	Max Memory Used: 39 MB	Status: error	Error Type: Runtime.ImportModuleError
INIT_REPORT Init Duration: 83.58 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
START RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25 Version: $LATEST
[WARNING]	2026-04-24T17:39:52.929Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
[ERROR] Runtime.ImportModuleError: Unable to import module 'lambda_news_agent': No module named 'lambda_news_agent'
Traceback (most recent call last):
```

### `global-liquidity-agent-v2`

- **Runtime:** `python3.11`
- **Memory:** 1024MB
- **Timeout:** 300s
- **Last modified:** 2025-11-01T22:48:28.000+0000
- **EB rules:**
  - `DailyLiquidityReportRule` — `cron(0 12 * * ? *)` (ENABLED)
  - `khalid-daily-report` — `cron(0 13 * * ? *)` (ENABLED)
- **Source:** [`aws/lambdas/global-liquidity-agent-v2/source/global_liquidity_fixed.py`](aws/lambdas/global-liquidity-agent-v2/source/global_liquidity_fixed.py) (56 LOC)

**Error signature:**
```
END RequestId: 25a27ba9-e9af-489b-af00-005778ebb745
REPORT RequestId: 25a27ba9-e9af-489b-af00-005778ebb745	Duration: 97.30 ms	Billed Duration: 98 ms	Memory Size: 1024 MB	Max Memory Used: 43 MB	Status: error	Error Type: Runtime.ImportModuleError
XRAY TraceId: 1-69ebaae8-4dbb86ee66fd32e40fdbfba6	SegmentId: 2d34804ed85ebd6f	Sampled: true
INIT_REPORT Init Duration: 87.28 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
START RequestId: 25a27ba9-e9af-489b-af00-005778ebb745 Version: $LATEST
[WARNING]	2026-04-24T17:39:52.887Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
[ERROR] Runtime.ImportModuleError: Unable to import module 'khalid_no_email': No module named 'khalid_no_email'
Traceback (most recent call last):
```

### `fmp-stock-picks-agent`

- **Runtime:** `python3.12`
- **Memory:** 512MB
- **Timeout:** 900s
- **Last modified:** 2026-03-02T09:52:28.000+0000
- **EB rules:**
  - `fmp-movers-hourly` — `cron(0 14,16,18,20 ? * MON-FRI *)` (ENABLED)
  - `fmp-stock-picks-daily` — `cron(0 12 ? * MON-FRI *)` (ENABLED)
- **Source:** [`aws/lambdas/fmp-stock-picks-agent/source/lambda_function.py`](aws/lambdas/fmp-stock-picks-agent/source/lambda_function.py) (441 LOC)

**Error signature:**
```
REPORT RequestId: 408f8189-de88-4de4-a194-0e9275088051	Duration: 41672.16 ms	Billed Duration: 41673 ms	Memory Size: 512 MB	Max Memory Used: 112 MB
[ERROR] ClientError: An error occurred (AccessDenied) when calling the SendEmail operation: User `arn:aws:sts::857687956942:assumed-role/economyapi-lambda-role/fmp-stock-picks-agent' is not authorized
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 413, in lambda_handler
    ses.send_email(
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 602, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/var/lang/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
  File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 1078, in _make_api_call
    raise error_class(parsed_response, operation_name)
START RequestId: 408f8189-de88-4de4-a194-0e9275088051 Version: $LATEST
END RequestId: 408f8189-de88-4de4-a194-0e9275088051
REPORT RequestId: 408f8189-de88-4de4-a194-0e9275088051	Duration: 96181.76 ms	Billed Duration: 96182 ms	Memory Size: 512 MB	Max Memory Used: 111 MB
[ERROR] ClientError: An error occurred (AccessDenied) when calling the SendEmail operation: User `arn:aws:sts::857687956942:assumed-role/economyapi-lambda-role/fmp-stock-picks-agent' is not authorized
```

### `daily-liquidity-report`

- **Runtime:** `python3.11`
- **Memory:** 2048MB
- **Timeout:** 300s
- **Last modified:** 2025-10-05T01:56:26.000+0000
- **EB rules:**
  - `daily-liquidity-7am` — `cron(45 12 * * ? *)` (ENABLED)
- **Source:** [`aws/lambdas/daily-liquidity-report/source/lambda_function.py`](aws/lambdas/daily-liquidity-report/source/lambda_function.py) (104 LOC)

**Error signature:**
```
REPORT RequestId: e757b0c5-eaa1-45c0-9e62-9ab617093f72	Duration: 1168.35 ms	Billed Duration: 1169 ms	Memory Size: 2048 MB	Max Memory Used: 94 MB
[ERROR] ClientError: An error occurred (AccessControlListNotSupported) when calling the PutObject operation: The bucket does not allow ACLs
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 14, in lambda_handler
    s3.put_object(
  File "/var/lang/lib/python3.11/site-packages/botocore/client.py", line 602, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/var/lang/lib/python3.11/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
  File "/var/lang/lib/python3.11/site-packages/botocore/client.py", line 1078, in _make_api_call
    raise error_class(parsed_response, operation_name)
START RequestId: e757b0c5-eaa1-45c0-9e62-9ab617093f72 Version: $LATEST
END RequestId: e757b0c5-eaa1-45c0-9e62-9ab617093f72
REPORT RequestId: e757b0c5-eaa1-45c0-9e62-9ab617093f72	Duration: 1313.87 ms	Billed Duration: 1314 ms	Memory Size: 2048 MB	Max Memory Used: 94 MB
[ERROR] ClientError: An error occurred (AccessControlListNotSupported) when calling the PutObject operation: The bucket does not allow ACLs
```

### `ecb-data-daily-updater`

- **Runtime:** `python3.9`
- **Memory:** 256MB
- **Timeout:** 60s
- **Last modified:** 2025-08-10T22:05:40.000+0000
- **EB rules:**
  - `ecb-daily-update-rule` — `cron(0 6 * * ? *)` (ENABLED)
- **Source:** [`aws/lambdas/ecb-data-daily-updater/source/lambda_function.py`](aws/lambdas/ecb-data-daily-updater/source/lambda_function.py) (57 LOC)

**Error signature:**
```
Loaded 160 indicators
[ERROR] AttributeError: 'str' object has no attribute 'get'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 19, in lambda_handler
    if 'CISS' in indicator.get('symbol', '') and 'SS_CI' in indicator.get('symbol', ''):
START RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38 Version: $LATEST
END RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38
REPORT RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38	Duration: 351.68 ms	Billed Duration: 352 ms	Memory Size: 256 MB	Max Memory Used: 97 MB
Loaded 160 indicators
```

### `treasury-auto-updater`

- **Runtime:** `python3.9`
- **Memory:** 128MB
- **Timeout:** 3s
- **Last modified:** 2025-08-13T21:04:42.000+0000
- **EB rules:**
  - `treasury-monday-update` — `cron(0 10 ? * MON *)` (ENABLED)
  - `treasury-thursday-update` — `cron(0 10 ? * THU *)` (ENABLED)
- **Source:** [`aws/lambdas/treasury-auto-updater/source/lambda_function.py`](aws/lambdas/treasury-auto-updater/source/lambda_function.py) (41 LOC)

**Error signature:**
```
END RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6
REPORT RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6	Duration: 97.32 ms	Billed Duration: 98 ms	Memory Size: 128 MB	Max Memory Used: 39 MB	Status: error	Error Type: Runtime.ImportModuleError
INIT_REPORT Init Duration: 85.23 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
START RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6 Version: $LATEST
[WARNING]	2026-04-23T10:03:02.072Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
[ERROR] Runtime.ImportModuleError: Unable to import module 'updater': No module named 'updater'
Traceback (most recent call last):
```

### `justhodl-data-collector`

- **Runtime:** `python3.9`
- **Memory:** 128MB
- **Timeout:** 15s
- **Last modified:** 2025-09-21T20:12:32.048+0000
- **EB rules:**
  - `justhodl-hourly-collection` — `rate(1 hour)` (ENABLED)
- **Source:** [`aws/lambdas/justhodl-data-collector/source/save_to_s3.py`](aws/lambdas/justhodl-data-collector/source/save_to_s3.py) (22 LOC)

**Error signature:**
```
REPORT RequestId: a56df5fa-020d-4bb8-adaa-6ca7b931b399	Duration: 414.46 ms	Billed Duration: 415 ms	Memory Size: 128 MB	Max Memory Used: 90 MB
[ERROR] HTTPError: HTTP Error 403: Forbidden
Traceback (most recent call last):
  File "/var/task/save_to_s3.py", line 11, in lambda_handler
    response = urllib.request.urlopen('https://api.justhodl.ai/')
  File "/var/lang/lib/python3.9/urllib/request.py", line 214, in urlopen
    return opener.open(url, data, timeout)
  File "/var/lang/lib/python3.9/urllib/request.py", line 523, in open
    response = meth(req, response)
  File "/var/lang/lib/python3.9/urllib/request.py", line 632, in http_response
    response = self.parent.error(
  File "/var/lang/lib/python3.9/urllib/request.py", line 561, in error
    return self._call_chain(*args)
  File "/var/lang/lib/python3.9/urllib/request.py", line 494, in _call_chain
    result = func(*args)
```

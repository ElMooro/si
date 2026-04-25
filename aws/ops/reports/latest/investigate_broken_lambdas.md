# Investigate 7 Lambdas with high error rates (read-only)

**Status:** success  
**Duration:** 5.5s  
**Finished:** 2026-04-25T01:49:48+00:00  

## Data

| lambdas_investigated | with_eb_rules | with_source_in_repo |
|---|---|---|
| 7 | 7 | 7 |

## Log
## --- news-sentiment-agent ---

- `01:49:42`   Config: runtime=python3.9 mem=512MB timeout=30s
- `01:49:42`   Last modified: 2025-09-21T17:02:16.000+0000
- `01:49:43`   EB rules: 1
- `01:49:43`     news-sentiment-update                    state=ENABLED    schedule=rate(30 minutes)
- `01:49:43`   Source: aws/lambdas/news-sentiment-agent/source/lambda_function.py (7 LOC)
- `01:49:43`   Found 26 log events from latest streams
- `01:49:43`   Error signature:
- `01:49:43`     END RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25
- `01:49:43`     REPORT RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25	Duration: 95.78 ms	Billed Duration: 96 ms	Memory Size: 512 MB	Max Memory Used: 39 MB	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:43`     INIT_REPORT Init Duration: 83.58 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:43`     START RequestId: 57325a9d-ff5f-42e4-83df-80f3dcaaef25 Version: $LATEST
- `01:49:43`     [WARNING]	2026-04-24T17:39:52.929Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
- `01:49:43`     [ERROR] Runtime.ImportModuleError: Unable to import module 'lambda_news_agent': No module named 'lambda_news_agent'
- `01:49:43`     Traceback (most recent call last):
## --- global-liquidity-agent-v2 ---

- `01:49:43`   Config: runtime=python3.11 mem=1024MB timeout=300s
- `01:49:43`   Last modified: 2025-11-01T22:48:28.000+0000
- `01:49:44`   EB rules: 2
- `01:49:44`     DailyLiquidityReportRule                 state=ENABLED    schedule=cron(0 12 * * ? *)
- `01:49:44`     khalid-daily-report                      state=ENABLED    schedule=cron(0 13 * * ? *)
- `01:49:44`   Source: aws/lambdas/global-liquidity-agent-v2/source/global_liquidity_fixed.py (56 LOC)
- `01:49:44`   Found 20 log events from latest streams
- `01:49:44`   Error signature:
- `01:49:44`     END RequestId: 25a27ba9-e9af-489b-af00-005778ebb745
- `01:49:44`     REPORT RequestId: 25a27ba9-e9af-489b-af00-005778ebb745	Duration: 97.30 ms	Billed Duration: 98 ms	Memory Size: 1024 MB	Max Memory Used: 43 MB	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:44`     XRAY TraceId: 1-69ebaae8-4dbb86ee66fd32e40fdbfba6	SegmentId: 2d34804ed85ebd6f	Sampled: true
- `01:49:44`     INIT_REPORT Init Duration: 87.28 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:44`     START RequestId: 25a27ba9-e9af-489b-af00-005778ebb745 Version: $LATEST
- `01:49:44`     [WARNING]	2026-04-24T17:39:52.887Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
- `01:49:44`     [ERROR] Runtime.ImportModuleError: Unable to import module 'khalid_no_email': No module named 'khalid_no_email'
- `01:49:44`     Traceback (most recent call last):
## --- fmp-stock-picks-agent ---

- `01:49:44`   Config: runtime=python3.12 mem=512MB timeout=900s
- `01:49:44`   Last modified: 2026-03-02T09:52:28.000+0000
- `01:49:44`   EB rules: 2
- `01:49:44`     fmp-movers-hourly                        state=ENABLED    schedule=cron(0 14,16,18,20 ? * MON-FRI *)
- `01:49:44`     fmp-stock-picks-daily                    state=ENABLED    schedule=cron(0 12 ? * MON-FRI *)
- `01:49:44`   Source: aws/lambdas/fmp-stock-picks-agent/source/lambda_function.py (441 LOC)
- `01:49:45`   Found 26 log events from latest streams
- `01:49:45`   Error signature:
- `01:49:45`     REPORT RequestId: 408f8189-de88-4de4-a194-0e9275088051	Duration: 41672.16 ms	Billed Duration: 41673 ms	Memory Size: 512 MB	Max Memory Used: 112 MB
- `01:49:45`     [ERROR] ClientError: An error occurred (AccessDenied) when calling the SendEmail operation: User `arn:aws:sts::857687956942:assumed-role/economyapi-lambda-role/fmp-stock-picks-agent' is not authorized
- `01:49:45`     Traceback (most recent call last):
- `01:49:45`       File "/var/task/lambda_function.py", line 413, in lambda_handler
- `01:49:45`         ses.send_email(
- `01:49:45`       File "/var/lang/lib/python3.12/site-packages/botocore/client.py", line 602, in _api_call
- `01:49:45`         return self._make_api_call(operation_name, kwargs)
- `01:49:45`       File "/var/lang/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
## --- daily-liquidity-report ---

- `01:49:45`   Config: runtime=python3.11 mem=2048MB timeout=300s
- `01:49:45`   Last modified: 2025-10-05T01:56:26.000+0000
- `01:49:45`   EB rules: 1
- `01:49:45`     daily-liquidity-7am                      state=ENABLED    schedule=cron(45 12 * * ? *)
- `01:49:45`   Source: aws/lambdas/daily-liquidity-report/source/lambda_function.py (104 LOC)
- `01:49:45`   Found 13 log events from latest streams
- `01:49:45`   Error signature:
- `01:49:45`     REPORT RequestId: e757b0c5-eaa1-45c0-9e62-9ab617093f72	Duration: 1168.35 ms	Billed Duration: 1169 ms	Memory Size: 2048 MB	Max Memory Used: 94 MB
- `01:49:45`     [ERROR] ClientError: An error occurred (AccessControlListNotSupported) when calling the PutObject operation: The bucket does not allow ACLs
- `01:49:45`     Traceback (most recent call last):
- `01:49:45`       File "/var/task/lambda_function.py", line 14, in lambda_handler
- `01:49:45`         s3.put_object(
- `01:49:45`       File "/var/lang/lib/python3.11/site-packages/botocore/client.py", line 602, in _api_call
- `01:49:45`         return self._make_api_call(operation_name, kwargs)
- `01:49:45`       File "/var/lang/lib/python3.11/site-packages/botocore/context.py", line 123, in wrapper
## --- ecb-data-daily-updater ---

- `01:49:45`   Config: runtime=python3.9 mem=256MB timeout=60s
- `01:49:45`   Last modified: 2025-08-10T22:05:40.000+0000
- `01:49:46`   EB rules: 1
- `01:49:46`     ecb-daily-update-rule                    state=ENABLED    schedule=cron(0 6 * * ? *)
- `01:49:46`   Source: aws/lambdas/ecb-data-daily-updater/source/lambda_function.py (57 LOC)
- `01:49:46`   Found 16 log events from latest streams
- `01:49:46`   Error signature:
- `01:49:46`     Loaded 160 indicators
- `01:49:46`     [ERROR] AttributeError: 'str' object has no attribute 'get'
- `01:49:46`     Traceback (most recent call last):
- `01:49:46`       File "/var/task/lambda_function.py", line 19, in lambda_handler
- `01:49:46`         if 'CISS' in indicator.get('symbol', '') and 'SS_CI' in indicator.get('symbol', ''):
- `01:49:46`     START RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38 Version: $LATEST
- `01:49:46`     END RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38
- `01:49:46`     REPORT RequestId: a0324843-7f21-4325-8e8c-fb10b7122b38	Duration: 351.68 ms	Billed Duration: 352 ms	Memory Size: 256 MB	Max Memory Used: 97 MB
## --- treasury-auto-updater ---

- `01:49:46`   Config: runtime=python3.9 mem=128MB timeout=3s
- `01:49:46`   Last modified: 2025-08-13T21:04:42.000+0000
- `01:49:46`   EB rules: 2
- `01:49:46`     treasury-monday-update                   state=ENABLED    schedule=cron(0 10 ? * MON *)
- `01:49:46`     treasury-thursday-update                 state=ENABLED    schedule=cron(0 10 ? * THU *)
- `01:49:46`   Source: aws/lambdas/treasury-auto-updater/source/lambda_function.py (41 LOC)
- `01:49:47`   Found 44 log events from latest streams
- `01:49:47`   Error signature:
- `01:49:47`     END RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6
- `01:49:47`     REPORT RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6	Duration: 97.32 ms	Billed Duration: 98 ms	Memory Size: 128 MB	Max Memory Used: 39 MB	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:47`     INIT_REPORT Init Duration: 85.23 ms	Phase: invoke	Status: error	Error Type: Runtime.ImportModuleError
- `01:49:47`     START RequestId: a0021e4d-360f-47bc-8efe-c95e40e16df6 Version: $LATEST
- `01:49:47`     [WARNING]	2026-04-23T10:03:02.072Z		LAMBDA_WARNING: Unhandled exception. The most likely cause is an issue in the function code. However, in rare cases, a Lambda runtime update can cause unexpected fu
- `01:49:47`     [ERROR] Runtime.ImportModuleError: Unable to import module 'updater': No module named 'updater'
- `01:49:47`     Traceback (most recent call last):
## --- justhodl-data-collector ---

- `01:49:47`   Config: runtime=python3.9 mem=128MB timeout=15s
- `01:49:47`   Last modified: 2025-09-21T20:12:32.048+0000
- `01:49:47`   EB rules: 1
- `01:49:47`     justhodl-hourly-collection               state=ENABLED    schedule=rate(1 hour)
- `01:49:47`   Source: aws/lambdas/justhodl-data-collector/source/save_to_s3.py (22 LOC)
- `01:49:47`   Found 14 log events from latest streams
- `01:49:47`   Error signature:
- `01:49:47`     REPORT RequestId: a56df5fa-020d-4bb8-adaa-6ca7b931b399	Duration: 414.46 ms	Billed Duration: 415 ms	Memory Size: 128 MB	Max Memory Used: 90 MB
- `01:49:47`     [ERROR] HTTPError: HTTP Error 403: Forbidden
- `01:49:47`     Traceback (most recent call last):
- `01:49:47`       File "/var/task/save_to_s3.py", line 11, in lambda_handler
- `01:49:47`         response = urllib.request.urlopen('https://api.justhodl.ai/')
- `01:49:47`       File "/var/lang/lib/python3.9/urllib/request.py", line 214, in urlopen
- `01:49:47`         return opener.open(url, data, timeout)
- `01:49:47`       File "/var/lang/lib/python3.9/urllib/request.py", line 523, in open
## Build per-Lambda recommendation doc

- `01:49:47` ✅   Wrote: aws/ops/audit/broken_lambdas_2026-04-25.md
- `01:49:48` ✅   S3 backup: _audit/broken_lambdas_2026-04-25.md
- `01:49:48` Done


**Status:** failure  
**Duration:** 65.6s  
**Finished:** 2026-09-28T11:22:21+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6286_morning_producer_replay.py", line 62, in main
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/op.FN/'tests/run_tests.py')],cwd=ROOT,check=True)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/subprocess.py", line 571, in run
    raise CalledProcessError(retcode, process.args,
subprocess.CalledProcessError: Command '['/opt/hostedtoolcache/Python/3.12.14/x64/bin/python3', '/home/runner/work/si/si/aws/lambdas/justhodl-activist-filings-scanner/tests/run_tests.py']' returned non-zero exit status 1.

```

## Data

| justhodl-momentum-breakout | justhodl-volatility-squeeze-hunter |
|---|---|
|  | {'function': 'justhodl-volatility-squeeze-hunter', 'key': 'data/volatility-squeeze.json', 'expected_commit': 'b3dd1c2b5f1ee461471d83a4fd733932c1bf694b', 'actual_runtime': {'code_sha256': '/Q/y5N7tCtlF6QHflPfyrFa7rXxoZvJGvbpA7UBcR/Q=', 'source_files_checked': 3, 'handler_bytes': 25967, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'b3dd1c2b5f1ee461471d83a4fd733932c1bf694b'}, 'schedules': [], 'function_name': 'justhodl-volatility-squeeze-hunter', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'fanout_route': {'manifest_key': 'config/fanout-manifest.json', 'sha256': 'decdf10e3114fef2919769327ad6c19a7041fb6f33f8b8b6a017a7ed03f086dd', 'bytes': 11573, 'etag': '"a8b0d37b4f8e60d26597b7c0fd0f08a5"', 'matching_ticks': ['daily-morn'], 'routes': [{'tick': 'daily-morn', 'kind': 'EventBridge rule', 'name': 'jhk-tick-daily-morn', 'state': 'ENABLED', 'expression': 'cron(0 11 * * ? *)', 'timezone': 'UTC'}], 'router_code_sha256': 'fIYL7j/Di8OpAhWWDKG3dnE9ay6kNjsWbe9tt72okZE=', 'router_sources_checked': 2}, 'producer_settings': {'request_limits': {'N_WORKERS': 12, 'MAX_TICKERS': 1500, 'TIMEOUT_BUDGET_S': 550}, 'public_head': 'data/volatility-squeeze.json', 'bucket': 'justhodl-dashboard-live'}, 'native_publication': {'status': 'published_price_originals_replayed', 'bytes': 3184786, 'sha256': 'da9a5634fad9b82e9fd664bc086fa8152157268ab979d0f0a3329fde49de10cb', 'generated_at': '2026-09-28T11:00:25.922316+00:00', 'version': '2.0.0', 'source_blobs': 74, 'request_occurrences': 1500, 'quality': {'acquisition_outcomes': {'not_attempted_runtime_rate_or_size_limit': 1424, 'received': 76}, 'independent_roots': 1, 'market_coverage_verified': False, 'measured_occurrences': 70, 'observation_outcomes': {'http_unavailable': 4, 'not_attempted_runtime_rate_or_size_limit': 1424, 'parsed_completed_observations': 72}, 'status': 'research_only'}, 'first_release_history_verified': False, 'independent_evidence': False, 'investment_authority': False}, 'current_archive_verified': True, 'consumer_qualification': False, 'investment_authority': False} |
| {'function': 'justhodl-momentum-breakout', 'key': 'data/momentum-breakout.json', 'expected_commit': 'b7db9ef147732700a4817e156eca2d2d53171896', 'actual_runtime': {'code_sha256': 'FENmD+l3t2o2K6RW3LPagMqcQupIATFnULvqX6GLPUI=', 'source_files_checked': 3, 'handler_bytes': 23004, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'b7db9ef147732700a4817e156eca2d2d53171896'}, 'schedules': [], 'function_name': 'justhodl-momentum-breakout', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'fanout_route': {'manifest_key': 'config/fanout-manifest.json', 'sha256': 'decdf10e3114fef2919769327ad6c19a7041fb6f33f8b8b6a017a7ed03f086dd', 'bytes': 11573, 'etag': '"a8b0d37b4f8e60d26597b7c0fd0f08a5"', 'matching_ticks': ['daily-morn'], 'routes': [{'tick': 'daily-morn', 'kind': 'EventBridge rule', 'name': 'jhk-tick-daily-morn', 'state': 'ENABLED', 'expression': 'cron(0 11 * * ? *)', 'timezone': 'UTC'}], 'router_code_sha256': 'fIYL7j/Di8OpAhWWDKG3dnE9ay6kNjsWbe9tt72okZE=', 'router_sources_checked': 2}, 'producer_settings': {'request_limits': {'N_WORKERS': 12, 'MAX_TICKERS': 600, 'TIMEOUT_BUDGET_S': 260, 'MIN_DOLLAR_VOL': '5000000'}, 'public_head': 'data/momentum-breakout.json', 'bucket': 'justhodl-dashboard-live'}, 'native_publication': {'status': 'published_momentum_originals_replayed', 'bytes': 4270783, 'sha256': 'b6f8059968ca64bc14e6b91800a333d08f75d86c8ac2f66ae9d03991e83ee057', 'generated_at': '2026-09-28T11:00:25.675909+00:00', 'version': '2.0.0', 'source_blobs': 95, 'request_occurrences': 600, 'quality': {'acquisition_outcomes': {'not_attempted_runtime_rate_or_size_limit': 504, 'received': 96}, 'independent_roots': 1, 'market_coverage_verified': False, 'measured_occurrences': 92, 'observation_outcomes': {'http_unavailable': 4, 'not_attempted_runtime_rate_or_size_limit': 504, 'parsed_completed_observations': 92}, 'status': 'research_only'}, 'first_release_history_verified': False, 'independent_evidence': False, 'investment_authority': False}, 'current_archive_verified': True, 'consumer_qualification': False, 'investment_authority': False} |  |

## Log


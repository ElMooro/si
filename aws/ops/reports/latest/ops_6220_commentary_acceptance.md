
**Status:** failure  
**Duration:** 6.6s  
**Finished:** 2026-09-28T14:07:02+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6220_commentary_acceptance.py", line 24, in main
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/subprocess.py", line 571, in run
    raise CalledProcessError(retcode, process.args,
subprocess.CalledProcessError: Command '['/opt/hostedtoolcache/Python/3.12.14/x64/bin/python3', '/home/runner/work/si/si/aws/lambdas/justhodl-page-ai-commentary/tests/run_tests.py']' returned non-zero exit status 1.

```

## Data

| actual_runtime_before_validation |
|---|
| {'code_sha256': 'Xlff+Uam4o5VSty3QVyEoZKm4txNJNBEjG1Nl75Zrxc=', 'source_files_checked': 15, 'handler_bytes': 20270, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'ec93564b571c4b13d79bd1e721dcdeab0061efd9'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-page-ai-commentary-daily', 'state': 'ENABLED', 'expression': 'cron(0 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-page-ai-commentary', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |

## Log


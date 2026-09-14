# ops 5545 (re-arm of 5544) -- first owned trace burst: prompts-only tasks, runner rights, live price, spot job on the digest-pinned image

**Status:** failure  
**Duration:** 127.3s  
**Finished:** 2026-09-14T00:55:19+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5545_first_trace_burst_rearm.py", line 147, in main
    sm.create_training_job(**kw, Tags=cg.tags("factory-trace-burst", 3) + [{"Key": "jh-factory", "Value": "burst-gen-0"}])
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the CreateTrainingJob operation: Unexpected failure code: InvalidImageDigest

```

## Data

| head |
|---|
| 0d9dfe89df |

## Log
- `00:53:46` ✅ tasks: 464 prompts (holdout excluded 164, scanned 464) families={"mbpp": 464} -> s3://justhodl-ai-857687956942/factory/bursts/tasks/20260914-005346/
- `00:53:47` ✅ training-job rights attached to group justhodl-runner-ecr for github-actions-justhodl; waiting 90 s
- `00:55:18` ✅ training price ml.g5.2xlarge = $1.5150/h (aws-price-list); burst cap $3.0300 at 7200s (spot bills less)

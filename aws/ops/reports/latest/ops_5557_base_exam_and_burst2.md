# ops 5557 -- base exam job (frozen HumanEval prompts, greedy) + burst 2 (MBPP+APPS, K=6) on the owned lane

**Status:** failure  
**Duration:** 216.8s  
**Finished:** 2026-09-14T21:37:51+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5557_base_exam_and_burst2.py", line 143, in main
    launch(spec2, name2, "s3://%s/%s" % (PRI, prefix2), "burst", {"task_cap": "1200"}, {"burst_index": 2, "tasks": len(tasks2), "samples_per_task": 6, "temperature": 0.8})
  File "/home/runner/work/si/si/aws/ops/pending/ops_5557_base_exam_and_burst2.py", line 106, in launch
    sm.create_training_job(**kw, Tags=cg.tags("factory-" + kind, 3) + [{"Key": "jh-factory", "Value": kind}])
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.ResourceLimitExceeded: An error occurred (ResourceLimitExceeded) when calling the CreateTrainingJob operation: The account-level service limit 'ml.g5.2xlarge for spot training job usage' is 1 Instances, with current utilization of 1 Instances and a request delta of 1 Instances. Please use AWS Service Quotas to request an increase for this quota. If AWS Service Quotas is not available, contact AWS support to request an increase for this quota.

```

## Data

| head |
|---|
| 242e8dd662 |

## Log
- `21:34:15` ✅ exam tasks: 164 frozen HumanEval prompts -> s3://justhodl-ai-857687956942/factory/exams/code/prompts-only/20260914-213415/ (prompts only)
- `21:34:15` ✅ training price ml.g5.2xlarge = $1.5150/h; per-job cap $3.0300
- `21:34:16` ✅ BASE EXAM launched: jh-exam-gen0-20260914-213415 -- 164 prompts, greedy, adapter=base; grade with factory-exam.yml (burst=jh-exam-gen0-20260914-213415, generation=gen-0)

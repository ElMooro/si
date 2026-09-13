# ops 5528 -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-09-13T21:08:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5528_own_training_lane.py", line 82, in main
    ecr.describe_repositories(repositoryNames=["justhodl/factory-train"])
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the DescribeRepositories operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ecr:DescribeRepositories on resource: arn:aws:ecr:us-east-1:857687956942:repository/justhodl/factory-train because no identity-based policy allows the ecr:DescribeRepositories action

```

## Data

| head |
|---|
| 0cfdd34a6c |

## Log
- `21:08:00` ✅ justhodl-ai receipt commit=0cfdd34 source_identical=True sha_match=True run=34782830752

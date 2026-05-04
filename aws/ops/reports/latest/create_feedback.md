# Create justhodl-feedback + table + URL

**Status:** failure  
**Duration:** 0.1s  
**Finished:** 2026-05-04T12:42:40+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_create_feedback.py", line 74, in main
    ensure_table(r)
  File "/home/runner/work/si/si/aws/ops/pending/_create_feedback.py", line 46, in ensure_table
    ddb.create_table(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the CreateTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:CreateTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-feedback because no identity-based policy allows the dynamodb:CreateTable action

```

## Log


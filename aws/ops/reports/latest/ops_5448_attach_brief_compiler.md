
**Status:** failure  
**Duration:** 85.4s  
**Finished:** 2026-09-12T14:50:49+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5448_attach_brief_compiler.py", line 161, in main
    iam.put_role_policy(RoleName=reconciler_role.rsplit("/", 1)[1], PolicyName="ReconcileSixBriefSchedules", PolicyDocument=json.dumps(scoped_policy))
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.LimitExceededException: An error occurred (LimitExceeded) when calling the PutRolePolicy operation: Maximum policy size of 10240 bytes exceeded for role lambda-execution-role

```

## Log


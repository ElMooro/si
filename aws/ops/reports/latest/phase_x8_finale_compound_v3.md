
# 1) Force-deploy compound v3 (9 feeds)

- `10:47:26`   FATAL: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-compound-aggregator
- `10:47:26`       Traceback (most recent call last):
- `10:47:26`         File "/home/runner/work/si/si/aws/ops/pending/_phase_x8_compound_v3_with_institutional.py", line 204, in <module>
- `10:47:26`           main()
- `10:47:26`         File "/home/runner/work/si/si/aws/ops/pending/_phase_x8_compound_v3_with_institutional.py", line 51, in main
- `10:47:26`           L.update_function_code(FunctionName="justhodl-compound-aggregator",
- `10:47:26`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
- `10:47:26`           return self._make_api_call(operation_name, kwargs)
- `10:47:26`                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- `10:47:26`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
- `10:47:26`           return func(*args, **kwargs)
- `10:47:26`                  ^^^^^^^^^^^^^^^^^^^^^
- `10:47:26`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
- `10:47:26`           raise error_class(parsed_response, operation_name)
- `10:47:26`       botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-compound-aggregator
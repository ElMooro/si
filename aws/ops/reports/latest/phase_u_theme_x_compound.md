
# 1) Force-deploy theme-rotation v2 (with curated holdings fallback)

- `08:12:52`     source: 28972 chars
- `08:12:52`       ✓ fetch_etf_holdings(ticker, fallback_top=fallback)
- `08:12:52`       ✓ curated_lookup
- `08:12:52`       ✓ Try newer stable endpoint first
- `08:12:53`   FATAL: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-theme-rotation-engine
- `08:12:53`       Traceback (most recent call last):
- `08:12:53`         File "/home/runner/work/si/si/aws/ops/pending/_phase_u_redeploy_theme_rot_and_cross_ref.py", line 170, in <module>
- `08:12:53`           main()
- `08:12:53`         File "/home/runner/work/si/si/aws/ops/pending/_phase_u_redeploy_theme_rot_and_cross_ref.py", line 51, in main
- `08:12:53`           L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
- `08:12:53`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
- `08:12:53`           return self._make_api_call(operation_name, kwargs)
- `08:12:53`                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- `08:12:53`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
- `08:12:53`           return func(*args, **kwargs)
- `08:12:53`                  ^^^^^^^^^^^^^^^^^^^^^
- `08:12:53`         File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
- `08:12:53`           raise error_class(parsed_response, operation_name)
- `08:12:53`       botocore.errorfactory.ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be performed at this time. An update is in progress for resource: arn:aws:lambda:us-east-1:857687956942:function:justhodl-theme-rotation-engine
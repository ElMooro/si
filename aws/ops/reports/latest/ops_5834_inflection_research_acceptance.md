
**Status:** failure  
**Duration:** 1.9s  
**Finished:** 2026-09-19T06:22:58+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5834_inflection_research_acceptance.py", line 66, in main
    invoke(names[0],{'action':'research_measurements','suppress_alerts':True})
  File "/home/runner/work/si/si/aws/ops/pending/ops_5834_inflection_research_acceptance.py", line 57, in invoke
    response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded(payload),
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.TooManyRequestsException: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 0): Rate Exceeded.

```

## Data

| active_aliases | runtimes |
|---|---|
| {} | {'justhodl-daily-report-v3': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'AUXjhc0e/r37s1eyjGynsldc2VT+UA/jCc42ppi08Ls='}, 'justhodl-liquidity-inflection': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'LO2Rvc9QOcYAwtcQF7uFj9gl9+Gy5U+gJ6XniEC4FZQ='}, 'justhodl-signal-board': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': '8Z9b0Gj0IohLYLseEdzDx7nF5e3iYlCQGbuPFCK/cCQ='}, 'justhodl-risk-regime': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'S4MP1P6oBOc/9Cs/rFtBUN9uViPwyvyFvIpARp1qxac='}, 'justhodl-master-ranker': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': '6XwKc+NZ5I8y0yijLDd256aBsu/46535fZGCNRkJxQo='}} |

## Log


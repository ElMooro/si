
**Status:** failure  
**Duration:** 182.1s  
**Finished:** 2026-09-19T06:32:06+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5835_inflection_research_acceptance.py", line 70, in main
    invoke(names[1],{'suppress_alerts':True})  # publish validated existing core while optional identities refresh
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_5835_inflection_research_acceptance.py", line 64, in invoke
    assert not response.get('FunctionError'),{'function':fn,'result':result}
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: {'function': 'justhodl-liquidity-inflection', 'result': {'errorType': 'Sandbox.Timedout', 'errorMessage': 'RequestId: 18e7d27a-e654-4b9b-8fa6-7971f30c7441 Error: Task timed out after 180.00 seconds'}}

```

## Data

| active_aliases | reserved_concurrency | runtimes |
|---|---|---|
| {} |  | {'justhodl-daily-report-v3': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'AUXjhc0e/r37s1eyjGynsldc2VT+UA/jCc42ppi08Ls='}, 'justhodl-liquidity-inflection': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'LO2Rvc9QOcYAwtcQF7uFj9gl9+Gy5U+gJ6XniEC4FZQ='}, 'justhodl-signal-board': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': '8Z9b0Gj0IohLYLseEdzDx7nF5e3iYlCQGbuPFCK/cCQ='}, 'justhodl-risk-regime': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': 'S4MP1P6oBOc/9Cs/rFtBUN9uViPwyvyFvIpARp1qxac='}, 'justhodl-master-ranker': {'commit': '1c7695b4d46441d6b60a770e93f044094d9e7ac4', 'code_sha256': '6XwKc+NZ5I8y0yijLDd256aBsu/46535fZGCNRkJxQo='}} |
|  | {'justhodl-daily-report-v3': 1, 'justhodl-liquidity-inflection': None} |  |

## Log



**Status:** failure  
**Duration:** 51.6s  
**Finished:** 2026-09-19T18:26:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5862_official_original_research_acceptance.py", line 51, in main
    assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' invocation failed'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: justhodl-official-pulse invocation failed

```

## Data

| function | invoke_status | runtimes | throttle_rejections |
|---|---|---|---|
|  |  | {'justhodl-official-pulse': {'commit': '6dce88007ae9b66d77a7e9d2ab2a3412ef42a2b9', 'code_sha256': 'Fo8PzEZFT03TjvTLkOs5hRnL63cK7wnCF2lznx9nrBI='}, 'justhodl-tic-flows': {'commit': '6dce88007ae9b66d77a7e9d2ab2a3412ef42a2b9', 'code_sha256': '3JoF+rnPOpogkIDkof94y/XFcZwESLFx5QVY+nuyNwg='}} |  |
| justhodl-official-pulse | 503 |  | 0 |

## Log


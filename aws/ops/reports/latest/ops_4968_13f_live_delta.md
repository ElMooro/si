# ops 4968 -- 13F live-delta

**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-08-24T12:55:48+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4968_13f_live_delta.py", line 217, in main
    src = (ROOTP / "aws/lambdas/%s/source/lambda_function.py"
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: unsupported operand type(s) for %: 'PosixPath' and 'str'

```

## Log
- `12:55:48` mark 2026-08-24T12:55:48+00:00
- `12:55:48` G-1 PASS
- `12:55:48` P0 EDGAR truth sweep — FROM the runner

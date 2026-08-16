# ops 4731 -- REG coverage gap + historical depth audit (read-only)

**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-08-16T03:28:58+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4731_reg_coverage_and_history_depth_audit.py", line 68, in main
    src = open(ROOT / "aws/lambdas/justhodl-provider-catalog/source/lambda_function.py").read()
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/si/si/aws/aws/lambdas/justhodl-provider-catalog/source/lambda_function.py'

```

## Log
## Part A -- REG coverage: what's producing real data but invisible on data.html


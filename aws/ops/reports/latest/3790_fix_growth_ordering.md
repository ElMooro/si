# ops 3790 — move growth block above the leaderboard build

**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-07-24T00:31:01+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/py_compile.py", line 144, in compile
    code = loader.source_to_code(source_bytes, dfile or file,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<frozen importlib._bootstrap_external>", line 1063, in source_to_code
  File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
  File "/home/runner/work/si/si/aws/lambdas/justhodl-chokepoint/source/lambda_function.py", line 864
    try:
    ^^^
SyntaxError: expected 'except' or 'finally' block

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3790_fix_growth_ordering.py", line 88, in main
    py_compile.compile(str(LF), doraise=True)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/py_compile.py", line 150, in compile
    raise py_exc
py_compile.PyCompileError:   File "/home/runner/work/si/si/aws/lambdas/justhodl-chokepoint/source/lambda_function.py", line 864
    try:
    ^^^
SyntaxError: expected 'except' or 'finally' block


```

## Data

| growth_block_offset | leaderboard_offset |
|---|---|
| 48480 | 46558 |

## Log
## G0 — prove the ordering defect

- `00:31:01` ✅ G0.block_found :: v4.2 block located
- `00:31:01` ✅ G0.pct_found :: v4.1 block located
- `00:31:01` ✅ G0.lead_found :: leaderboard build located
- `00:31:01` ✅ G0.is_after :: growth assigned AFTER the leaderboard snapshot — the confirmed bug
- `00:31:01` ✅ G0.block_sane :: extracted block is 2945 chars
- `00:31:01` ✅ FIX.anchor :: insert anchor unique

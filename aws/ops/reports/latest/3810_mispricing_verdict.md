# ops 3810 — v5.0 mispricing verdict (why/who/what)

**Status:** failure  
**Duration:** 1.3s  
**Finished:** 2026-07-24T17:30:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/py_compile.py", line 144, in compile
    code = loader.source_to_code(source_bytes, dfile or file,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<frozen importlib._bootstrap_external>", line 1063, in source_to_code
  File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
  File "/home/runner/work/si/si/aws/lambdas/justhodl-chokepoint/source/lambda_function.py", line 1211
    try:
    ^^^
SyntaxError: expected 'except' or 'finally' block

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3810_mispricing_verdict.py", line 283, in main
    py_compile.compile(str(LF), doraise=True)
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/py_compile.py", line 150, in compile
    raise py_exc
py_compile.PyCompileError:   File "/home/runner/work/si/si/aws/lambdas/justhodl-chokepoint/source/lambda_function.py", line 1211
    try:
    ^^^
SyntaxError: expected 'except' or 'finally' block


```

## Log
## G0 — keys verified live in ops 3809

- `17:30:07` ✅ G0.v441 :: engine at v4.4.1
- `17:30:07` ✅ G0.struct :: structural score present
- `17:30:07` ✅ G0.ledger_var :: ledger rows in scope for persistence
- `17:30:07` ✅ G0.datetime :: datetime available
- `17:30:07` ✅ G0.anchor :: splice anchor unique
- `17:30:08` ✅ G0.direction_map :: data/estimate-revisions.json -> direction_map n=393
- `17:30:08` ✅ G0.dark_map :: data/dark-pool.json -> dark_map n=939
- `17:30:08` ✅ G0.tickers :: data/finra-short.json -> tickers n=501
- `17:30:08` ✅ G0.all_qualifying :: data/earnings-pead.json -> all_qualifying n=244
- `17:30:08` ✅ G0.league :: data/industry-boom.json -> league n=119
## Splice v5.0


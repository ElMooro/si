
**Status:** failure  
**Duration:** 13.5s  
**Finished:** 2026-09-20T10:48:37+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5911_crisis_research_acceptance.py", line 75, in main
    runtimes[fn]=runtime(lam,fn)
                 ^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5911_crisis_research_acceptance.py", line 43, in runtime
    source=ROOT/'aws/lambdas'/fn/'source';configuration=json.loads((source.parent/'config.json').read_bytes())
                                                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/pathlib.py", line 1019, in read_bytes
    with self.open(mode='rb') as f:
         ^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/pathlib.py", line 1013, in open
    return io.open(self, mode, buffering, encoding, errors, newline)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/si/si/aws/lambdas/justhodl-allocator/config.json'

```

## Data

| exact_commit | linux_fixture_replay | packaged_files_checked | portable_reference_digest | verified_function |
|---|---|---|---|---|
|  | exact_match |  | 824e8102ab3ade1438004e23d828bdbb3d3fd49c1a1875c19a081ed8d11154ee |  |
| 0fdc2c779a1ea95a04c035e37ce8294699567cac |  | 46 |  | justhodl-ai |
| 0fdc2c779a1ea95a04c035e37ce8294699567cac |  | 20 |  | justhodl-ai-chat |

## Log


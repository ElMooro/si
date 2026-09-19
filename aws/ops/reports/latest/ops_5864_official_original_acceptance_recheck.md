
**Status:** failure  
**Duration:** 62.4s  
**Finished:** 2026-09-19T18:36:30+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5864_official_original_acceptance_recheck.py", line 60, in main
    assert output['dollar_leg']['tic_context']['status']=='immutable_descriptive_context'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Data

| function | invoke_status | runtimes | throttle_rejections |
|---|---|---|---|
|  |  | {'justhodl-tic-flows': {'commit': '6dce88007ae9b66d77a7e9d2ab2a3412ef42a2b9', 'code_sha256': '3JoF+rnPOpogkIDkof94y/XFcZwESLFx5QVY+nuyNwg='}, 'justhodl-official-pulse': {'commit': '45a306aa49a24591ca0e69ffa442d9d80bcab178', 'code_sha256': '+nQSo8/xkfxn7ilPADSDXzRKXyPxR7eOKUeTZd9UuGU='}} |  |
| justhodl-official-pulse | 200 |  | 0 |

## Log


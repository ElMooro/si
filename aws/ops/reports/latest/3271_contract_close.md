
**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-07-13T18:36:23+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3271_contract_close.py", line 39, in <module>
    or re.search(r"window\s*=\s*(\d+)", src)).group(1))
                                              ^^^^^
AttributeError: 'NoneType' object has no attribute 'group'

```

## Log



**Status:** failure  
**Duration:** 3.8s  
**Finished:** 2026-09-27T07:35:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6213_freight_original_baseline.py", line 101, in main
    baseline['consumers'][fn]=runtime(clients['lambda'],s3,clients['events'],clients['scheduler'],fn)
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6204_shipping_consumer_baseline.py", line 73, in runtime
    'repository_sources':{p.relative_to(ROOT).as_posix():retain(s3,p.read_bytes()) for p in [*paths,*shared,*([config] if config.exists() else [])]}}
                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6204_shipping_consumer_baseline.py", line 26, in retain
    if not isinstance(raw,bytes) or not 0<len(raw)<=64*1024*1024:raise ValueError('Complete bounded original required')
                                                                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Complete bounded original required

```

## Log


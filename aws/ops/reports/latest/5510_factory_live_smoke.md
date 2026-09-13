
**Status:** failure  
**Duration:** 9.2s  
**Finished:** 2026-09-13T14:36:39+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5510_factory_live_smoke.py", line 175, in <module>
    main(rep)
  File "/home/runner/work/si/si/aws/ops/pending/ops_5510_factory_live_smoke.py", line 136, in main
    if status!=401:raise RuntimeError('public_admission_boundary:'+str(status))
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: public_admission_boundary:403

```

## Log


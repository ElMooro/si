
**Status:** failure  
**Duration:** 1.9s  
**Finished:** 2026-10-01T02:25:55+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6404_symdir_identifier_integrity_acceptance.py", line 57, in main
    before=normalized(runtime(*clients,FN),commit);after=normalized(runtime(*clients,FN),commit)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6404_symdir_identifier_integrity_acceptance.py", line 44, in normalized
    validate(value, commit)
  File "/home/runner/work/si/si/aws/ops/staged/ops_6404_symdir_identifier_integrity_acceptance.py", line 36, in validate
    raise ValueError('Native sources or original controls differ')
ValueError: Native sources or original controls differ

```

## Log


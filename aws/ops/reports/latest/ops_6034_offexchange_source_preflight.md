
**Status:** failure  
**Duration:** 2.5s  
**Finished:** 2026-09-25T01:35:39+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6034_offexchange_source_preflight.py", line 121, in main
    try:raw=get(s3,key)
            ^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6034_offexchange_source_preflight.py", line 34, in get
    return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6034_offexchange_source_preflight.py", line 30, in bounded
    if not 0<len(raw)<=limit:raise ValueError('Whole source byte bound')
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Whole source byte bound

```

## Log


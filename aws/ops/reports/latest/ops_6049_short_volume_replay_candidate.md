
**Status:** failure  
**Duration:** 24.1s  
**Finished:** 2026-09-25T04:18:12+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6049_short_volume_replay_candidate.py", line 62, in main
    compiled = model.compile_output(inputs, read)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/short_volume_research_model.py", line 153, in compile_output
    record = measurements.comparisons(name, histories[name], dates)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/short_volume_measurements.py", line 115, in comparisons
    'descriptive_z_score': rounded(difference / deviation) if difference is not None and deviation else None,
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/short_volume_measurements.py", line 22, in rounded
    result = value.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
decimal.InvalidOperation: [<class 'decimal.InvalidOperation'>]

```

## Log


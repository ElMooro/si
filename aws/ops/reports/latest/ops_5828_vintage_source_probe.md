
**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-09-19T04:09:58+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5828_vintage_source_probe.py", line 40, in main
    raise ValueError('FRED '+sid+' '+endpoint+' HTTP '+str(exc.code)+': '+message[:500]) from None
ValueError: FRED WALCL series HTTP 400: {"error_code":400,"error_message":"Bad Request.  Variable realtime_end can not be after today's date (2026-09-18) unless it's equal to the real-time max date 9999-12-31."}

```

## Log


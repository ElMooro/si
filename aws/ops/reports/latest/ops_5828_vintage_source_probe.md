
**Status:** failure  
**Duration:** 3.7s  
**Finished:** 2026-09-19T04:14:26+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5828_vintage_source_probe.py", line 40, in main
    raise ValueError('FRED '+sid+' '+endpoint+' HTTP '+str(exc.code)+': '+message[:500]) from None
ValueError: FRED RRPONTSYD series/observations HTTP 400: {"error_code":400,"error_message":"Bad Request.  There are 2605 vintage dates in the specified real-time period: 1776-07-04 to 9999-12-31.  This exceeds the maximum number of vintage dates allowed for this file type (2000)."}

```

## Data

| endpoint | key | response_count | returned_rows | series |
|---|---|---|---|---|
| series | data/vintage-research/probe-originals/889e0d35147ad54448ad952e4b1cd18ef604ea2b82e5abd853f0e375ffd5d8a7.json.gz | None | 4 | WALCL |
| series/observations | data/vintage-research/probe-originals/45ff3d89c67ecfba6c56466989fab228f2e975fdd89dd9dce1900b505ffa74eb.json.gz | 1779 | 1000 | WALCL |
| series | data/vintage-research/probe-originals/50548382fc5000cff4f271b8a93a254d49f3ef5c4f80e32c96ef5554a5cd0163.json.gz | None | 4 | WTREGEN |
| series/observations | data/vintage-research/probe-originals/d4fb367303c044c35eaf95a63535cd54cfe2f30d71ab1f57e9561e8fb017454a.json.gz | 2743 | 1000 | WTREGEN |
| series | data/vintage-research/probe-originals/d10fb4a567baee84166bfa86e73ad82738dc08d7f2c77ac593c7c922fe80045b.json.gz | None | 1 | RRPONTSYD |

## Log


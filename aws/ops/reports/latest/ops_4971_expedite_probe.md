## P0 imf-full _discover error (from state) + runner re-parse

**Status:** failure  
**Duration:** 0.8s  
**Finished:** 2026-08-24T23:10:16+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4971_expedite_probe.py", line 111, in <module>
    u = fp.get("url") or fp.get("last_url")
        ^^^^^^
AttributeError: 'str' object has no attribute 'get'

```

## Log
- `23:10:16`   Lambda _discover err: {"err": "catalog parse yielded 0 flows"}
- `23:10:16`   runner refetch: status=204 bytes=0 loose-ids=0 sample=[]
## P1 census: the 5 structural failures, stored reasons

- `23:10:16`   state at data/warm/census-us/_state/state.json
- `23:10:16`   FAIL aies-miscsector          "no data any mode (last HTTP 400)"
- `23:10:16`   FAIL asm-industry             "no data any mode (last HTTP 400)"
- `23:10:16`   FAIL poverty-saipe-schdist    "no data any mode (last HTTP 400)"
- `23:10:16`   FAIL pseo-earnings            "no data any mode (last HTTP 400)"
- `23:10:16`   FAIL pseo-flows               "no data any mode (last HTTP 400)"

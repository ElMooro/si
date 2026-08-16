# ops 4766 -- dollar fix verify (v1.2 retry-invoke)

**Status:** failure  
**Duration:** 28.1s  
**Finished:** 2026-08-16T18:38:35+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4767_repo_dollar_verify_v12.py", line 53, in main
    rep.log(f"  still missing: {m['id']} cats_tried={m['cats_tried']}")
                                                     ~^^^^^^^^^^^^^^
KeyError: 'cats_tried'

```

## Data

| check | value |
|---|---|
| v11_answered | True |
| dollar_rows | 0 |

## Log
- `18:38:35` attempt 0: v=1.2 series=296 secs=27.3

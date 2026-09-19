
**Status:** failure  
**Duration:** 74.6s  
**Finished:** 2026-09-19T19:58:34+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5871_etf_published_research_acceptance.py", line 64, in main
    zeros=[v for v in rows if v['nav_decimal']=='0'];assert zeros and all(v['nav_valued_share_change_decimal'] is None for v in zeros);zero_rows+=len(zeros)
                                                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Data

| code_sha256 | collection_reinvoked | commit | prior_transport_failure |
|---|---|---|---|
| c9XriboytlZZbgmeOjRj3L01m507hM0vWSZH73eWxBw= | False | 619ccd108921c174705d92c22947fe56ed016e39 | ops5870 connection closed after publication |

## Log


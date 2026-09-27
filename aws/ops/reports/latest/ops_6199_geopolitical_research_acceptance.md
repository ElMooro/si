
**Status:** failure  
**Duration:** 1.6s  
**Finished:** 2026-09-27T11:54:42+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6199_geopolitical_research_acceptance.py", line 29, in main
    'bytes':len(raw),'sha256':store.sha(raw)}
                              ^^^^^^^^^
AttributeError: module 'geo_news_store' has no attribute 'sha'

```

## Log


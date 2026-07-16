## Fuse into risk-regime (risk-off block)

**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-07-16T17:36:52+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/3397_fuse_risk_crisis.py", line 45, in <module>
    rr = deploy_and_run(r, "justhodl-risk-regime", "data/risk-regime.json", None)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/3397_fuse_risk_crisis.py", line 24, in deploy_and_run
    eb_rule_name=c["schedule"]["rule_name"], eb_schedule=c["schedule"]["cron"],
                 ~~~~~~~~~~~~~^^^^^^^^^^^^^
KeyError: 'rule_name'

```

## Log


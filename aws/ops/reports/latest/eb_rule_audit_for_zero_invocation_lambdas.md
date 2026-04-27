# EB rule audit — 0-invocation Lambdas

**Status:** failure  
**Duration:** 1.2s  
**Finished:** 2026-04-27T17:21:53+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_eb_rule_audit_zero_invocations.py", line 182, in main
    if not has_invoke_permission(fn, rarn):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/_eb_rule_audit_zero_invocations.py", line 109, in has_invoke_permission
    if stmt.get("Principal", {}).get("Service") == "events.amazonaws.com":
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'str' object has no attribute 'get'

```

## Log
- `17:21:52` Targets: justhodl-email-reports-v2, justhodl-khalid-metrics, justhodl-data-collector, scrapeMacroData, fmp-stock-picks-agent, news-sentiment-agent, justhodl-intelligence, justhodl-repo-monitor
- `17:21:52` 
## justhodl-email-reports-v2

- `17:21:52`   CloudWatch 24h: invocations=0, errors=0
- `17:21:52`   Rule 'DailyEmailReportsV2' state=ENABLED schedule=cron(0 12 * * ? *)
- `17:21:53` ✅     ✓ added lambda:InvokeFunction permission (AllowEB-DailyEmailReportsV2-1777310512)
## justhodl-khalid-metrics

- `17:21:53`   CloudWatch 24h: invocations=0, errors=0
- `17:21:53`   Rule 'justhodl-khalid-metrics-refresh' state=ENABLED schedule=cron(0 11 * * ? *)

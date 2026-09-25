
**Status:** failure  
**Duration:** 542.6s  
**Finished:** 2026-09-25T14:11:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/checks/share_structure_batch_runner.py", line 130, in main
    ref,executed=campaign.run_batch(s3,request_id,plan_ref,part,credential,baseline.now,adoption,
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/share_structure_campaign.py", line 76, in run_batch
    results=collect(specs,{},fetch,checkpoint,workers=3)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/financial_statement_campaign.py", line 138, in collect
    raise ValueError('Full source capture incomplete; adopt retained successes in a new reviewed campaign')
ValueError: Full source capture incomplete; adopt retained successes in a new reviewed campaign

```

## Log


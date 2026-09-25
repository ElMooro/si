
**Status:** failure  
**Duration:** 52.7s  
**Finished:** 2026-09-25T03:34:25+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6045_offexchange_native_acceptance.py", line 121, in main
    consumers[name]=runtime(lam,s3,events,scheduler,name);assert consumers[name]['receipt']=={'status':'matched','commit':commit},name
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_5975_etf_constituent_source_preflight.py", line 76, in runtime
    actual=scheduler.get_schedule(Name=declared['schedule_name']);assert actual['Target']['Arn']==cfg['FunctionArn']
                                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Log


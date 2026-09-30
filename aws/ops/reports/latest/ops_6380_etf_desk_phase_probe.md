
**Status:** failure  
**Duration:** 2.5s  
**Finished:** 2026-09-30T20:44:29+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6380_etf_desk_phase_probe.py", line 72, in main
    raise RuntimeError('s3:HeadObject public ETF artifact failed: ' + error_code(exc)) from None
RuntimeError: s3:HeadObject public ETF artifact failed: 404

```

## Data

| aws_writes | binding | bytes | environment_values_reported | etag | function | last_modified | native_invocations | private_original_reads | provider_requests | public_key | runtime | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 |  |  | 0 |  |  |  | 0 | 0 | 0 |  |  | Three exact ETF dependency bindings, selected Lambda metadata and three public object headers only |
|  | {'kind': 'events', 'scheduler_exact_name': 'not_found', 'rule': {'Name': 'justhodl-etf-global-desk-daily', 'Arn': 'arn:aws:events:us-east-1:857687956942:rule/justhodl-etf-global-desk-daily', 'ScheduleExpression': 'cron(20 22 * * ? *)', 'State': 'ENABLED', 'Description': '18:20 ET daily — after ETF Global EOD window, before constituents pressure', 'EventBusName': 'default'}, 'event_pattern_present': False, 'targets': [{'Id': 'audit-justhodl-etf-global-desk', 'Arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-global-desk', 'input_present': False, 'input_bytes': None, 'input_sha256': None, 'other_field_names': []}]} |  |  |  | justhodl-etf-global-desk |  |  |  |  |  |  |  |
|  |  |  |  |  | justhodl-etf-global-desk |  |  |  |  |  | {'FunctionName': 'justhodl-etf-global-desk', 'FunctionArn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-global-desk', 'Runtime': 'python3.12', 'State': 'Active', 'LastUpdateStatus': 'Successful', 'LastModified': '2026-09-21T11:40:38.000+0000', 'Timeout': 900, 'MemorySize': 4096, 'CodeSha256': 'IaKwi3IH3r5A9Wx5zcfUEwvVFjZOCSoloeg4XBCIPHc=', 'RevisionId': '658f7c63-0d0e-4247-95cd-8daff0cedd40'} |  |
|  | {'kind': 'events', 'scheduler_exact_name': 'not_found', 'rule': {'Name': 'justhodl-etf-constituents-daily', 'Arn': 'arn:aws:events:us-east-1:857687956942:rule/justhodl-etf-constituents-daily', 'ScheduleExpression': 'cron(45 22 * * ? *)', 'State': 'ENABLED', 'Description': '17:45 ET daily — 45min after flow engine, after macro regime, after AI strategist', 'EventBusName': 'default'}, 'event_pattern_present': False, 'targets': [{'Id': '1', 'Arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-constituents', 'input_present': False, 'input_bytes': None, 'input_sha256': None, 'other_field_names': []}]} |  |  |  | justhodl-etf-constituents |  |  |  |  |  |  |  |
|  |  |  |  |  | justhodl-etf-constituents |  |  |  |  |  | {'FunctionName': 'justhodl-etf-constituents', 'FunctionArn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-constituents', 'Runtime': 'python3.12', 'State': 'Active', 'LastUpdateStatus': 'Successful', 'LastModified': '2026-09-21T09:38:20.000+0000', 'Timeout': 840, 'MemorySize': 3072, 'CodeSha256': 'tQijiYgGGGHPF/C5v8io2+OtWNhiZSBpUOXiq4O3iW0=', 'RevisionId': '9055ed7f-dd57-478f-a26e-bffb1d44844b'} |  |
|  | {'kind': 'events', 'scheduler_exact_name': 'not_found', 'rule': {'Name': 'justhodl-flow-lookthrough-daily', 'Arn': 'arn:aws:events:us-east-1:857687956942:rule/justhodl-flow-lookthrough-daily', 'ScheduleExpression': 'cron(15 23 * * ? *)', 'State': 'ENABLED', 'Description': '18:15 ET daily — after etf-flows (22:00) + capital-flow-radar (22:30)', 'EventBusName': 'default'}, 'event_pattern_present': False, 'targets': [{'Id': '1', 'Arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-flow-lookthrough', 'input_present': False, 'input_bytes': None, 'input_sha256': None, 'other_field_names': []}]} |  |  |  | justhodl-flow-lookthrough |  |  |  |  |  |  |  |
|  |  |  |  |  | justhodl-flow-lookthrough |  |  |  |  |  | {'FunctionName': 'justhodl-flow-lookthrough', 'FunctionArn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-flow-lookthrough', 'Runtime': 'python3.12', 'State': 'Active', 'LastUpdateStatus': 'Successful', 'LastModified': '2026-09-21T09:38:49.000+0000', 'Timeout': 840, 'MemorySize': 3072, 'CodeSha256': 'ezYoNdBBHdtWzIZQf9p5sWlCrwa304yPLCX0dMtb6FM=', 'RevisionId': '015cb700-3122-4d9b-8029-4ab9d4483b80'} |  |
|  |  | 5063638 |  | "3acfa5784a1234e93ef8c31b45fc6e74" |  | 2026-09-29 22:23:44+00:00 |  |  |  | data/etf-desk-research.json |  |  |
|  |  | 1116729 |  | "7785b6a3f7cc6f0dfb0875aaf3cd475f" |  | 2026-09-29 22:49:36+00:00 |  |  |  | data/etf-holdings-research.json |  |  |

## Log


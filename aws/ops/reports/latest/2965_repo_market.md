## 0. Probe NY Fed markets API from the runner

**Status:** failure  
**Duration:** 1.3s  
**Finished:** 2026-07-07T06:08:25+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_2965_repo_market.py", line 114, in main
    deploy_lambda(report=rep, function_name=FN,
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 248, in deploy_lambda
    create_or_update_lambda(
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 102, in create_or_update_lambda
    _lam.create_function(
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the CreateFunction operation: 1 validation error detected: Value 'Repo Market Desk - dedicated overnight-funding stress engine. NY Fed SOFR distribution (p1/p25/p75/p99 + volume), TGCR/BGCR/EFFR/OBFR, SOFR-IORB with IOER splice, RRP/SRF/discount-window/swap-line buffers, reserves drain, calendar context, episode ranking since 2018, 9-component 0-100 score + Telegram regime tripwires. data/repo-market.json.' at 'description' failed to satisfy constraint: Member must have length less than or equal to 256

```

## Data

| env_keys | first_row_keys | probe_rows | sep17_row |
|---|---|---|---|
|  | ['effectiveDate', 'percentPercentile1', 'percentPercentile25', 'percentPercentile75', 'percentPercentile99', 'percentRate', 'revisionIndicator', 'type', 'volumeInBillions'] | 27 | [{"effectiveDate": "2019-09-17", "type": "SOFR", "percentRate": 5.25, "percentPercentile1": 2.25, "percentPercentile25": 5.0, "percentPercentile75": 5.85, "percentPercentile99": 9.0, "volumeInBillions": 1177, "revisionIndicator": ""}] |
| ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET', 'TELEGRAM_CHAT_ID', 'TELEGRAM_TOKEN'] |  |  |  |

## Log
## 1. Env bundle + create justhodl-repo-market

- `06:08:24`   zip: 9355 bytes
## 1. Lambda

- `06:08:24`   Lambda missing — creating

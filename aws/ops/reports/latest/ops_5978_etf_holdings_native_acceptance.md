
**Status:** failure  
**Duration:** 967.7s  
**Finished:** 2026-09-21T08:45:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5978_etf_holdings_native_acceptance.py", line 286, in main
    request = invoke_public(lam, s3, store, fn); requests[fn] = request; r.kv(**{kind+'_request': request})
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_5978_etf_holdings_native_acceptance.py", line 86, in invoke_public
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native producer did not publish; inspect durable request without reinvoking'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: Native producer did not publish; inspect durable request without reinvoking

```

## Data

| audit_originals_replayed | retained_audit_manifest | runtimes |
|---|---|---|
|  |  | {'justhodl-equity-confluence': {'commit': 'a073f17597a303e1cf27cee408d0d42837b257a7', 'code_sha256': 'QuTxRhexNHq6fXSXt/bb13o24TgzUgd7NwGikvbjiJ0=', 'packaged_files_checked': 5, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-etf-constituents': {'commit': 'a073f17597a303e1cf27cee408d0d42837b257a7', 'code_sha256': 'XXEErRD4/zVIbEOABxCkcu7DYaZSpZj6hAd8+wmN5b8=', 'packaged_files_checked': 13, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-flow-lookthrough': {'commit': 'a073f17597a303e1cf27cee408d0d42837b257a7', 'code_sha256': 'GALlTCG+pjwsCS/hdLr1ZMx6tpFVZYsQC8ck+aNz8kU=', 'packaged_files_checked': 9, 'all_packaged_sources_match': True, 'alias': None}} |
| {'current_BND': {'rows': 14750, 'effective_dates': {'2026-08-31': 14750}, 'weight_audit': {'raw_observed_sum_decimal': '0.98477670', 'rows_with_numeric_weight': 14750, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 3, 'replayed': True}, 'current_EFA': {'rows': 687, 'effective_dates': {'2026-09-17': 687}, 'weight_audit': {'raw_observed_sum_decimal': '1.00000030', 'rows_with_numeric_weight': 687, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'current_SOXL': {'rows': 57, 'effective_dates': {'2026-09-18': 57}, 'weight_audit': {'raw_observed_sum_decimal': '3.1711496262', 'rows_with_numeric_weight': 57, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'current_SPY': {'rows': 505, 'effective_dates': {'2026-09-17': 505}, 'weight_audit': {'raw_observed_sum_decimal': '0.9995599352', 'rows_with_numeric_weight': 505, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'current_TLT': {'rows': 49, 'effective_dates': {'2026-09-17': 49}, 'weight_audit': {'raw_observed_sum_decimal': '1.0000000', 'rows_with_numeric_weight': 49, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'current_VOO': {'rows': 515, 'effective_dates': {'2026-08-31': 515}, 'weight_audit': {'raw_observed_sum_decimal': '1.0000009', 'rows_with_numeric_weight': 515, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'prior_SPY': {'rows': 505, 'effective_dates': {'2026-08-21': 505}, 'weight_audit': {'raw_observed_sum_decimal': '0.9994817659', 'rows_with_numeric_weight': 505, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}, 'prior_VOO': {'rows': 517, 'effective_dates': {'2026-07-31': 517}, 'weight_audit': {'raw_observed_sum_decimal': '0.9999992', 'rows_with_numeric_weight': 517, 'rows_without_numeric_weight': 0, 'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}, 'original_pages': 1, 'replayed': True}} | 9fcdfada29f0e2a54ff504b2dc0c14f83e3a4b51e9d16cf72c379e4c2035e772 |  |

## Log


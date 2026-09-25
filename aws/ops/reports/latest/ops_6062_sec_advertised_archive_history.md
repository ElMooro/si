
**Status:** failure  
**Duration:** 6.9s  
**Finished:** 2026-09-25T07:35:10+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6062_sec_advertised_archive_history.py", line 113, in main
    retained[url] = fetch(s3, url, deadline)
                    ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6062_sec_advertised_archive_history.py", line 69, in fetch
    result['inventory'] = sec.inventory(body, url, result['received_at'][:10])
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/sec_ftd_inventory.py", line 136, in inventory
    records = rows(body, url, cutoff)
              ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/sec_ftd_inventory.py", line 110, in rows
    raise ValueError(f'Settlement outside advertised archive at line {line_number}')
ValueError: Settlement outside advertised archive at line 2

```

## Data

| engine_invocations | error_type | private_account_reads | provider_requests | public_head_writes | retained_progress |
|---|---|---|---|---|---|
| 0 | ValueError | 0 | 1 | 0 | {'captures': {'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip': {'headers': {'content-length': '1393881', 'content-type': 'application/octet-stream', 'date': 'Fri, 25 Sep 2026 07:15:22 GMT', 'last-modified': 'Tue, 15 Sep 2026 13:09:21 GMT'}, 'http_status': 200, 'inventory': {'control_totals': {'quantity_checksum_matches': True, 'quantity_sum_is_file_integrity_control_not_economic_flow': True, 'quantity_sum_source_line': 61553, 'record_count_matches': True, 'record_count_source_line': 61552, 'reported_quantity_sum': '1956006104', 'reported_record_count': 61550}, 'cusips': 13056, 'cusips_with_multiple_reported_labels': 56, 'daily_new_fail_flow_measured': False, 'fail_age_measured': False, 'forced_buy_in_forecast_qualified': False, 'historical_publication_time_verified': False, 'member': {'bytes': 3874330, 'name': 'cnsfails202608b.txt', 'sha256': '2976f599ff70b48fafa50359809fbaee58a0e0495e60f19475c2b37fc8ce6b22'}, 'missing_previous_day_prices': 359, 'price_currency_explicit_in_file': False, 'price_observation_date_explicit_in_file': False, 'quantity_definition': 'aggregate_net_outstanding_fail_balance_on_settlement_date', 'reported_zero_balances': 0, 'rows': 61550, 'security_continuity_verified': False, 'settlements': {'2026-08-17': 5738, '2026-08-18': 5392, '2026-08-19': 5717, '2026-08-20': 5989, '2026-08-21': 5600, '2026-08-24': 5949, '2026-08-25': 5538, '2026-08-26': 5341, '2026-08-27': 5525, '2026-08-28': 5204, '2026-08-31': 5557}, 'short_sale_origin_verified': False, 'source_schema_valid': True, 'symbols': 13060, 'symbols_with_multiple_cusips': 37}, 'original': {'bytes': 1393881, 'key': 'audit-private/20260909-originals/sec-ftd-research/0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203.bin', 'sha256': '0c90519d0d0e959b86331c5fe546cefa027c8a17998111270001b0ee61bbf203'}, 'received_at': '2026-09-25T07:15:22.562531+00:00', 'requested_at': '2026-09-25T07:15:21.922920+00:00', 'status': 'response_retained', 'url': 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip'}, 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608a.zip': {'headers': {'content-length': '1352929', 'content-type': 'application/octet-stream', 'date': 'Fri, 25 Sep 2026 07:27:30 GMT', 'last-modified': 'Mon, 31 Aug 2026 11:54:19 GMT'}, 'http_status': 200, 'inventory': {'control_totals': {'quantity_checksum_matches': True, 'quantity_sum_is_file_integrity_control_not_economic_flow': True, 'quantity_sum_source_line': 59957, 'record_count_matches': True, 'record_count_source_line': 59956, 'reported_quantity_sum': '2449972489', 'reported_record_count': 59954}, 'cusips': 13255, 'cusips_with_multiple_reported_labels': 25, 'daily_new_fail_flow_measured': False, 'fail_age_measured': False, 'forced_buy_in_forecast_qualified': False, 'historical_publication_time_verified': False, 'member': {'bytes': 3774425, 'name': 'cnsfails202608a.txt', 'sha256': '8757ee214aa593510b24cc686fcc68a39d18a50d897491449fdd70b3d15e13e4'}, 'missing_previous_day_prices': 369, 'price_currency_explicit_in_file': False, 'price_observation_date_explicit_in_file': False, 'quantity_definition': 'aggregate_net_outstanding_fail_balance_on_settlement_date', 'reported_zero_balances': 0, 'rows': 59954, 'security_continuity_verified': False, 'settlements': {'2026-08-03': 6269, '2026-08-04': 6773, '2026-08-05': 5945, '2026-08-06': 6108, '2026-08-07': 5743, '2026-08-10': 5581, '2026-08-11': 5882, '2026-08-12': 5837, '2026-08-13': 5749, '2026-08-14': 6067}, 'short_sale_origin_verified': False, 'source_schema_valid': True, 'symbols': 13247, 'symbols_with_multiple_cusips': 28}, 'original': {'bytes': 1352929, 'key': 'audit-private/20260909-originals/sec-ftd-research/7eac7eb87278af473d8ba63387da2bb1ce22432261589d732c54e2c54640fefa.bin', 'sha256': '7eac7eb87278af473d8ba63387da2bb1ce22432261589d732c54e2c54640fefa'}, 'received_at': '2026-09-25T07:27:31.183305+00:00', 'requested_at': '2026-09-25T07:27:30.676577+00:00', 'status': 'response_retained', 'url': 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608a.zip'}}} |

## Log


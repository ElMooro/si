- `13:25:49` ✗ Bounded evidence incomplete; do not infer zero activity or absent bindings.
**Status:** failure  
**Duration:** 0.8s  
**Finished:** 2026-10-01T13:25:49+00:00  

## Error

```
SystemExit: 1
```

## Data

| api_call_limit | api_calls | aws_mutations | bodies_or_logs_read | bounded_reads_complete | function | inventory_complete | metrics | native_invocations | output_metadata | probe_commit | probe_source_sha256 | read_errors | rules | schedules |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | ba572377be4e9e9f203d01d401d0e1f829dd9ee2 | c77d3718fa5617a451a518b613382b30553fb6ae413a8253d4472cbfda11da6f |  |  |  |
| 33 | 10 | 0 | 0 | False | justhodl-industry-case | False | {'start_inclusive': '2026-10-01T12:55:00+00:00', 'end_exclusive': '2026-10-01T13:25:00+00:00', 'series': {'invocations': {'status': 'unavailable_or_partial', 'observed_value': None, 'reported_minutes': 0, 'expected_minutes': 30, 'api_complete': True, 'all_minutes_reported': False, 'unit': 'count'}, 'errors': {'status': 'unavailable_or_partial', 'observed_value': None, 'reported_minutes': 0, 'expected_minutes': 30, 'api_complete': True, 'all_minutes_reported': False, 'unit': 'count'}, 'throttles': {'status': 'unavailable_or_partial', 'observed_value': None, 'reported_minutes': 0, 'expected_minutes': 30, 'api_complete': True, 'all_minutes_reported': False, 'unit': 'count'}, 'duration': {'status': 'unavailable_or_partial', 'observed_value': None, 'reported_minutes': 0, 'expected_minutes': 30, 'api_complete': True, 'all_minutes_reported': False, 'unit': 'milliseconds'}}, 'pagination_incomplete': False, 'limitation': 'Sparse or absent minutes are unknown, not zero. Ingestion may lag. Function aggregates do not identify code versions, triggers, completion, or successful writes.'} | 0 | {'last_modified': '2026-08-18T03:56:00+00:00', 'bytes': 345721, 'etag': '"aac7ab97bee95ac5f7a4f940908aa4f3"', 'encoding': 'gzip', 'body_read': False, 'semantic_freshness_verified': False} |  |  | [] | {'rules': [], 'lookup_matches': 0, 'bounded_scope_incomplete': False, 'issues': [], 'inventory_complete': False, 'scope': 'Default bus; unqualified, $LATEST and live target lookups only. Other qualifiers, buses and indirect triggers unexamined. Empty results do not establish no binding.'} | {'schedules': [], 'matching_summaries': 0, 'bounded_scope_incomplete': False, 'issues': [], 'inventory_complete': False, 'scope': 'Regional direct Lambda targets across returned groups; universal AWS SDK targets and indirect invocations unexamined. No global absence claim.'} |

## Log


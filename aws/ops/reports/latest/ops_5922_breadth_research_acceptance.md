
**Status:** failure  
**Duration:** 124.1s  
**Finished:** 2026-09-20T13:10:57+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5922_breadth_research_acceptance.py", line 134, in main
    assert packet['quality']['status']=='fresh' and len(packet['source_evidence'])==253 and not packet['source_errors']
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Data

| acceptance_consumer_invocations | accepted_public_request | exact_public_request | linux_fixture_replay | notifications_sent | pages_commit | paid_ai_calls | portable_reference_digest | portfolio_writes | private_account_reads | runtimes | schedules |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 |  |  | exact_match | 0 |  | 0 | 64f00746747b07b73bcd61c0d8525436bf4ea123cc654cac143f050f83f4633e | 0 | 0 | {'justhodl-market-internals': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': 'MC0pYSXM0QJfdpz063kjBxzMXcaW8NxF+ppBjcPVP/s=', 'packaged_files_checked': 5, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-playbook-engine': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': 'agjyvxcfL5DZHmrglEpXHomTXeAkmeK+02Y+IvkTwBA=', 'packaged_files_checked': 6, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-signal-board': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': 'iM6jAMsZVFXBqyalKRBxtLs7mZ7oPn4yZPT2Ouj05MY=', 'packaged_files_checked': 8, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-symbol-dictionary': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': '6umZQq60+1ZaAE5N+myAfjpxUTbZvMBD4zmIbT6uiEg=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-thesis-engine': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': 'aQvXmy1sc60iyVp6lAwNqk+EGHTRfqXLFkouuXkHMZE=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-wl-engines': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': '8P4Sy9FhIKD1iQkS/C66pLtDS0VV+ZstGWtMBKloV4Q=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}} |  |
|  |  |  |  |  | 3a3408be40f4c60c41beab29bdbab6952894534e |  |  |  |  |  | {'market-internals-daily': {'state': 'ENABLED', 'expression': 'cron(40 12 ? * TUE-SAT *)', 'bound_targets': 1}, 'justhodl-market-internals-daily': {'removed_breadth_targets': 1, 'remaining_other_targets': 0, 'state': 'DISABLED'}, 'market-internals-hourly': {'removed_breadth_targets': 1, 'remaining_other_targets': 0, 'state': 'DISABLED'}} |
|  | {'request_id': 'chatgpt-native-breadth-3a3408be40f4-1', 'status_key': 'data/breadth-research/requests/0722a09cc35b49c282cabc2fa85ea96630bad62148b779c4bbf45b59b19cb265.json', 'aws_transport_request_id': 'f50a46fc-7851-41d9-ad1e-6ab440864b97'} |  |  |  |  |  |  |  |  |  |  |
|  |  | {'as_of': '2026-09-18', 'completed_at': '2026-09-20T13:10:15.092553+00:00', 'contract': 'breadth-public-request.v1', 'execution_id': 'f50a46fc-7851-41d9-ad1e-6ab440864b97', 'generated_at': '2026-09-20T13:09:30.980149+00:00', 'notifications_sent': 0, 'paid_ai_calls': 0, 'phase': 'complete', 'portfolio_writes': 0, 'private_account_reads': 0, 'published': True, 'quality': {'basis': 'native_session_completeness_and_original_source_collection', 'failed_sessions': 253, 'status': 'unavailable'}, 'replay': {'manifest_key': 'data/breadth-research/runs/2afad1ac241b022981b96d7433ed33fd418177fe7f88731f32b3ba6158dde65d.json', 'output_sha256': '005c49f08c4e7677335e03feec09895b523ffb3072d2b24859d2c577d188dd1d'}, 'request_id': 'chatgpt-native-breadth-3a3408be40f4-1', 'started_at': '2026-09-20T13:09:07.659933+00:00', 'status': 'complete'} |  |  |  |  |  |  |  |  |  |

## Log


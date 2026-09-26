
**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-09-26T23:19:34+00:00  

## Data

| account_reads | actual_runtime | archive_writes | compiler_sha256 | expected_commit | history_writes | native_invocations | notifications_sent | preserved_history_rows | provider_requests | public_writes | publication | schedules_changed | scope | source_verification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'Nfy9HT46r3ig/ASff0YvE3/nISXMnMhH/Y1H4jVwPWk=', 'source_files_checked': 3, 'handler_bytes': 13394, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '657cc27c4c95daf5372c1ba3c2fe7c9e408b7ddd'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'global-sovereign-12h', 'state': 'ENABLED', 'expression': 'cron(15 6,18 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-global-sovereign', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | {'lambda_function.py': 'de138946c80ad8c3f996e08f25dab6335f1a1b8dd0e40cf77772f48924646226', 'sovereign_history.py': '617a3a6cd394ebde7f6e56f13a721d23c3f309bc7fc00abbbe71f6646c37a78f', 'sovereign_sources.py': '71743be36292345f130dc5468b69fa44719fa3cbd7416336f0899f1dc77b8c97'} | 657cc27c4c95daf5372c1ba3c2fe7c9e408b7ddd | 0 | 0 | 0 | 73 | 0 | 0 | {'generated_at': '2026-09-26T18:16:52.868630+00:00', 'version': '1.4.0', 'whole_bytes': 22506, 'whole_sha256': '4f275087d42c194e83d8ee1b15685cb7f75a4973a66a059b532dfc07225642d2'} | 0 | Exact actual source package and retained direct provider fields when naturally published. No model, source-definition, quote-clock or portfolio qualification. | {'status': 'pending_original_scheduled_acquisition'} |

## Log


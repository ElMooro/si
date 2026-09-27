
**Status:** success  
**Duration:** 7.2s  
**Finished:** 2026-09-27T01:46:38+00:00  

## Data

| account_reads | actual_runtime | archive_writes | baseline_sha256 | complete_derived_input_bytes | complete_predecessor_countries | complete_predecessor_preserved | current_head_sha256 | expected_commit | fixture_countries | fixture_excluded | fixture_public_head_writes | fixture_replay | fixture_scope | history_writes | native_invocations | native_publication | notifications_sent | provider_requests | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'VKFjFlhjdvgq/2BjMxARVyNi9btL94p3JpSVEiVJoVg=', 'source_files_checked': 2, 'handler_bytes': 30076, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'f648d56424745e88a3449d133aaa68b205a6b443'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'global-recession-sched', 'state': 'ENABLED', 'expression': 'cron(40 12 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-global-recession', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 863712562cb23f3ac62fb46aaea39d14a98f1ce48afe1ae1f01ab5e37520016a | {'data/global-business-cycle.json': 261250, 'data/oecd-cli.json': 22861, 'data/portwatch.json': 109122, 'data/china-liquidity.json': 6804, 'data/indicator-bus.json': 2862350, 'data/global-recession.json': 26822} | 34 | True | bee30cf67d4dd85d084c48e63d8c05e21f068de3537ad4f6e4be0ba346b9ab8e | f648d56424745e88a3449d133aaa68b205a6b443 | 34 | 0 | 1 | {'contract': 'recession-native-replay.v1', 'complete_native_stages': 2, 'input_operations': 5, 'processing_clocks': 3, 'final_calculation_sha256': '3b89050545b7a9adc7c6e65f65d304ccbac3949de0df38fc9cddb305d0e9a5df', 'point_in_time_verified': False, 'forecast_qualified': False} | Complete retained derived inputs, both native calculations and offline clock replay in memory. FRED disabled for this fixture; not original-provider replay of the historical baseline. | 0 | 0 | {'status': 'pending_original_1240_schedule', 'generated_at': '2026-09-26T12:40:04+00:00', 'version': '1.3.1'} | 0 | 0 | 0 | 0 | Retained evidence, exact calculation replay and research authority boundary. Definition validation, original historical vintages, calibration and portfolio qualification remain open. |

## Log


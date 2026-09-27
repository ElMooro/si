
**Status:** success  
**Duration:** 24.4s  
**Finished:** 2026-09-27T12:11:44+00:00  

## Data

| account_reads | actual_runtime | baseline_sha256 | expected_commit | history_writes | native_invocations | native_publication | notifications_sent | provider_requests | public_writes | schedules_changed | scope | synthetic_fixture_scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': '9el14ir0zwfqw7D5ZXJTqgXRExSU08HdYXmWdK1SnnY=', 'source_files_checked': 6, 'handler_bytes': 94365, 'timeout': 900, 'memory_mb': 1536, 'receipt': {'status': 'matched', 'commit': 'c6823a5e50338a8604b9edf4a92010dac4149606'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-gbc-daily', 'state': 'ENABLED', 'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-global-business-cycle', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 486237160e60f960ebf12a1db05064a5983cef706413608e6b0c6e94dacdaa35 | c6823a5e50338a8604b9edf4a92010dac4149606 | 0 | 0 | {'status': 'complete_native_acquisition_and_calculation_replayed', 'generated_at': '2026-09-27T12:01:23.039500+00:00', 'engine_version': '3.0.5', 'bytes': 253385, 'sha256': '7c2658fcefd3cd7d3da8af5dd850b87de5c46e0fa245a041ba112db18baf0581', 'replay': {'status': 'complete_native_calculations_replayed', 'outputs': 3, 'operations': 73, 'comparison': 'Every parsed field, including recorded processing clocks; object member order is not a data difference.', 'native_provider_requests': 0, 'public_writes': 0, 'point_in_time_verified': False, 'forecast_qualified': False}, 'manifest_sha256': '05dd078d530035a5107453a0835031b46c97c267434239772eb25f073191410c', 'operations': 73, 'provider_attempts': 71} | 0 | 0 | 0 | 0 | Whole acquisition and exact parsed-calculation replay. Stored warehouse inputs remain derived; original upstream Polygon responses, definitions, historical vintages, predictive edge and portfolio qualification remain unverified. | All 34 countries, all three full native output packets and all recorded clocks; separately all 1254 warehouse objects across every fixture metadata page. Synthetic tests are not historical provider proof. |

## Log


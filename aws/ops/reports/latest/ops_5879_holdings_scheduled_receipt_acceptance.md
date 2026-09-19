
**Status:** success  
**Duration:** 65.0s  
**Finished:** 2026-09-19T22:36:31+00:00  

## Data

| acceptance | action | invocation_origin | legacy_collector_invoked | reused_scheduler_evidence | runtime | scheduled_publication |
|---|---|---|---|---|---|---|
|  | holdings_research_collect | AWS EventBridge Scheduler | False |  | {'commit': '42b290f582b4a71bdc9cb7bcb060416efaa412ac', 'code_sha256': 's8nKFUkJ/FrK+GS3ct0noATLDHmOZfgXJGH9m7z5V7k='} |  |
|  |  |  |  | {'run_id': '35472690408', 'request_id': 'holdings-42b290f582b4a71b-35472690408', 'report_sha256': '43de563503b01b9d9601bcd22f1b89754b0e6ddb766c26f6d87b6a10a82d7aef', 'new_collector_invocations': 0} |  |  |
|  |  |  |  |  |  | {'generated_at': '2026-09-19T22:17:19.467543+00:00', 'source_generated_at': '2026-09-19T22:17:16.432529+00:00', 'request_id': 'holdings-42b290f582b4a71b-35472690408', 'replay': {'manifest_key': 'data/holdings-research/runs/e5f89192a9caf1ebc4b7398d4aa7a1e1232068859c0ff6181a24fccc0a6f2ba8.json', 'output_sha256': '8f300b012290ed35c5a957b669c88fdcaaa936635a16e240767b9847433118a5'}} |
| {'contract': 'holdings-native-verification.v1', 'generated_at': '2026-09-19T22:36:31.197493+00:00', 'commit': '42b290f582b4a71bdc9cb7bcb060416efaa412ac', 'code_sha256': 's8nKFUkJ/FrK+GS3ct0noATLDHmOZfgXJGH9m7z5V7k=', 'research_generated_at': '2026-09-19T22:17:19.467543+00:00', 'replay': {'manifest_key': 'data/holdings-research/runs/e5f89192a9caf1ebc4b7398d4aa7a1e1232068859c0ff6181a24fccc0a6f2ba8.json', 'output_sha256': '8f300b012290ed35c5a957b669c88fdcaaa936635a16e240767b9847433118a5'}, 'output_sha256': '8f300b012290ed35c5a957b669c88fdcaaa936635a16e240767b9847433118a5', 'replay_reproduced': True, 'collection_request_id': 'holdings-42b290f582b4a71b-35472690408', 'source_generated_at': '2026-09-19T22:17:16.432529+00:00', 'filings': 37, 'native_rows': 132975, 'comparison_rows': 45641, 'funds': 18, 'current_period_funds': 15, 'protected_legacy_verified': ['data/13f-positions.json', 'data/13f-flows-by-ticker.json', 'data/13f-by-ticker.json', 'data/13f-desk.json', 'data/13f-state/first-seen.json', 'data/13f-price-anchors.json', 'data/13f-cusip-map.json', 'data/capital-flow.json', 'data/capital-flow-history.json'], 'legacy_products_unchanged': True, 'legacy_collector_invoked': False, 'unchanged_quantity_regression_verified': True, 'restatement_regression_verified': True, 'read_action_did_not_recollect': True, 'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0, 'call': None, 'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'additional_independent_votes': 0, 'schedule': {'name': 'justhodl-holdings-originals-research', 'state': 'ENABLED', 'expression': 'cron(40 0/2 * * ? *)', 'timezone': 'UTC'}, 'scheduler_execution_verified': True, 'direct_collector_invokes': 0, 'scheduler_evidence_run': '35472690408', 'new_scheduler_jobs': 0, 'scheduler_report_sha256': '43de563503b01b9d9601bcd22f1b89754b0e6ddb766c26f6d87b6a10a82d7aef', 'remaining': 'Legacy producer and downstream consumer migration are still outstanding.'} |  |  |  |  |  |  |

## Log


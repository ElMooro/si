# ops 6565 - restore insider-cluster-scanner + warroom-weights from backup; schedule the scanner daily; drop one dangling rule

**Status:** success  
**Duration:** 19.3s  
**Finished:** 2026-10-20T21:32:55+00:00  

## Data

| functions_now | justhodl_insider_cluster_scanner | justhodl_insider_cluster_scanner_daily | justhodl_warroom_weights | multi_agent_orchestrator_schedule | s3_manifest |
|---|---|---|---|---|---|
| 904 | created | created | created | deleted | updated |

## Log
- `21:32:41` justhodl-insider-cluster-scanner: created from backups/deleted-functions/20261020T205550Z/justhodl-insider-cluster-scanner.zip (174928 bytes, 40 files), state Active, 1536 MB / 900s, runtime python3.12, env vars 13, layers 1, package == repo source
- `21:32:44` justhodl-warroom-weights: created from backups/deleted-functions/20261020T205550Z/justhodl-warroom-weights.zip (162729 bytes, 40 files), state Active, 1024 MB / 600s, runtime python3.12, env vars 3, layers 0, package == repo source
- `21:32:45` ✅ schedule justhodl-insider-cluster-scanner-daily created: cron(50 13 * * ? *) UTC, one attempt, no retries
- `21:32:45` ✅ rule multi-agent-orchestrator-schedule (cron(29 14 * * ? *), target: deleted multi-agent-orchestrator) backed up and deleted
- `21:32:45` S3 schedule manifest: updated; DIFFERS from (apart from generated_at) the committed repo file
- `21:32:45` justhodl-warroom-weights-monthly: cron(10 7 1 * ? *) ENABLED -> justhodl-warroom-weights
- `21:32:55` ✅ restored; fleet is 904 functions

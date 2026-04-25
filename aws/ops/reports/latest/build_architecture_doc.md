# Build canonical system architecture doc

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-25T00:37:48+00:00  

## Data

| doc_lines | doc_size_kb | lambdas_documented | sources_analyzed |
|---|---|---|---|
| 816 | 31.8 | 95 | 41 |

## Log
- `00:37:47`   Inventory loaded: 95 Lambdas, 5000 S3 keys
## Analyzing Lambda sources from repo

- `00:37:48`   Sources analyzed: 41/95
## Building EB rule → Lambda mapping

- `00:37:48`   EB → Lambda mappings: 68
## Categorizing Lambdas

- `00:37:48`     core_pipeline: 10
- `00:37:48`     data_collectors: 34
- `00:37:48`     intelligence_agents: 9
- `00:37:48`     user_facing: 6
- `00:37:48`     learning_loop: 3
- `00:37:48`     telegram_bot: 1
- `00:37:48`     deprecated_or_unclear: 28
- `00:37:48`     legacy_openbb: 4
## Generating doc

- `00:37:48` ✅   Saved to: aws/ops/audit/system_architecture_2026-04-25.md (816 lines)
- `00:37:48` ✅   Backup to: s3://justhodl-dashboard-live/_audit/system_architecture_2026-04-25.md
- `00:37:48` Done

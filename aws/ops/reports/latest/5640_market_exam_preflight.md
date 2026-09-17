# ops 5640 -- market exam preflight on the runner

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-09-17T19:46:46+00:00  

## Error

```
SystemExit: 1
```

## Data

| control_enabled | endpoint | holdout_drills | holdout_first | manifest_keys | market | model | season_id | season_weights | train_drills | train_first |
|---|---|---|---|---|---|---|---|---|---|---|
| True | jh-owned-coder-async |  |  |  |  | qwen2-5-coder-7b-instruct | None | None |  |  |
|  |  | 0 | None |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 | None |
|  |  |  |  | ['code', 'counts', 'drill_provenance_sha256', 'frozen_at', 'git_sha', 'holdout_blocks', 'label_sessions', 'rule', 'schema_version', 'season_policy_hash', 'symbols', 'train_blocks'] | null |  |  |  |  |  |

## Log
## 1. Inputs

## 2. Drills frozen?

## 3. One probe invoke through the exam's own call

- `19:46:46` ✗ no drills at all -- scripts/factory_holdout.py freeze has never written the market drills

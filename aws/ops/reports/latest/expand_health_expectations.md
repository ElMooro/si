# Expand health expectations to all 95 Lambdas + auto-derived S3 files

**Status:** success  
**Duration:** 37.0s  
**Finished:** 2026-04-25T01:26:34+00:00  

## Data

| auto_derived | hand_curated | new_components | next_step | prev_components |
|---|---|---|---|---|
| 49 | 29 | 78 | step 90 cost audit | 29 |

## Log
- `01:25:57`   Hand-curated entries to preserve: 29
- `01:25:57`   Inventory loaded: 95 Lambdas
- `01:25:57`   Lambdas with enabled schedules: 61
## Auto-deriving Lambda expectations

- `01:25:57`   Auto-derived 41 Lambda entries
- `01:25:57`   Skipped (no schedule): 45
## Auto-deriving S3 file expectations from source code

- `01:25:57`   S3 keys mentioned by put_object across all Lambdas: 37
- `01:25:57`   Auto-derived 8 S3 file entries
- `01:25:57`   Skipped (dynamic/no-ext): 12
- `01:25:57`   Skipped (no scheduled writer): 2
## Merging hand-curated + auto-derived

- `01:25:57`   Total entries: 78
- `01:25:57`   By type: {'s3_file': 20, 'lambda': 50, 'dynamodb': 3, 'ssm': 2, 'eb_rule': 3}
- `01:25:57`   By severity: {'critical': 18, 'important': 24, 'nice_to_have': 36}
- `01:25:57`   By origin: {'hand': 29, 'auto': 49}
## Writing new expectations.py

- `01:25:57` ✅   Archived previous version to expectations.v1.py
- `01:25:57` ✅   Wrote new expectations.py (26,572 bytes)
## Re-deploying health monitor with expanded expectations

- `01:26:01` ✅   Re-deployed: 8819 bytes
- `01:26:01` ✅   Bumped timeout 120s → 300s for the larger checker run
- `01:26:34` ✅   Invoke clean (status 200)
- `01:26:34` 
  System: red
- `01:26:34`   Counts: {'green': 55, 'yellow': 2, 'red': 19, 'info': 2, 'unknown': 0}
- `01:26:34`   Total components: 78
- `01:26:34`   Duration: 26.4s
- `01:26:34` Done

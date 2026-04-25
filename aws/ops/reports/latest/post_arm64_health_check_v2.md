# Post-arm64 health check v2 (now 30+ min post-migration)

**Status:** success  
**Duration:** 6.6s  
**Finished:** 2026-04-25T10:34:31+00:00  

## Data

| arm64_incompat_reverted | clean | not_yet_invoked | pre_existing_errors | total_arm64 |
|---|---|---|---|---|
| 0 | 7 | 74 | 0 | 81 |

## Log
- `10:34:24`   Migration started: 2026-04-25T10:24:00+00:00
- `10:34:24`   Time since: 10 minutes
- `10:34:25`   Total arm64 Lambdas: 81
## Summary

- `10:34:31`   arm64 fleet: 81
- `10:34:31`   Not yet invoked since migration: 74  (no concerns yet)
- `10:34:31`   Invoked clean: 7  ✅
- `10:34:31`   Pre-existing errors (not arm64): 0
- `10:34:31`   arm64 incompatible — REVERTED: 0
- `10:34:31` ✅ 
  ✅ Migration is healthy — 0 arm64 incompatibilities found across 81 Lambdas
- `10:34:31` Done

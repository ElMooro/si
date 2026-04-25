# Verify daily-report-v3 right-size persisted + works at 768MB

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-04-25T02:05:56+00:00  

## Data

| avg_duration_ms | max_memory_used_mb | new_memory_mb | state |
|---|---|---|---|
| 78570 | 125 | 1024 | verified_clean_at_new_memory |

## Log
## 1. Current Lambda configuration

- `02:05:55`   Memory: 768MB
- `02:05:55`   Last modified: 2026-04-25T02:03:59.000+0000
- `02:05:55` ✅   Memory is at target 768MB
## 2. Pull post-change REPORT lines from CloudWatch

- `02:05:55`   Found 5 recent log streams
- `02:05:56`   Post-change REPORT lines: 1
- `02:05:56`   Pre-change REPORT lines (for comparison): 1
## 3. Post-change runs analysis

- `02:05:56`   At 1024MB allocated:
- `02:05:56`     Avg duration: 78570ms (78.6s)
- `02:05:56`     Max memory used: 125MB
- `02:05:56`     Headroom: 88%
- `02:05:56` 
- `02:05:56`   Comparison to pre-change (1024MB):
- `02:05:56`     Avg duration: 75833ms (pre) → 78570ms (post)
- `02:05:56`     Delta: +3.6%
- `02:05:56` ✅   Duration delta acceptable. Right-size SUCCESSFUL.
## 4. Check for errors since change

- `02:05:56`   Last 30 min: 6 invocations, 0 errors
- `02:05:56` Done

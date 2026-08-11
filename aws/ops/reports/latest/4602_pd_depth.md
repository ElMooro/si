# ops 4602 — PD full-history depth

**Status:** failure  
**Duration:** 11.4s  
**Finished:** 2026-08-11T17:37:16+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:37:05` fred guard: scope=full_catalog ver=2.3.0 imported=108897 (untouched)
## 1. Settle v2, run one sync tranche

- `17:37:05`   before: hist_v=None done=1539 status=COMPLETE-maintaining
- `17:37:15`   after: hist_v=2 done=20/1539 status=converging breaks=['APR 2013 TO DEC 2014', 'JAN 1998 TO JUN 2001', 'JAN 2015 TO DEC 2021', 'JAN 2022 TO JUN 2024', 'JUL 2001 TO MAR 2013', 'JUL 2024 AND ON']
- `17:37:15` ✗   [pd] CONTRACT MISS — v2 tranche ran (20 keys deep-pulled)
## 2. Depth proof on a sampled key

- `17:37:16`   PDABTOT: n_obs=696 breaks_used=['<current-only>'] first=2013-04-03 last=2026-07-29 size=4.4KB gz
- `17:37:16` ✗   [depth] CONTRACT MISS — full multi-break history (obs=696, back to 2013-04-03)
- `17:37:16`   projected converged footprint ≈ 7 MB (vs the 5MB stub era)
## verdict

- `17:37:16` ✗ pd depth: 2 red

# ops 4602 — PD full-history depth

**Status:** failure  
**Duration:** 121.4s  
**Finished:** 2026-08-11T17:54:16+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:52:14` fred guard: scope=full_catalog ver=2.3.0 imported=109793 (untouched)
## 1. Settle v2, run one sync tranche

- `17:52:15`   before: hist_v=2 done=40 status=converging
- `17:54:16`   after: hist_v=2 done=60/1539 status=converging breaks=['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024', 'SBP2001', 'SBP2013']
- `17:54:16` ✗   [pd] CONTRACT MISS — v2 tranche ran at full width (60 keys)
- `17:54:16` ✅   [pd] seriesbreaks are keyids: ['SBN2013', 'SBN2015', 'SBN2022', 'SBN2024', 'SBP2001', 'SBP2013']
## 2. Depth proof on a sampled key

- `17:54:16`   PDABTOT: n_obs=696 breaks_used=['<current-only>'] first=2013-04-03 last=2026-07-29 size=4.4KB gz
- `17:54:16` ✅   [depth] history proven (obs=696, first=2013-04-03, breaks=['<current-only>'])
- `17:54:16`   projected converged footprint ≈ 7 MB (vs the 5MB stub era)
## verdict

- `17:54:16` ✗ pd depth: 1 red

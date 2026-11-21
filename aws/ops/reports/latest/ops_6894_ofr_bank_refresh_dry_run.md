# ops 6894 - OFR bank refresh, dry run from the checkout (read-only), 2026-11-21T20:19

**Status:** success  
**Duration:** 15.7s  
**Finished:** 2026-11-21T20:20:11+00:00  

## Data

| gone | hf_would_write | new_observations | revised | seconds | shrank | unchanged | would_write |
|---|---|---|---|---|---|---|---|
| 0 | 2 | 13109 | 12 | 13.1 | 0 | 0 | 442 |

## Log
## 1. The engine's own dry run

- `20:20:08`   dataset fnyr  {"series": 30, "written": 30, "unchanged": 0}
- `20:20:08`   dataset repo  {"series": 164, "written": 164, "unchanged": 0}
- `20:20:08`   dataset nypd  {"series": 194, "written": 194, "unchanged": 0}
- `20:20:08`   dataset mmf   {"series": 42, "written": 42, "unchanged": 0}
- `20:20:08`   dataset tyld  {"series": 12, "written": 12, "unchanged": 0}
- `20:20:08`   dataset ficc  {"series": 2, "written": 2, "unchanged": 0}
- `20:20:08` would write 442, unchanged 0, shrank 0, failed 0 | hedge-fund pair: would write 2 | 13.1 s for 6 calls, 17.6 MB
- `20:20:08` newest observation dates across the source: {"2026-11-19": 72, "2026-11-18": 36, "2026-11-11": 81, "2026-11-09": 2, "2026-11-04": 1, "2026-10-31": 41}
- `20:20:08` ✅ the state file is byte-for-byte what it was
## 2. History: every series it would write, banked file against source answer

- `20:20:11` 442 series: banked observations kept 531285, revised 12, gone 0 | new observations 13109 | 17.5 MB of series documents before gzip
- `20:20:11` series that would lose banked observations: 0
- `20:20:11` series with revised values: 6
    MMF-MMF_RP_T_TOT-M {"kept": 185, "revised": 3, "gone": 0, "new": 4}
    MMF-MMF_RP_T_OO-M {"kept": 185, "revised": 3, "gone": 0, "new": 4}
    MMF-MMF_RP_OA_TOT-M {"kept": 186, "revised": 2, "gone": 0, "new": 4}
    MMF-MMF_RP_OA_OO-M {"kept": 186, "revised": 2, "gone": 0, "new": 4}
    MMF-MMF_RP_AG_TOT-M {"kept": 187, "revised": 1, "gone": 0, "new": 4}
    MMF-MMF_RP_AG_OO-M {"kept": 187, "revised": 1, "gone": 0, "new": 4}
- `20:20:11` series with no observation at the source: 7 ['NYPD-PD_SB_TOT-A', 'NYPD-PD_SL_GE30-A', 'NYPD-PD_SL_TOT-A', 'REPO-GCF_AR_B27-F', 'REPO-GCF_AR_B27-P', 'REPO-GCF_TV_B27-F', 'REPO-GCF_TV_B27-P']
- `20:20:11` ✅ a refresh keeps the banked history (0.0000% of banked observations absent at the source) and adds 13109 observations

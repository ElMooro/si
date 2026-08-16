# ops 4761 -- combined repo master inventory (built from the bank)

**Status:** success  
**Duration:** 25.0s  
**Finished:** 2026-08-16T17:58:35+00:00  

## Data

| REPO-TRIV1_AR_B27-P_first | REPO-TRIV1_AR_LE30-P_last | REPO-TRIV1_TV_B27-P_first | REPO-TRIV1_TV_LE30-P_last | check | names_fnyr | names_nypd | names_repo | value |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 164 |  |
|  |  |  |  |  |  | 194 |  |  |
|  |  |  |  |  | 30 |  |  |  |
|  |  |  |  | series_in_scope |  |  |  | 300 |
|  |  |  |  | entries_built |  |  |  | 300 |
|  |  |  |  | entries_with_banked_depth |  |  |  | 300 |
|  |  | 2025-08-13 | 2026-08-06 |  |  |  |  |  |
| 2025-08-13 | 2026-08-06 |  |  |  |  |  |  |  |
|  |  |  |  | soma_fired |  |  |  | False |

## Log
## Names from OFR metadata (live, per-dataset)

## Depth scan (real spans from the warm bank)

## Tenor-break proof from the bank (doc claims 2025-08-13)

- `17:58:35` ✅ REPO-TRIV1_TV_LE30-P ends 2026-08-06 -> REPO-TRIV1_TV_B27-P begins 2025-08-13 (doc: 2025-08-13)
- `17:58:35` ✅ REPO-TRIV1_AR_LE30-P ends 2026-08-06 -> REPO-TRIV1_AR_B27-P begins 2025-08-13 (doc: 2025-08-13)
## Haircut + metadata file entries

- `17:58:35` ✅ file entry: NY Fed tri-party haircuts (current, post-Nov-2025) (215434 bytes)
- `17:58:35` ✅ file entry: NY Fed tri-party haircuts (May 2010 - Oct 2025) (439002 bytes)
- `17:58:35` ✅ file entry: PD series-break definitions (212 bytes)
- `17:58:35` ✅ inventory written: 300 series + 3 files -> data/repo-master-inventory.json (hot) + warm permanent copy
## soma-cusip fresh check (first tick due 18:41 UTC)

- `17:58:35` still pre-first-tick at op runtime

# ops 4772 -- PD splice v2: value-brute joins + splice

**Status:** success  
**Duration:** 73.8s  
**Finished:** 2026-08-16T19:54:21+00:00  

## Data

| check | value |
|---|---|
| keyids_total | 1116 |
| fails_kids | 24 |
| financing_kids | 558 |
| board_nypd_rows | 106 |
| verified_mappings | 3 |
| scale_factors | {"1.0": 3} |
| unverified | 103 |
| spliced_docs_banked | 3 |
| rows_with_deeper_floor | 0 |

## Log
## brute-force value joins

- `19:54:07`   ✓ NYPD-PD_RRP_EQT_GE30-A <-> PDSIOSB-ABSTAG30 f=1.0 (24/24)
- `19:54:07`   ✓ NYPD-PD_RRP_EQT_L30-A <-> PDSIOSB-ABSTAL30 f=1.0 (24/24)
- `19:54:07`   ✓ NYPD-PD_RRP_EQT_OO-A <-> PDSIOSB-OTAL30 f=1.0 (22/24)
- `19:54:07`   unverified: NYPD-PD_AFtD_AG-A
- `19:54:07`   unverified: NYPD-PD_AFtD_AG_MBS-A
- `19:54:07`   unverified: NYPD-PD_AFtD_AG_eMBS-A
- `19:54:07`   unverified: NYPD-PD_AFtD_CORS-A
- `19:54:07`   unverified: NYPD-PD_AFtD_OMBS-A
- `19:54:07`   unverified: NYPD-PD_AFtD_T-A
- `19:54:07`   unverified: NYPD-PD_AFtD_TIPS-A
- `19:54:07`   unverified: NYPD-PD_AFtD_TOT-A
- `19:54:07`   unverified: NYPD-PD_AFtD_T_eTIPS-A
- `19:54:07`   unverified: NYPD-PD_AFtR_AG-A
## older-break fetch + permanent splice bank


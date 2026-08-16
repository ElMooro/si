# ops 4779 -- splice v4 from the banked master dump

**Status:** success  
**Duration:** 31.0s  
**Finished:** 2026-08-16T20:44:18+00:00  

## Data

| check | value |
|---|---|
| distinct_kids | 2288 |
| kids_before_2005 | 199 |
| kids_before_2013 | 207 |
| financing_new | 45 |
| total_mapped | 82 |
| floors_pre2005 | 0 |

## Log
## A. dump census (correct columns)

- `20:43:49`   PDFTD-UST: 2013-04-03 -> 2026-08-05 n=697
- `20:43:49`   PDFTD-USTET: 2013-04-03 -> 2026-08-05 n=697
- `20:43:49`   PDFTD-FGM: 2013-04-03 -> 2026-08-05 n=697
- `20:43:49`   PDFTD-FGEM: 2013-04-03 -> 2026-08-05 n=697
- `20:43:49`   PDFTD-CS: 2013-04-03 -> 2026-08-05 n=697
- `20:43:49`   PDFTD-OM: 2013-04-03 -> 2026-08-05 n=697
## B. rebuild mapped docs from the dump

## C. financing expansion (dump brute)

- `20:44:18` ✅   NYPD-PD_AFtD_TIPS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_T_eTIPS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_AG_MBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_AG_eMBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_CORS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_OMBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_T-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_AG-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtD_TOT-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_TIPS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_T_eTIPS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_AG_MBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_AG_eMBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_CORS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_OMBS-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_T-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_AG-A: floor 2013-04-03
- `20:44:18` ✅   NYPD-PD_AFtR_TOT-A: floor 2013-04-03

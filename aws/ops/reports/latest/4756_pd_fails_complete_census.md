# ops 4756 -- the complete PD fails census (OFR mirror + NY Fed full)

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-08-16T16:55:28+00:00  

## Data

| check | earliest | keyid | latest | n | value |
|---|---|---|---|---|---|
| ofr_nypd_total |  |  |  |  | 194 |
| ofr_fails_broad_scan |  |  |  |  | 18 |
| ofr_fails_FtD |  |  |  |  | 9 |
| ofr_fails_FtR |  |  |  |  | 9 |
| nyfed_pd_catalog |  |  |  |  | 1539 |
| nyfed_pd_done |  |  |  |  | 1539 |
| nyfed_fails_keyids |  |  |  |  | 64 |
| fails_held_on_s3 |  |  |  |  | 64 |
| fails_missing_on_s3 |  |  |  |  | 0 |
|  | 2013-04-03 | PDFTD-CS | 2026-08-14 | 698 |  |

## Log
## A. OFR NYPD mirror -- is 18 the whole fails subset?

- `16:55:26` OFR fails grid: NYPD-PD_AFtD_AG-A, NYPD-PD_AFtD_AG_MBS-A, NYPD-PD_AFtD_AG_eMBS-A, NYPD-PD_AFtD_CORS-A, NYPD-PD_AFtD_OMBS-A, NYPD-PD_AFtD_T-A, NYPD-PD_AFtD_TIPS-A, NYPD-PD_AFtD_TOT-A, NYPD-PD_AFtD_T_eTIPS-A, NYPD-PD_AFtR_AG-A, NYPD-PD_AFtR_AG_MBS-A, NYPD-PD_AFtR_AG_eMBS-A, NYPD-PD_AFtR_CORS-A, NYPD-PD_AFtR_OMBS-A, NYPD-PD_AFtR_T-A, NYPD-PD_AFtR_TIPS-A, NYPD-PD_AFtR_TOT-A, NYPD-PD_AFtR_T_eTIPS-A
## B. NY Fed full PD catalog -- the real fails universe

- `16:55:26`   PDFTD-CS
- `16:55:26`   PDFTD-CSC
- `16:55:26`   PDFTD-FGEM
- `16:55:26`   PDFTD-FGEMC
- `16:55:26`   PDFTD-FGM
- `16:55:26`   PDFTD-FGMC
- `16:55:26`   PDFTD-OM
- `16:55:26`   PDFTD-OMC
- `16:55:26`   PDFTD-UST
- `16:55:26`   PDFTD-USTC
- `16:55:26`   PDFTD-USTET
- `16:55:26`   PDFTD-USTETC
- `16:55:26`   PDFTR-CS
- `16:55:26`   PDFTR-CSC
- `16:55:26`   PDFTR-FGEM
- `16:55:26`   PDFTR-FGEMC
- `16:55:26`   PDFTR-FGM
- `16:55:26`   PDFTR-FGMC
- `16:55:26`   PDFTR-OM
- `16:55:26`   PDFTR-OMC
- `16:55:26`   PDFTR-UST
- `16:55:26`   PDFTR-USTC
- `16:55:26`   PDFTR-USTET
- `16:55:26`   PDFTR-USTETC
- `16:55:26`   PDSIOSB-UTSETTA30
- `16:55:26`   PDSIOSB-UTSETTAG30
- `16:55:26`   PDSIOSB-UTSETTOT
- `16:55:26`   PDSIRRA-CBGUTSETTAG30
- `16:55:26`   PDSIRRA-CBGUTSETTAL30
- `16:55:26`   PDSIRRA-CBSPUTSETTAG30
- `16:55:26`   PDSIRRA-CBSPUTSETTAL30
- `16:55:26`   PDSIRRA-CBSUTSETTAG30
- `16:55:26`   PDSIRRA-CBSUTSETTAL30
- `16:55:26`   PDSIRRA-GCFUTSETTAG30
- `16:55:26`   PDSIRRA-GCFUTSETTAL30
- `16:55:26`   PDSIRRA-TRIGUTSETTAG30
- `16:55:26`   PDSIRRA-TRIGUTSETTAL30
- `16:55:26`   PDSIRRA-TRISPUTSETTAG30
- `16:55:26`   PDSIRRA-TRISPUTSETTAL30
- `16:55:26`   PDSIRRA-UBGUTSETTAG30
- `16:55:26`   PDSIRRA-UBGUTSETTAL30
- `16:55:26`   PDSIRRA-UBSUTSETTAG30
- `16:55:26`   PDSIRRA-UBSUTSETTAL30
- `16:55:26`   PDSIRRA-UTSETTOT
- `16:55:26`   PDSOOS-UTSETTAG30
- `16:55:26`   PDSOOS-UTSETTAL30
- `16:55:26`   PDSOOS-UTSETTOT
- `16:55:26`   PDSORA-CBGUTSETTAG30
- `16:55:26`   PDSORA-CBGUTSETTAL30
- `16:55:26`   PDSORA-CBSPUTSETTAG30
- `16:55:26`   PDSORA-CBSPUTSETTAL30
- `16:55:26`   PDSORA-CBSUTSETTAG30
- `16:55:26`   PDSORA-CBSUTSETTAL30
- `16:55:26`   PDSORA-GCFUTSETTAG30
- `16:55:26`   PDSORA-GCFUTSETTAL30
- `16:55:26`   PDSORA-TRIGUTSETTAG30
- `16:55:26`   PDSORA-TRIGUTSETTAL30
- `16:55:26`   PDSORA-TRISPUTSETTAG30
- `16:55:26`   PDSORA-TRISPUTSETTAL30
- `16:55:26`   PDSORA-UBGUTSETTAG30
- `16:55:26`   PDSORA-UBGUTSETTAL30
- `16:55:26`   PDSORA-UBSUTSETTAG30
- `16:55:26`   PDSORA-UBSUTSETTAL30
- `16:55:26`   PDSORA-UTSETTOT
## C. Bank-verify every NY Fed fails keyid on S3

## D. Depth proof -- current, legacy, MBS-class

- `16:55:28` ✅ PDFTD-CS: 698 obs, 2013-04-03 -> 2026-08-14
## E. Bank stragglers via the engine's own path

- `16:55:28` ✅ no stragglers -- every fails series in the full NY Fed catalog is already banked; nothing to write

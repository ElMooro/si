# ops 4774 -- fails join diagnostic (raw values)

**Status:** success  
**Duration:** 4.1s  
**Finished:** 2026-08-16T19:57:07+00:00  

## Data

| check | value |
|---|---|
| pdft_kids_in_meta | 24 |
| ofr_fails_rows | 18 |

## Log
- `19:57:03` kids: PDFTD-CS, PDFTD-CSC, PDFTD-FGEM, PDFTD-FGEMC, PDFTD-FGM, PDFTD-FGMC, PDFTD-OM, PDFTD-OMC, PDFTD-UST, PDFTD-USTC, PDFTD-USTET, PDFTD-USTETC, PDFTR-CS, PDFTR-CSC, PDFTR-FGEM, PDFTR-FGEMC, PDFTR-FGM, PDFTR-FGMC, PDFTR-OM, PDFTR-OMC, PDFTR-UST, PDFTR-USTC, PDFTR-USTET, PDFTR-USTETC
## one raw bank row (field truth)

- `19:57:03` PDFTD-CS doc[:500]: {"series": "PDFTD-CS", "as_of": "2026-08-14T17:33:14+00:00", "hist_v": 3, "breaks_used": ["SBN2013", "SBN2015", "SBN2022", "SBN2024"], "n_obs": 697, "first": "2013-04-03", "last": "2026-08-05", "rows": [{"asofdate": "2013-04-03", "keyid": "PDFTD-CS", "value": "14232"}, {"asofdate": "2013-04-10", "keyid": "PDFTD-CS", "value": "11095"}, {"asofdate": "2013-04-17", "keyid": "PDFTD-CS", "value": "13889"}, {"asofdate": "2013-04-24", "keyid": "PDFTD-CS", "value": "11294"}, {"asofdate": "2013-05-01", "k
- `19:57:05`   PDFTD-CS: n=697 last=2026-08-05
- `19:57:05`   PDFTD-CSC: n=694 last=2026-08-05
- `19:57:05`   PDFTD-FGEM: n=697 last=2026-08-05
- `19:57:05`   PDFTD-FGEMC: n=694 last=2026-08-05
- `19:57:05`   PDFTD-FGM: n=697 last=2026-08-05
- `19:57:05`   PDFTD-FGMC: n=694 last=2026-08-05
- `19:57:05`   PDFTD-OM: n=697 last=2026-08-05
- `19:57:05`   PDFTD-OMC: n=693 last=2026-08-05
- `19:57:05`   PDFTD-UST: n=697 last=2026-08-05
- `19:57:05`   PDFTD-USTC: n=693 last=2026-08-05
- `19:57:05`   PDFTD-USTET: n=697 last=2026-08-05
- `19:57:05`   PDFTD-USTETC: n=693 last=2026-08-05
- `19:57:05`   PDFTR-CS: n=697 last=2026-08-05
- `19:57:05`   PDFTR-CSC: n=694 last=2026-08-05
- `19:57:05`   PDFTR-FGEM: n=697 last=2026-08-05
- `19:57:05`   PDFTR-FGEMC: n=694 last=2026-08-05
- `19:57:05`   PDFTR-FGM: n=697 last=2026-08-05
- `19:57:05`   PDFTR-FGMC: n=694 last=2026-08-05
- `19:57:05`   PDFTR-OM: n=697 last=2026-08-05
- `19:57:05`   PDFTR-OMC: n=693 last=2026-08-05
- `19:57:05`   PDFTR-UST: n=697 last=2026-08-05
- `19:57:05`   PDFTR-USTC: n=693 last=2026-08-05
- `19:57:05`   PDFTR-USTET: n=697 last=2026-08-05
- `19:57:05`   PDFTR-USTETC: n=693 last=2026-08-05
- `19:57:07` ofr fails ids: NYPD-PD_AFtD_AG-A, NYPD-PD_AFtD_AG_MBS-A, NYPD-PD_AFtD_AG_eMBS-A, NYPD-PD_AFtD_CORS-A, NYPD-PD_AFtD_OMBS-A, NYPD-PD_AFtD_T-A, NYPD-PD_AFtD_TIPS-A, NYPD-PD_AFtD_TOT-A, NYPD-PD_AFtD_T_eTIPS-A, NYPD-PD_AFtR_AG-A, NYPD-PD_AFtR_AG_MBS-A, NYPD-PD_AFtR_AG_eMBS-A, NYPD-PD_AFtR_CORS-A, NYPD-PD_AFtR_OMBS-A, NYPD-PD_AFtR_T-A, NYPD-PD_AFtR_TIPS-A, NYPD-PD_AFtR_TOT-A, NYPD-PD_AFtR_T_eTIPS-A
## hypotheses, raw side-by-side

- `19:57:07` NYPD-PD_AFtD_TIPS-A vs PDFTD-UST: common_dates=603
- `19:57:07`     2026-06-17: ofr=6107000000.0  bank=6107.0
- `19:57:07`     2026-06-24: ofr=8392000000.0  bank=8392.0
- `19:57:07`     2026-07-01: ofr=9036000000.0  bank=9036.0
- `19:57:07`     2026-07-08: ofr=7969000000.0  bank=7969.0
- `19:57:07`     2026-07-15: ofr=9038000000.0  bank=9038.0
- `19:57:07`     2026-07-22: ofr=8852000000.0  bank=8852.0
- `19:57:07` NYPD-PD_AFtD_T_eTIPS-A vs PDFTD-USTET: common_dates=603
- `19:57:07`     2026-06-17: ofr=95132000000.0  bank=95132.0
- `19:57:07`     2026-06-24: ofr=83497000000.0  bank=83497.0
- `19:57:07`     2026-07-01: ofr=96157000000.0  bank=96157.0
- `19:57:07`     2026-07-08: ofr=101129000000.0  bank=101129.0
- `19:57:07`     2026-07-15: ofr=101788000000.0  bank=101788.0
- `19:57:07`     2026-07-22: ofr=92303000000.0  bank=92303.0
- `19:57:07` NYPD-PD_AFtD_AG_eMBS-A vs PDFTD-FGEM: common_dates=603
- `19:57:07`     2026-06-17: ofr=1634000000.0  bank=1634.0
- `19:57:07`     2026-06-24: ofr=1145000000.0  bank=1145.0
- `19:57:07`     2026-07-01: ofr=818000000.0  bank=818.0
- `19:57:07`     2026-07-08: ofr=844000000.0  bank=844.0
- `19:57:07`     2026-07-15: ofr=1135000000.0  bank=1135.0
- `19:57:07`     2026-07-22: ofr=291000000.0  bank=291.0
- `19:57:07` TOT vs sum(12 kids): common=600
- `19:57:07`     2026-06-17: ofr_TOT=156791000000.0  sum=152676.0
- `19:57:07`     2026-06-24: ofr_TOT=153015000000.0  sum=149239.0
- `19:57:07`     2026-07-01: ofr_TOT=165509000000.0  sum=178003.0
- `19:57:07`     2026-07-08: ofr_TOT=165251000000.0  sum=164993.0
- `19:57:07`     2026-07-15: ofr_TOT=176147000000.0  sum=187043.0
- `19:57:07`     2026-07-22: ofr_TOT=156636000000.0  sum=137125.0

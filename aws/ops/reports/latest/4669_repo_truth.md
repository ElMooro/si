# ops 4669 — OFR depth (real mnemonics) · 1998 question · tri-party hunt

**Status:** failure  
**Duration:** 15.4s  
**Finished:** 2026-08-14T21:39:48+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. OUR OFR catalog — real names + measured depth

- `21:39:33`   NYPD (194): ['NYPD-PD_AFtD_AG-A', 'NYPD-PD_AFtD_AG_MBS-A', 'NYPD-PD_AFtD_AG_eMBS-A', 'NYPD-PD_AFtD_CORS-A', 'NYPD-PD_AFtD_OMBS-A', 'NYPD-PD_AFtD_T-A']
- `21:39:33`   REPO (164): ['REPO-DVP_AR_B27-F', 'REPO-DVP_AR_B27-P', 'REPO-DVP_AR_B830-F', 'REPO-DVP_AR_B830-P', 'REPO-DVP_AR_G30-F', 'REPO-DVP_AR_G30-P']
- `21:39:33`   MMF (42): ['MMF-MMF_AG_TOT-M', 'MMF-MMF_BRA_TOT-M', 'MMF-MMF_OA_TOT-M', 'MMF-MMF_RP_AG_G30-M', 'MMF-MMF_RP_AG_LE30-M', 'MMF-MMF_RP_AG_OO-M']
- `21:39:33`   FNYR (30): ['FNYR-BGCR-A', 'FNYR-BGCR_1Pctl-A', 'FNYR-BGCR_25Pctl-A', 'FNYR-BGCR_75Pctl-A', 'FNYR-BGCR_99Pctl-A', 'FNYR-BGCR_UV-A']
- `21:39:33`   TYLD (12): ['TYLD-TCMR-10Yr-A', 'TYLD-TCMR-1Mo-A', 'TYLD-TCMR-1Yr-A', 'TYLD-TCMR-20Yr-A', 'TYLD-TCMR-2Mo-A', 'TYLD-TCMR-2Yr-A']
- `21:39:33`   REPO-DVP_AR_B27-F            no dated rows; head={'REPO-DVP_AR_B27-F': {'timeseries': {'aggregation': [['2025-08-14', 4.42], ['2025-08-15', 4.4], ['2025-08-18'
- `21:39:33`   REPO-DVP_AR_B27-P            no dated rows; head={'REPO-DVP_AR_B27-P': {'timeseries': {'aggregation': [['2025-08-14', 4.42], ['2025-08-15', 4.4], ['2025-08-18'
- `21:39:33`   REPO-DVP_AR_B830-F           no dated rows; head={'REPO-DVP_AR_B830-F': {'timeseries': {'aggregation': [['2025-08-14', 4.42], ['2025-08-15', 4.42], ['2025-08-1
- `21:39:33`   FNYR-BGCR-A                  no dated rows; head={'FNYR-BGCR-A': {'timeseries': {'aggregation': [['2018-04-02', 1.77], ['2018-04-03', 1.81], ['2018-04-04', 1.7
- `21:39:33`   FNYR-BGCR_1Pctl-A            no dated rows; head={'FNYR-BGCR_1Pctl-A': {'timeseries': {'aggregation': [['2018-04-02', 1.15], ['2018-04-03', 1.5], ['2018-04-04'
- `21:39:33`   FNYR-BGCR_25Pctl-A           no dated rows; head={'FNYR-BGCR_25Pctl-A': {'timeseries': {'aggregation': [['2018-04-02', 1.76], ['2018-04-03', 1.8], ['2018-04-04
- `21:39:33`   MMF-MMF_AG_TOT-M             no dated rows; head={'MMF-MMF_AG_TOT-M': {'timeseries': {'aggregation': [['2010-11-30', 434141816591.42], ['2010-12-31', 454103560
- `21:39:33`   MMF-MMF_BRA_TOT-M            no dated rows; head={'MMF-MMF_BRA_TOT-M': {'timeseries': {'aggregation': [['2010-11-30', 839880384355.29], ['2010-12-31', 85905048
- `21:39:33`   MMF-MMF_OA_TOT-M             no dated rows; head={'MMF-MMF_OA_TOT-M': {'timeseries': {'aggregation': [['2010-11-30', 787373136130.21], ['2010-12-31', 817202185
- `21:39:34`   NYPD-PD_AFtD_AG-A            no dated rows; head={'NYPD-PD_AFtD_AG-A': {'timeseries': {'disclosure_edits': [], 'aggregation': [['2015-01-07', 20612000000.0], [
- `21:39:34`   NYPD-PD_AFtD_AG_MBS-A        no dated rows; head={'NYPD-PD_AFtD_AG_MBS-A': {'timeseries': {'disclosure_edits': [], 'aggregation': [['2015-01-07', 14937000000.0
- `21:39:34`   NYPD-PD_AFtD_AG_eMBS-A       no dated rows; head={'NYPD-PD_AFtD_AG_eMBS-A': {'timeseries': {'disclosure_edits': [], 'aggregation': [['2015-01-07', 5675000000.0
- `21:39:34`   TYLD-TCMR-10Yr-A             no dated rows; head={'TYLD-TCMR-10Yr-A': {'timeseries': {'aggregation': [['1990-01-02', 7.94], ['1990-01-03', 7.99], ['1990-01-04'
- `21:39:34`   TYLD-TCMR-1Mo-A              no dated rows; head={'TYLD-TCMR-1Mo-A': {'timeseries': {'aggregation': [['2001-07-31', 3.67], ['2001-08-01', 3.65], ['2001-08-02',
- `21:39:34`   TYLD-TCMR-1Yr-A              no dated rows; head={'TYLD-TCMR-1Yr-A': {'timeseries': {'aggregation': [['1990-01-02', 7.81], ['1990-01-03', 7.85], ['1990-01-04',
- `21:39:34`   depth summary: 0 sampled, 0 reach pre-2015 (earliest None)
- `21:39:34` ✗   [ofr] could not measure ANY banked depth — payload shape unknown, importer would be blind
## 2. Build probe list — 1998 question + tri-party

- `21:39:34`   probing 1998 break SBP2001 with: ['PDPOSGS-B', 'PDFTD-CS', 'PDFTR-CS', 'PDSORA-ABSC', 'PDTRGST-TOT', 'PDABTOT', 'PDCSTOT']
- `21:39:48`   (temp probe deleted)
## 3. The 1998 verdict

- `21:39:48`   SBP2001/PDPOSGS-B -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDPOSGS-B -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDFTD-CS -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDFTD-CS -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDFTR-CS -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDFTR-CS -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDSORA-ABSC -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDSORA-ABSC -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDTRGST-TOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDTRGST-TOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDABTOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDABTOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2001/PDCSTOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   SBP2013/PDCSTOT -> EMPTY {"pd": { "timeseries": [ ] } }
- `21:39:48`   [1998] SBP2001 empty across every probed family — the 1998-2001 break exists in the index but serves no timeseries via this API path. PD floor stays 2013 unless the bulk CSV route carries it.
## 4. Tri-party (#4 — haircuts)

- `21:39:48` ✅   tri-party xlsx (current) -> 95175 bytes ct=text/html; charset=utf-8 | <!DOCTYPE html> <html lang="en"> <head>      <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1"> <meta http-equiv="Content-Type" conte
- `21:39:48` ✅   tri-party stats page -> 112605 bytes ct=text/html; charset=utf-8 | <!DOCTYPE html> <html lang="en"> <head>       <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1"> <meta http-equiv="Content-Type" con
- `21:39:48`   GCF/tri-party API root -> ERR HTTP Error 400: Bad Request
- `21:39:48`   marketshare tri-party -> ERR HTTP Error 400: Bad Request
- `21:39:48` ✅   OFR tri-party via series/full -> 66467 bytes ct=application/json | {"REPO-TRI_AR_OO-P": {"timeseries": {"aggregation": [["2014-08-22", 0.08], ["2014-08-25", 0.08], ["2014-08-26", 0.08], ["2014-08-27", 0.07], ["2014-08
- `21:39:48` ✅   [tri-party] 3 live endpoint(s) — importer can target these
- `21:39:48`   manifest -> data/_state/repo-truth-manifest.json
## verdict

- `21:39:48` ✗ repo truth: 1 red

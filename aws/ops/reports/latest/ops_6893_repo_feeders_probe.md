# ops 6893 - the repo board's feeders: bank against source (read-only), 2026-11-22T15:52+00:00

**Status:** success  
**Duration:** 19.5s  
**Finished:** 2026-11-22T15:52:28+00:00  

## Data

| bank_behind | bank_current | ofr_files | ofr_newest_write | source_not_on_board | source_series |
|---|---|---|---|---|---|
| 0 | 435 | 442 | 2026-11-21 | 142 | 442 |

## Log
## 1. The OFR bank

- `15:52:09` state written 2026-11-22 02:29 | status COMPLETE-maintaining | as_of 2026-11-22T02:29:38+00:00 | catalog 442 | done 442 | failures 0 | catalog source https://data.financialresearch.gov/v1/metadata/mnemonics | new last run []
- `15:52:09` series files: 442, 3.8 MB | by day of last write: 2026-11-21 442
## 2. The OFR API today

- `15:52:09` /metadata/mnemonics: 200, 10045 bytes, 0.2 s -> 442 mnemonics | not in the bank's catalog: []
- `15:52:09` /series/full?mnemonic=FNYR-SOFR-A: 200, 48852 bytes, 0.2 s | top keys ['FNYR-SOFR-A'] | inner keys ['timeseries', 'metadata'] | newest ('2026-11-19', 2157, 3.91)
- `15:52:10` /series/dataset?dataset=fnyr: 200, 1.7 MB, 1.2 s | top keys ['short_name', 'long_name', 'timeseries'] | 30 series | sample FNYR-BGCR-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 30
- `15:52:16` /series/dataset?dataset=repo: 200, 9.6 MB, 6.0 s | top keys ['short_name', 'long_name', 'timeseries'] | 164 series | sample REPO-DVP_AR_G30-P keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 30, 2026-11-18 36, 2026-11-09 2, 2026-06-30 65, 2026-06-24 1
- `15:52:18` /series/dataset?dataset=nypd: 200, 3.7 MB, 2.0 s | top keys ['short_name', 'long_name', 'timeseries'] | 194 series | sample NYPD-PD_AFtD_AG-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-11 81, 2026-11-04 1, 2026-10-21 2, 2026-10-14 2, 2026-10-07 4
- `15:52:19` /series/dataset?dataset=mmf: 200, 0.2 MB, 0.3 s | top keys ['short_name', 'long_name', 'timeseries'] | 42 series | sample MMF-MMF_AG_TOT-M keys ['timeseries', 'metadata'] | newest dates: 2026-10-31 41, 2026-09-30 1
- `15:52:21` /series/dataset?dataset=tyld: 200, 2.2 MB, 1.9 s | top keys ['short_name', 'long_name', 'timeseries'] | 12 series | sample TYLD-TCMR-10Yr-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 12
## 3. Bank against source, per mnemonic

- `15:52:22` source series 442 | bank current 435 | bank behind 0 ({}) | not banked 7 ['NYPD-PD_SB_TOT-A', 'NYPD-PD_SL_GE30-A', 'NYPD-PD_SL_TOT-A', 'REPO-GCF_AR_B27-F', 'REPO-GCF_AR_B27-P', 'REPO-GCF_TV_B27-F', 'REPO-GCF_TV_B27-P']
- `15:52:22` primary-dealer mnemonics at the source: 82 still printing (NYPD-PD_AFtD_AG-A, NYPD-PD_AFtD_AG_MBS-A, NYPD-PD_AFtD_AG_eMBS-A, NYPD-PD_AFtD_CORS-A, NYPD-PD_AFtD_OMBS-A, NYPD-PD_AFtD_T-A, NYPD-PD_AFtD_TIPS-A, NYPD-PD_AFtD_TOT-A, NYPD-PD_AFtD_T_eTIPS-A, NYPD-PD_AFtR_AG-A ...), 91 ended before mid-2022
- `15:52:22` board inventory: 300 series | source series the inventory does not carry: 142 (MMF-MMF_AG_TOT-M, MMF-MMF_BRA_TOT-M, MMF-MMF_OA_TOT-M, MMF-MMF_RP_AG_G30-M, MMF-MMF_RP_AG_LE30-M, MMF-MMF_RP_AG_OO-M, MMF-MMF_RP_AG_TOT-M, MMF-MMF_RP_G30-M, MMF-MMF_RP_LE30-M, MMF-MMF_RP_OA_G30-M, MMF-MMF_RP_OA_LE30-M, MMF-MMF_RP_OA_OO-M, MMF-MMF_RP_OA_TOT-M, MMF-MMF_RP_OO-M ...)
- `15:52:22`   of those, printing in the last 30 days: MMF-MMF_AG_TOT-M, MMF-MMF_BRA_TOT-M, MMF-MMF_OA_TOT-M, MMF-MMF_RP_AG_G30-M, MMF-MMF_RP_AG_LE30-M, MMF-MMF_RP_AG_OO-M, MMF-MMF_RP_AG_TOT-M, MMF-MMF_RP_G30-M, MMF-MMF_RP_LE30-M, MMF-MMF_RP_OA_G30-M, MMF-MMF_RP_OA_LE30-M, MMF-MMF_RP_OA_OO-M, MMF-MMF_RP_OA_TOT-M, MMF-MMF_RP_OO-M, MMF-MMF_RP_TOT-M, MMF-MMF_RP_T_G30-M, MMF-MMF_RP_T_LE30-M, MMF-MMF_RP_T_OO-M, MMF-MMF_RP_T_TOT-M, MMF-MMF_RP_wDFI-M, MMF-MMF_RP_wFFI-M, MMF-MMF_RP_wFICC-M, MMF-MMF_RP_wOCP-M, MMF-MMF_TOT-M, MMF-MMF_T_TOT-M, MMF-MY_MMF_RP_AG_G30-M, MMF-MY_MMF_RP_AG_LE30-M, MMF-MY_MMF_RP_AG_OO-M, MMF-MY_MMF_RP_AG_TOT-M, MMF-MY_MMF_RP_G30-M, MMF-MY_MMF_RP_LE30-M, MMF-MY_MMF_RP_OA_G30-M, MMF-MY_MMF_RP_OA_LE30-M, MMF-MY_MMF_RP_OA_OO-M, MMF-MY_MMF_RP_OA_TOT-M, MMF-MY_MMF_RP_OO-M, MMF-MY_MMF_RP_TOT-M, MMF-MY_MMF_RP_T_G30-M, MMF-MY_MMF_RP_T_LE30-M, MMF-MY_MMF_RP_T_OO-M
## 4. Hedge-fund catalog: sponsored repo

- `15:52:22` state written 2026-11-22 02:30 | status COMPLETE-maintaining | catalog 497 | refresh_cursor 273
- `15:52:22`   FICC-SPONSORED_REPO_VOL      bank newest 2026-10-13 (file written 2026-11-21) | source 200 newest 2026-10-13 (1640 obs, 0.2 s)
- `15:52:22`   FICC-SPONSORED_REVREPO_VOL   bank newest 2026-10-13 (file written 2026-11-21) | source 200 newest 2026-10-13 (1640 obs, 0.1 s)
- `15:52:22`   /hf/v1/series/dataset?dataset=ficc: 200, 111823 bytes, 0.2 s, 2 series ['FICC-SPONSORED_REPO_VOL', 'FICC-SPONSORED_REVREPO_VOL']
## 5. The engine's own runs, 14 days

- `15:52:23` justhodl-ofr-stfm            runs/day 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 11-21:3 | errors 0 | mean duration 6.8 s
- `15:52:24` justhodl-repo                runs/day 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 11-21:3 | errors 0 | mean duration 576.5 s
- `15:52:24` justhodl-repo-market         runs/day 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 11-21:1 | errors 0 | mean duration 10.4 s
- `15:52:25` justhodl-nyfed-markets-full  runs/day 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 11-21:1 | errors 0 | mean duration 12.2 s
- `15:52:26` justhodl-nyfed-pd            runs/day 11-12:1 11-13:1 11-19:1 11-20:1 11-21:1 | errors 0 | mean duration 6.8 s
## 6. The other feeders

- `15:52:26` haircut index written 2026-11-22 | 81 series | keys ['built_at', 'refreshed_at', 'source']
- `15:52:26`   www.newyorkfed.org/medialibrary/media/banking/pdf/tpr_stats_historical.xlsx -> 200, 101161 bytes, 0.4 s, last-modified None | 
- `15:52:26`   www.newyorkfed.org/medialibrary/media/research/banking_research/tri-party-repo/tpr-statistics-hi -> 200, 101161 bytes, 0.2 s, last-modified None | 
- `15:52:27`   www.dtcc.com/charts/daily-total-us-treasury-trade-fails.json -> 404, 0 bytes, 0.5 s, last-modified None | 
- `15:52:27`   www.dtcc.com/-/media/Files/Downloads/Charts/daily-treasury-fails.csv -> 404, 0 bytes, 0.5 s, last-modified None | 
- `15:52:28`   markets.newyorkfed.org/api/pd/list/timeseries.json -> 200, 335616 bytes, 0.2 s, last-modified None | {"pd": { "timeseries": [ { "seriesbreak": "SBN2024", "keyid": "PDPOSMBS-TOT", "description": "Total - Mortgage
- `15:52:28`   markets.newyorkfed.org/api/pd/get/PDSORA-UTSETTOT.json -> 200, 55567 bytes, 0.1 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2013-04-03", "keyid": "PDSORA-UTSETTOT", "value": "1535302" } , { "aso
- `15:52:28`   markets.newyorkfed.org/api/pd/latest/SBN2024.json -> 200, 114798 bytes, 0.1 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2026-11-11", "keyid": "PDWOTIPSC", "value": "-9341" }, { "asofdate": "
- `15:52:28` data/settlement-fails.json written 2026-11-21 21:30 | {"allocation_pct": null, "as_of": "2026-11-11", "call": null, "calls_eligible": false, "classes": "{6}", "contract": "fr2004-fails-research.v1", "decision_id": "fails-e057189a63bd8e4264872401c6dcde457211f65d5e4a5e03d41f5909190177c2", "engine": "settlement-fails", "execution_eligible": false, "generated_at": "2026-11-21T21:30:39.834735+00:00", "headline": "{48}", "legacy_context": "{6}", "limitatio
- `15:52:28` ✅ read-only; nothing changed

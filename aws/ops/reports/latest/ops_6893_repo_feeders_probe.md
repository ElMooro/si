# ops 6893 - the repo board's feeders: bank against source (read-only), 2026-11-21T20:14+00:00

**Status:** success  
**Duration:** 21.0s  
**Finished:** 2026-11-21T20:14:35+00:00  

## Data

| bank_behind | bank_current | ofr_files | ofr_newest_write | source_not_on_board | source_series |
|---|---|---|---|---|---|
| 312 | 123 | 442 | 2026-08-06 | 142 | 442 |

## Log
## 1. The OFR bank

- `20:14:14` state written 2026-11-21 02:29 | status COMPLETE-maintaining | as_of 2026-11-21T02:29:38+00:00 | catalog 442 | done 442 | failures 0 | catalog source https://data.financialresearch.gov/v1/metadata/mnemonics | new last run []
- `20:14:15` series files: 442, 3.8 MB | by day of last write: 2026-08-06 442
## 2. The OFR API today

- `20:14:15` /metadata/mnemonics: 200, 10045 bytes, 0.3 s -> 442 mnemonics | not in the bank's catalog: []
- `20:14:15` /series/full?mnemonic=FNYR-SOFR-A: 200, 48852 bytes, 0.2 s | top keys ['FNYR-SOFR-A'] | inner keys ['timeseries', 'metadata'] | newest ('2026-11-19', 2157, 3.91)
- `20:14:17` /series/dataset?dataset=fnyr: 200, 1.7 MB, 1.6 s | top keys ['short_name', 'long_name', 'timeseries'] | 30 series | sample FNYR-BGCR-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 30
- `20:14:23` /series/dataset?dataset=repo: 200, 9.6 MB, 6.5 s | top keys ['short_name', 'long_name', 'timeseries'] | 164 series | sample REPO-DVP_AR_G30-P keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 30, 2026-11-18 36, 2026-11-09 2, 2026-06-30 65, 2026-06-24 1
- `20:14:26` /series/dataset?dataset=nypd: 200, 3.7 MB, 2.4 s | top keys ['short_name', 'long_name', 'timeseries'] | 194 series | sample NYPD-PD_AFtD_AG-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-11 81, 2026-11-04 1, 2026-10-21 2, 2026-10-14 2, 2026-10-07 4
- `20:14:26` /series/dataset?dataset=mmf: 200, 0.2 MB, 0.4 s | top keys ['short_name', 'long_name', 'timeseries'] | 42 series | sample MMF-MMF_AG_TOT-M keys ['timeseries', 'metadata'] | newest dates: 2026-10-31 41, 2026-09-30 1
- `20:14:28` /series/dataset?dataset=tyld: 200, 2.2 MB, 2.0 s | top keys ['short_name', 'long_name', 'timeseries'] | 12 series | sample TYLD-TCMR-10Yr-A keys ['timeseries', 'metadata'] | newest dates: 2026-11-19 12
## 3. Bank against source, per mnemonic

- `20:14:30` source series 442 | bank current 123 | bank behind 312 ({'FNYR': 30, 'MMF': 42, 'NYPD': 90, 'REPO': 138, 'TYLD': 12}) | not banked 7 ['NYPD-PD_SB_TOT-A', 'NYPD-PD_SL_GE30-A', 'NYPD-PD_SL_TOT-A', 'REPO-GCF_AR_B27-F', 'REPO-GCF_AR_B27-P', 'REPO-GCF_TV_B27-F', 'REPO-GCF_TV_B27-P']
- `20:14:30`   FNYR-BGCR-A                    bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-BGCR_1Pctl-A              bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-BGCR_25Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-BGCR_75Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-BGCR_99Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-BGCR_UV-A                 bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR-A                    bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR_1Pctl-A              bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR_25Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR_75Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR_99Pctl-A             bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-EFFR_UV-A                 bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-OBFR-A                    bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   FNYR-OBFR_1Pctl-A              bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_OO-A              bank 2026-07-22 | source 2026-11-11 |   16 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_TOT-A             bank 2026-06-24 | source 2026-11-11 |    2 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_eTIPS_GE30-A      bank 2026-07-22 | source 2026-11-11 |   16 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_eTIPS_L30-A       bank 2026-07-22 | source 2026-11-11 |    7 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_eTIPS_OO-A        bank 2026-07-22 | source 2026-11-11 |   16 observations behind | banked 2026-08-06
- `20:14:30`   NYPD-PD_SL_T_eTIPS_TOT-A       bank 2026-07-22 | source 2026-11-11 |    7 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-30Yr-A               bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-3Mo-A                bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-3Yr-A                bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-5Yr-A                bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-6Mo-A                bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30`   TYLD-TCMR-7Yr-A                bank 2026-08-04 | source 2026-11-19 |   74 observations behind | banked 2026-08-06
- `20:14:30` primary-dealer mnemonics at the source: 82 still printing (NYPD-PD_AFtD_AG-A, NYPD-PD_AFtD_AG_MBS-A, NYPD-PD_AFtD_AG_eMBS-A, NYPD-PD_AFtD_CORS-A, NYPD-PD_AFtD_OMBS-A, NYPD-PD_AFtD_T-A, NYPD-PD_AFtD_TIPS-A, NYPD-PD_AFtD_TOT-A, NYPD-PD_AFtD_T_eTIPS-A, NYPD-PD_AFtR_AG-A ...), 91 ended before mid-2022
- `20:14:31` board inventory: 300 series | source series the inventory does not carry: 142 (MMF-MMF_AG_TOT-M, MMF-MMF_BRA_TOT-M, MMF-MMF_OA_TOT-M, MMF-MMF_RP_AG_G30-M, MMF-MMF_RP_AG_LE30-M, MMF-MMF_RP_AG_OO-M, MMF-MMF_RP_AG_TOT-M, MMF-MMF_RP_G30-M, MMF-MMF_RP_LE30-M, MMF-MMF_RP_OA_G30-M, MMF-MMF_RP_OA_LE30-M, MMF-MMF_RP_OA_OO-M, MMF-MMF_RP_OA_TOT-M, MMF-MMF_RP_OO-M ...)
- `20:14:31`   of those, printing in the last 30 days: MMF-MMF_AG_TOT-M, MMF-MMF_BRA_TOT-M, MMF-MMF_OA_TOT-M, MMF-MMF_RP_AG_G30-M, MMF-MMF_RP_AG_LE30-M, MMF-MMF_RP_AG_OO-M, MMF-MMF_RP_AG_TOT-M, MMF-MMF_RP_G30-M, MMF-MMF_RP_LE30-M, MMF-MMF_RP_OA_G30-M, MMF-MMF_RP_OA_LE30-M, MMF-MMF_RP_OA_OO-M, MMF-MMF_RP_OA_TOT-M, MMF-MMF_RP_OO-M, MMF-MMF_RP_TOT-M, MMF-MMF_RP_T_G30-M, MMF-MMF_RP_T_LE30-M, MMF-MMF_RP_T_OO-M, MMF-MMF_RP_T_TOT-M, MMF-MMF_RP_wDFI-M, MMF-MMF_RP_wFFI-M, MMF-MMF_RP_wFICC-M, MMF-MMF_RP_wOCP-M, MMF-MMF_TOT-M, MMF-MMF_T_TOT-M, MMF-MY_MMF_RP_AG_G30-M, MMF-MY_MMF_RP_AG_LE30-M, MMF-MY_MMF_RP_AG_OO-M, MMF-MY_MMF_RP_AG_TOT-M, MMF-MY_MMF_RP_G30-M, MMF-MY_MMF_RP_LE30-M, MMF-MY_MMF_RP_OA_G30-M, MMF-MY_MMF_RP_OA_LE30-M, MMF-MY_MMF_RP_OA_OO-M, MMF-MY_MMF_RP_OA_TOT-M, MMF-MY_MMF_RP_OO-M, MMF-MY_MMF_RP_TOT-M, MMF-MY_MMF_RP_T_G30-M, MMF-MY_MMF_RP_T_LE30-M, MMF-MY_MMF_RP_T_OO-M
## 4. Hedge-fund catalog: sponsored repo

- `20:14:31` state written 2026-11-21 02:29 | status COMPLETE-maintaining | catalog 497 | refresh_cursor 253
- `20:14:31`   FICC-SPONSORED_REPO_VOL      bank newest 2026-09-18 (file written 2026-10-14) | source 200 newest 2026-10-13 (1640 obs, 0.3 s)
- `20:14:31`   FICC-SPONSORED_REVREPO_VOL   bank newest 2026-09-18 (file written 2026-10-14) | source 200 newest 2026-10-13 (1640 obs, 0.2 s)
- `20:14:32`   /hf/v1/series/dataset?dataset=ficc: 200, 111823 bytes, 0.2 s, 2 series ['FICC-SPONSORED_REPO_VOL', 'FICC-SPONSORED_REVREPO_VOL']
## 5. The engine's own runs, 14 days

- `20:14:32` justhodl-ofr-stfm            runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 5.5 s
- `20:14:33` justhodl-repo                runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 598.4 s
- `20:14:33` justhodl-repo-market         runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 10.4 s
- `20:14:33` justhodl-nyfed-markets-full  runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 12.2 s
- `20:14:33` justhodl-nyfed-pd            runs/day 11-12:1 11-13:1 11-19:1 11-20:1 | errors 0 | mean duration 2.9 s
## 6. The other feeders

- `20:14:34` haircut index written 2026-08-16 | 81 series | keys ['built_at']
- `20:14:34`   www.newyorkfed.org/medialibrary/media/banking/pdf/tpr_stats_historical.xlsx -> 200, 101157 bytes, 0.4 s, last-modified None | 
- `20:14:34`   www.newyorkfed.org/medialibrary/media/research/banking_research/tri-party-repo/tpr-statistics-hi -> 200, 101157 bytes, 0.2 s, last-modified None | 
- `20:14:34`   www.dtcc.com/charts/daily-total-us-treasury-trade-fails.json -> 404, 0 bytes, 0.2 s, last-modified None | 
- `20:14:34`   www.dtcc.com/-/media/Files/Downloads/Charts/daily-treasury-fails.csv -> 404, 0 bytes, 0.1 s, last-modified None | 
- `20:14:35`   markets.newyorkfed.org/api/pd/list/timeseries.json -> 200, 335616 bytes, 0.1 s, last-modified None | {"pd": { "timeseries": [ { "seriesbreak": "SBN2024", "keyid": "PDPOSMBS-TOT", "description": "Total - Mortgage
- `20:14:35`   markets.newyorkfed.org/api/pd/get/PDSORA-UTSETTOT.json -> 200, 55567 bytes, 0.1 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2013-04-03", "keyid": "PDSORA-UTSETTOT", "value": "1535302" } , { "aso
- `20:14:35`   markets.newyorkfed.org/api/pd/latest/SBN2024.json -> 200, 114798 bytes, 0.0 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2026-11-11", "keyid": "PDWOTIPSC", "value": "-9341" }, { "asofdate": "
- `20:14:35` data/settlement-fails.json written 2026-11-20 21:30 | {"allocation_pct": null, "as_of": "2026-11-11", "call": null, "calls_eligible": false, "classes": "{6}", "contract": "fr2004-fails-research.v1", "decision_id": "fails-5f972fdbb2ead07899140fa1d7838fbc7c4a40a8e010de685e1c42009b13bad8", "engine": "settlement-fails", "execution_eligible": false, "generated_at": "2026-11-20T21:30:39.667691+00:00", "headline": "{48}", "legacy_context": "{6}", "limitatio
- `20:14:35` ✅ read-only; nothing changed

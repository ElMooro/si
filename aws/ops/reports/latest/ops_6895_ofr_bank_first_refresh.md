# ops 6895 - OFR bank: first refresh since 2026-08-06, 2026-11-21T20:25+00:00

**Status:** success  
**Duration:** 69.9s  
**Finished:** 2026-11-21T20:26:43+00:00  

## Data

| behind_after | hf_level | second_run_would_write | written |
|---|---|---|---|
| 0 | True | 0 | 442 |

## Log
## 1. What is deployed

- `20:25:33` justhodl-ofr-stfm updated 2026-11-21T20:23:49 | {"ofr_refresh.py": true, "lambda_function.py": true}
## 2. One run

- `20:25:33` series files before, by day of last write: {'2026-08-06': 442}
- `20:26:15` invoke: status 200 in 42 s
- `20:26:15` refresh {"written": 442, "unchanged": 0, "newest": {"2026-11-19": 72, "2026-11-18": 36, "2026-11-11": 81, "2026-11-09": 2, "2026-11-04": 1, "2026-10-31": 41}} | problems {"shrank": 0, "failed": 0, "datasets": {}} | hedge-fund pair {"written": 2, "unchanged": 0, "newest": {"2026-10-15": 2}} | tranche pulled 0 failed 0
## 3. Read back

- `20:26:15` state written 2026-11-21 20:26 | status COMPLETE-maintaining | refresh at 2026-11-21T20:25:34+00:00: written 442, unchanged 0, shrank 0, failed 0
- `20:26:15`   dataset fnyr  {"series": 30, "written": 30, "unchanged": 0}
- `20:26:15`   dataset repo  {"series": 164, "written": 164, "unchanged": 0}
- `20:26:15`   dataset nypd  {"series": 194, "written": 194, "unchanged": 0}
- `20:26:15`   dataset mmf   {"series": 42, "written": 42, "unchanged": 0}
- `20:26:15`   dataset tyld  {"series": 12, "written": 12, "unchanged": 0}
- `20:26:15` series files after: 442, 3.8 MB | by day of last write: {'2026-11-21': 442}
- `20:26:29` bank against source: 442 mnemonics, 442 current, 0 behind []
- `20:26:29`   FNYR-SOFR-A            bank newest 2026-11-19 | source newest 2026-11-19
- `20:26:29`   FNYR-TGCR-A            bank newest 2026-11-19 | source newest 2026-11-19
- `20:26:29`   REPO-TRIV1_AR_OO-P     bank newest 2026-11-18 | source newest 2026-11-18
- `20:26:29`   REPO-GCF_AR_OO-P       bank newest 2026-11-19 | source newest 2026-11-19
- `20:26:29`   REPO-DVP_AR_OO-P       bank newest 2026-11-19 | source newest 2026-11-19
- `20:26:29`   NYPD-PD_AFtD_T-A       bank newest 2026-11-11 | source newest 2026-11-11
- `20:26:29`   MMF-MMF_RP_TOT-M       bank newest 2026-10-31 | source newest 2026-10-31
- `20:26:29`   FICC-SPONSORED_REPO_VOL      bank newest 2026-10-15 (written 2026-11-21 20:26) | source newest 2026-10-15
- `20:26:29`   FICC-SPONSORED_REVREPO_VOL   bank newest 2026-10-15 (written 2026-11-21 20:26) | source newest 2026-10-15
## 3b. Source series the repo board's inventory does not carry

- `20:26:29` metadata of one series: {"mnemonic": "FNYR-BGCR-A", "description": {"vintage_approach": "Current vintage, as of last update", "vintage": "", "notes": "It is possible that revisions to the rates could occur after the data are retrieved by the OFR, or that other errors could be introduced, so parties seeking to use these data in contracts should retrieve them directly from the Federal Reserve Bank of New York. Users of these rates are subject to the New York Fed\u2019s <a href='https://www.newyorkfed.org/markets/reference-rates-terms-of-use'>terms of use.</a>", "name": "Broad General Collateral Rate", "subsetting": "None", "subtype": "Interest Rate", "description": "The volume-weighted median, which is the rate assoc
- `20:26:29`   MMF-MMF_AG_TOT-M               newest 2026-10-31 | Money Market Mutual Fund Investments in Federal Agency and GSE Securities
- `20:26:29`   MMF-MMF_BRA_TOT-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Bank-Related Assets
- `20:26:29`   MMF-MMF_OA_TOT-M               newest 2026-10-31 | Money Market Mutual Fund Investments in Other Assets
- `20:26:29`   MMF-MMF_RP_AG_G30-M            newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securities: Term, >30 Day
- `20:26:29`   MMF-MMF_RP_AG_LE30-M           newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securities: Term, <=30 Da
- `20:26:29`   MMF-MMF_RP_AG_OO-M             newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securities: Overnight/Ope
- `20:26:29`   MMF-MMF_RP_AG_TOT-M            newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securities: Total
- `20:26:29`   MMF-MMF_RP_G30-M               newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements: Term, >30 Days
- `20:26:29`   MMF-MMF_RP_LE30-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements: Term, <=30 Days
- `20:26:29`   MMF-MMF_RP_OA_G30-M            newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Term, >30 Days
- `20:26:29`   MMF-MMF_RP_OA_LE30-M           newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Term, <=30 Days
- `20:26:29`   MMF-MMF_RP_OA_OO-M             newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Overnight/Open
- `20:26:29`   MMF-MMF_RP_OA_TOT-M            newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Total
- `20:26:29`   MMF-MMF_RP_OO-M                newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements: Overnight/Open
- `20:26:29`   MMF-MMF_RP_TOT-M               newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements
- `20:26:29`   MMF-MMF_RP_T_G30-M             newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Term, >30 Days
- `20:26:29`   MMF-MMF_RP_T_LE30-M            newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Term, <=30 Days
- `20:26:29`   MMF-MMF_RP_T_OO-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Overnight/Open
- `20:26:29`   MMF-MMF_RP_T_TOT-M             newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Total
- `20:26:29`   MMF-MMF_RP_wDFI-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements with U.S. Financial Institutions
- `20:26:29`   MMF-MMF_RP_wFFI-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements with Foreign Financial Institutions
- `20:26:29`   MMF-MMF_RP_wFICC-M             newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements Cleared by FICC
- `20:26:29`   MMF-MMF_RP_wFR-M               newest 2026-09-30 | Money Market Mutual Fund Investments in Repurchase Agreements with the Federal Reserve
- `20:26:29`   MMF-MMF_RP_wOCP-M              newest 2026-10-31 | Money Market Mutual Fund Investments in Repurchase Agreements with Other Counterparties
- `20:26:29`   MMF-MMF_TOT-M                  newest 2026-10-31 | Money Market Mutual Fund Investments: Total
- `20:26:29`   MMF-MMF_T_TOT-M                newest 2026-10-31 | Money Market Mutual Fund Investments in U.S. Treasury Securities
- `20:26:29`   MMF-MY_MMF_RP_AG_G30-M         newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securitie
- `20:26:29`   MMF-MY_MMF_RP_AG_LE30-M        newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securitie
- `20:26:29`   MMF-MY_MMF_RP_AG_OO-M          newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securitie
- `20:26:29`   MMF-MY_MMF_RP_AG_TOT-M         newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Federal Agency and GSE Securitie
- `20:26:29`   MMF-MY_MMF_RP_G30-M            newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements: Term, >30 Days
- `20:26:29`   MMF-MY_MMF_RP_LE30-M           newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements: Term, <=30 Days
- `20:26:29`   MMF-MY_MMF_RP_OA_G30-M         newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Term, >30 Days
- `20:26:29`   MMF-MY_MMF_RP_OA_LE30-M        newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Term, <=30 Days
- `20:26:29`   MMF-MY_MMF_RP_OA_OO-M          newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Overnight/Open
- `20:26:29`   MMF-MY_MMF_RP_OA_TOT-M         newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by Other Assets: Total
- `20:26:29`   MMF-MY_MMF_RP_OO-M             newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements: Overnight/Open
- `20:26:29`   MMF-MY_MMF_RP_TOT-M            newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements: Total
- `20:26:29`   MMF-MY_MMF_RP_T_G30-M          newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Term, 
- `20:26:29`   MMF-MY_MMF_RP_T_LE30-M         newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Term, 
- `20:26:29`   MMF-MY_MMF_RP_T_OO-M           newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Overni
- `20:26:29`   MMF-MY_MMF_RP_T_TOT-M          newest 2026-10-31 | Median Yield on Money Market Mutual Fund Investments in Repurchase Agreements Backed by U.S. Treasury Securities: Total
- `20:26:29`   NYPD-PD_SB_ABS_GE30-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Asset-Backed Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_ABS_L30-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Asset-Backed Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_ABS_OO-A            newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Asset-Backed Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_ABS_TOT-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Asset-Backed Securities: Total
- `20:26:29`   NYPD-PD_SB_AG_GE30-A           newest 2026-10-07 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_AG_L30-A            newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_AG_MBS_GE30-A       newest 2026-10-07 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE MBS: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_AG_MBS_L30-A        newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE MBS: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_AG_MBS_OO-A         newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE MBS: Overnight/Open
- `20:26:29`   NYPD-PD_SB_AG_MBS_TOT-A        newest 2026-10-07 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE MBS: Total
- `20:26:29`   NYPD-PD_SB_AG_OO-A             newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_AG_TOT-A            newest 2026-10-07 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities: Total
- `20:26:29`   NYPD-PD_SB_AG_eMBS_GE30-A      newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities (Excluding MBS): Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_AG_eMBS_L30-A       newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities (Excluding MBS): Term, <30 Days
- `20:26:29`   NYPD-PD_SB_AG_eMBS_OO-A        newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities (Excluding MBS): Overnight/Open
- `20:26:29`   NYPD-PD_SB_AG_eMBS_TOT-A       newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Federal Agency and GSE Securities (Excluding MBS): Total
- `20:26:29`   NYPD-PD_SB_CORD_GE30-A         newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Corporate Debt: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_CORD_L30-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Corporate Debt: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_CORD_OO-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Corporate Debt: Overnight/Open
- `20:26:29`   NYPD-PD_SB_CORD_TOT-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Corporate Debt: Total
- `20:26:29`   NYPD-PD_SB_EQT_GE30-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Equities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_EQT_L30-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Equities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_EQT_OO-A            newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Equities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_EQT_TOT-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Equities: Total
- `20:26:29`   NYPD-PD_SB_GE30-A              newest 2024-10-09 | Primary Dealer Securities Borrowed: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_L30-A               newest 2026-02-18 | Primary Dealer Securities Borrowed: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_OO-A                newest 2026-11-11 | Primary Dealer Securities Borrowed: Overnight/Open
- `20:26:29`   NYPD-PD_SB_OS_GE30-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Other Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_OS_L30-A            newest 2026-02-18 | Primary Dealer Securities Borrowed Backed by Other Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_OS_OO-A             newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by Other Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_OS_TOT-A            newest 2025-07-30 | Primary Dealer Securities Borrowed Backed by Other Securities: Total
- `20:26:29`   NYPD-PD_SB_TIPS_GE30-A         newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Inflation-Protected Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_TIPS_L30-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Inflation-Protected Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_TIPS_OO-A           newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Inflation-Protected Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_TIPS_TOT-A          newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Inflation-Protected Securities: Total
- `20:26:29`   NYPD-PD_SB_TOT-A               newest None       | Primary Dealer Securities Borrowed: Total
- `20:26:29`   NYPD-PD_SB_T_GE30-A            newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_T_L30-A             newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SB_T_OO-A              newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SB_T_TOT-A             newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities: Total
- `20:26:29`   NYPD-PD_SB_T_eTIPS_GE30-A      newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities (Excluding TIPS): Term, >=30 Days
- `20:26:29`   NYPD-PD_SB_T_eTIPS_L30-A       newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities (Excluding TIPS): Term, <30 Days
- `20:26:29`   NYPD-PD_SB_T_eTIPS_OO-A        newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities (Excluding TIPS): Overnight/Open
- `20:26:29`   NYPD-PD_SB_T_eTIPS_TOT-A       newest 2026-11-11 | Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities (Excluding TIPS): Total
- `20:26:29`   NYPD-PD_SL_ABS_GE30-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by Asset-Backed Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_ABS_L30-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by Asset-Backed Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_ABS_OO-A            newest 2026-11-11 | Primary Dealer Securities Lent Backed by Asset-Backed Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_ABS_TOT-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by Asset-Backed Securities: Total
- `20:26:29`   NYPD-PD_SL_AG_GE30-A           newest 2024-02-28 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_AG_L30-A            newest 2026-10-21 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_AG_MBS_GE30-A       newest 2024-02-28 | Primary Dealer Securities Lent Backed by Federal Agency and GSE MBS: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_AG_MBS_L30-A        newest 2026-10-21 | Primary Dealer Securities Lent Backed by Federal Agency and GSE MBS: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_AG_MBS_OO-A         newest 2026-11-04 | Primary Dealer Securities Lent Backed by Federal Agency and GSE MBS: Overnight/Open
- `20:26:29`   NYPD-PD_SL_AG_MBS_TOT-A        newest 2024-02-07 | Primary Dealer Securities Lent Backed by Federal Agency and GSE MBS: Total
- `20:26:29`   NYPD-PD_SL_AG_OO-A             newest 2025-12-17 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_AG_TOT-A            newest 2024-02-07 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities: Total
- `20:26:29`   NYPD-PD_SL_AG_eMBS_GE30-A      newest 2026-11-11 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities (Excluding MBS): Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_AG_eMBS_L30-A       newest 2026-11-11 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities (Excluding MBS): Term, <30 Days
- `20:26:29`   NYPD-PD_SL_AG_eMBS_OO-A        newest 2026-04-29 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities (Excluding MBS): Overnight/Open
- `20:26:29`   NYPD-PD_SL_AG_eMBS_TOT-A       newest 2026-04-29 | Primary Dealer Securities Lent Backed by Federal Agency and GSE Securities (Excluding MBS): Total
- `20:26:29`   NYPD-PD_SL_CORD_GE30-A         newest 2026-11-11 | Primary Dealer Securities Lent Backed by Corporate Debt: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_CORD_L30-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by Corporate Debt: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_CORD_OO-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by Corporate Debt: Overnight/Open
- `20:26:29`   NYPD-PD_SL_CORD_TOT-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by Corporate Debt: Total
- `20:26:29`   NYPD-PD_SL_EQT_GE30-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by Equities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_EQT_L30-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by Equities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_EQT_OO-A            newest 2026-11-11 | Primary Dealer Securities Lent Backed by Equities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_EQT_TOT-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by Equities: Total
- `20:26:29`   NYPD-PD_SL_GE30-A              newest None       | Primary Dealer Securities Lent: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_L30-A               newest 2026-02-25 | Primary Dealer Securities Lent: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_OO-A                newest 2023-09-27 | Primary Dealer Securities Lent: Overnight/Open
- `20:26:29`   NYPD-PD_SL_OS_GE30-A           newest 2026-10-14 | Primary Dealer Securities Lent Backed by Other Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_OS_L30-A            newest 2026-11-11 | Primary Dealer Securities Lent Backed by Other Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_OS_OO-A             newest 2026-11-11 | Primary Dealer Securities Lent Backed by Other Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_OS_TOT-A            newest 2026-10-14 | Primary Dealer Securities Lent Backed by Other Securities: Total
- `20:26:29`   NYPD-PD_SL_TIPS_GE30-A         newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Inflation-Protected Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_TIPS_L30-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Inflation-Protected Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_TIPS_OO-A           newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Inflation-Protected Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_TIPS_TOT-A          newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Inflation-Protected Securities: Total
- `20:26:29`   NYPD-PD_SL_TOT-A               newest None       | Primary Dealer Securities Lent: Total
- `20:26:29`   NYPD-PD_SL_T_GE30-A            newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities: Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_T_L30-A             newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities: Term, <30 Days
- `20:26:29`   NYPD-PD_SL_T_OO-A              newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities: Overnight/Open
- `20:26:29`   NYPD-PD_SL_T_TOT-A             newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities: Total
- `20:26:29`   NYPD-PD_SL_T_eTIPS_GE30-A      newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities (Excluding TIPS): Term, >=30 Days
- `20:26:29`   NYPD-PD_SL_T_eTIPS_L30-A       newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities (Excluding TIPS): Term, <30 Days
- `20:26:29`   NYPD-PD_SL_T_eTIPS_OO-A        newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities (Excluding TIPS): Overnight/Open
- `20:26:29`   NYPD-PD_SL_T_eTIPS_TOT-A       newest 2026-11-11 | Primary Dealer Securities Lent Backed by U.S. Treasury Securities (Excluding TIPS): Total
- `20:26:29`   TYLD-TCMR-10Yr-A               newest 2026-11-19 | 10-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-1Mo-A                newest 2026-11-19 | 1-Month Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-1Yr-A                newest 2026-11-19 | 1-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-20Yr-A               newest 2026-11-19 | 20-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-2Mo-A                newest 2026-11-19 | 2-Month Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-2Yr-A                newest 2026-11-19 | 2-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-30Yr-A               newest 2026-11-19 | 30-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-3Mo-A                newest 2026-11-19 | 3-Month Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-3Yr-A                newest 2026-11-19 | 3-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-5Yr-A                newest 2026-11-19 | 5-Year Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-6Mo-A                newest 2026-11-19 | 6-Month Treasury Constant Maturity Rate
- `20:26:29`   TYLD-TCMR-7Yr-A                newest 2026-11-19 | 7-Year Treasury Constant Maturity Rate
## 4. A second look: what a run would write now

- `20:26:43` dry run on the Lambda: would write 0, unchanged 442 | hedge-fund pair would write 0
- `20:26:43` ✅ the bank holds the source's newest observation for every mnemonic, and a second run would write nothing

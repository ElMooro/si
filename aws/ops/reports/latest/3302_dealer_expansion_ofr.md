## 1. nyfed-pd v3 (financing + transactions)

**Status:** success  
**Duration:** 45.9s  
**Finished:** 2026-07-14T16:51:46+00:00  

## Data

| catalog_mmf_n | catalog_repo_n | corp_net_bonds_b | fails | financing | live_manifest_category | live_manifest_has_page | live_manifest_macro | mmf_n | mmf_picks | nypd_fails_cross | ofr_ftd_tot | repo_n | repo_venues | sf_regime | sf_ust_ftd_latest | turnover_velocity | txn_classes | txn_corporate | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 |  |  |  |  |  |  | None | [] | {'ftd_tot': {'mnemonic': 'NYPD-PD_AFtD_TOT-A', 'latest': 165509000000.0, 'as_of': '2026-07-01', 'd1': 12494000000.0, 'd20': -13287000000.0, 'z_1y': 0.39}, 'ftr_tot': {'mnemonic': 'NYPD-PD_AFtR_TOT-A', 'latest': 170687000000.0, 'as_of': '2026-07-01', 'd1': 1911000000.0, 'd20': 1086000000.0, 'z_1y': 0.49}} |  | None | {} |  |  |  |  |  |  |  |
|  |  | -3.87 |  | {'as_of': '2026-07-01', 'reverse_repo_in_b': 3215.2, 'repo_out_b': 1683.4, 'securities_in_b': 3215.2, 'securities_out_b': 1683.4, 'net_lend_b': 1531.8, 'sec_lent_b': 283.7, 'sec_borrowed_b': 847.2, 'corp_rev_repo_in_b': 46.9, 'corp_repo_out_b': 162.2, 'in_wow_b': 97.3, 'out_wow_b': -3193.7, 'read': 'Dealer financing book: $3.22T reverse repo IN (cash lent against collateral) vs $1.68T repo OUT (cash borrowed) — the leverage engine behind every position on this page.'} |  |  |  |  |  |  |  |  |  |  |  | 46.7 | ['ABS', 'AGENCY_DEBT', 'AGENCY_MBS', 'CORPORATE', 'GST', 'MUNIS', 'TREASURY'] | {'weekly_b': 172.2, 'avg_4w_b': 180.8, 'as_of': '2026-07-01'} | 3.0.0 |  |
|  |  |  |  |  |  |  |  |  |  |  | 165509000000.0 |  |  | None | 96.16 |  |  |  |  |  |
|  |  |  |  |  | Macro & Liquidity |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | [] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["OFR repo dataset error: dataset=repo -> 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte", "OFR mmf dataset error: dataset=mmf -> 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte", 'fails cross-check scale mismatch: ours 96.2 vs OFR 165509000000.0 (TOT covers all classes; ours is UST only — informational)'] |

## Log
- `16:51:00`   zip: 80378 bytes
## 1. Lambda

- `16:51:01`   Lambda exists — updating
- `16:51:06` ✅   ✓ updated justhodl-nyfed-pd
- `16:51:07` spec pos family cleared -> rediscovery will run
## 2. NEW justhodl-ofr-stfm + Scheduler

- `16:51:07`   zip: 76041 bytes
## 1. Lambda

- `16:51:07`   Lambda exists — updating
- `16:51:12` ✅   ✓ updated justhodl-ofr-stfm
- `16:51:13` scheduler updated
## 3. Verify OFR feed

## 4. Verify nyfed-pd v3 blocks + fails reconciliation

## 5. Sidebar + page markers (live)

- `16:51:46` OPS 3302 PASS — dealer desk covers positions + fails + financing + turnover, OFR STFM live for the fleet, sidebar fixed into Macro & Liquidity.

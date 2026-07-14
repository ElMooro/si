## 1. nyfed-pd v3 (financing + transactions)

**Status:** failure  
**Duration:** 57.1s  
**Finished:** 2026-07-14T16:49:43+00:00  

## Error

```
SystemExit: FAILS: financing block missing
```

## Data

| catalog_mmf_n | catalog_repo_n | corp_net_bonds_b | fails | financing | live_manifest_category | live_manifest_has_page | live_manifest_macro | mmf_n | mmf_picks | nypd_fails_cross | ofr_ftd_tot | repo_n | repo_venues | sf_regime | sf_ust_ftd_latest | turnover_velocity | txn_classes | txn_corporate | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 |  |  |  |  |  |  | None | [] | {'ftd_tot': {'mnemonic': 'NYPD-PD_AFtD_TOT-A', 'latest': 165509000000.0, 'as_of': '2026-07-01', 'd1': 12494000000.0, 'd20': -13287000000.0, 'z_1y': 0.39}, 'ftr_tot': {'mnemonic': 'NYPD-PD_AFtR_TOT-A', 'latest': 170687000000.0, 'as_of': '2026-07-01', 'd1': 1911000000.0, 'd20': 1086000000.0, 'z_1y': 0.49}} |  | None | {} |  |  |  |  |  |  |  |
|  |  | -3.87 |  | None |  |  |  |  |  |  |  |  |  |  |  | 46.7 | ['ABS', 'AGENCY_DEBT', 'AGENCY_MBS', 'CORPORATE', 'GST', 'MUNIS', 'TREASURY'] | {'weekly_b': 172.2, 'avg_4w_b': 180.8, 'as_of': '2026-07-01'} | 3.0.0 |  |
|  |  |  |  |  |  |  |  |  |  |  | 165509000000.0 |  |  | None | 96.16 |  |  |  |  |  |
|  |  |  |  |  | Macro & Liquidity |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | ['financing block missing'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["OFR repo dataset error: dataset=repo -> 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte", "OFR mmf dataset error: dataset=mmf -> 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte", 'fails cross-check scale mismatch: ours 96.2 vs OFR 165509000000.0 (TOT covers all classes; ours is UST only — informational)'] |

## Log
- `16:48:46`   zip: 80366 bytes
## 1. Lambda

- `16:48:46`   Lambda exists — updating
- `16:48:52` ✅   ✓ updated justhodl-nyfed-pd
## 2. NEW justhodl-ofr-stfm + Scheduler

- `16:48:52`   zip: 76041 bytes
## 1. Lambda

- `16:48:53`   Lambda exists — updating
- `16:48:58` ✅   ✓ updated justhodl-ofr-stfm
- `16:48:59` scheduler updated
## 3. Verify OFR feed

## 4. Verify nyfed-pd v3 blocks + fails reconciliation

## 5. Sidebar + page markers (live)


## 1. nyfed-pd v3 (financing + transactions)

**Status:** failure  
**Duration:** 335.2s  
**Finished:** 2026-07-14T16:41:33+00:00  

## Error

```
SystemExit: FAILS: ofr-stfm output never freshened; fewer than 2 repo venues resolved: []; financing block missing
```

## Data

| catalog_mmf_n | catalog_repo_n | corp_net_bonds_b | fails | financing | live_manifest_category | live_manifest_has_page | live_manifest_macro | mmf_n | mmf_picks | nypd_fails_cross | ofr_ftd_tot | repo_n | repo_venues | sf_regime | sf_ust_ftd_latest | turnover_velocity | txn_classes | txn_corporate | version | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 |  |  |  |  |  |  | None | [] | None |  | None | {} |  |  |  |  |  |  |  |
|  |  | -3.87 |  | None |  |  |  |  |  |  |  |  |  |  |  | 46.7 | ['ABS', 'AGENCY_DEBT', 'AGENCY_MBS', 'CORPORATE', 'GST', 'MUNIS', 'TREASURY'] | {'weekly_b': 172.2, 'avg_4w_b': 180.8, 'as_of': '2026-07-01'} | 3.0.0 |  |
|  |  |  |  |  |  |  |  |  |  |  | None |  |  | None | 96.16 |  |  |  |  |  |
|  |  |  |  |  | Macro & Liquidity |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | ['ofr-stfm output never freshened', 'fewer than 2 repo venues resolved: []', 'financing block missing'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [] |

## Log
- `16:35:58`   zip: 80043 bytes
## 1. Lambda

- `16:35:59`   Lambda exists — updating
- `16:36:02` ✅   ✓ updated justhodl-nyfed-pd
- `16:36:03` spec pos family cleared -> rediscovery will run
## 2. NEW justhodl-ofr-stfm + Scheduler

- `16:36:03`   zip: 75687 bytes
## 1. Lambda

- `16:36:03`   Lambda missing — creating
- `16:36:08` ✅   ✓ created justhodl-ofr-stfm
- `16:36:09` ✅   ✓ Function URL: https://rpiqab4imzgcizdoxyxztngvua0cdrgo.lambda-url.us-east-1.on.aws/
- `16:36:09` scheduler created
## 3. Verify OFR feed

## 4. Verify nyfed-pd v3 blocks + fails reconciliation

## 5. Sidebar + page markers (live)


## 1. Deploy the three engines

**Status:** success  
**Duration:** 89.9s  
**Finished:** 2026-07-14T17:40:17+00:00  

## Data

| composite_health | fails | gcf_tri | health | liq_score | mmf_families | mmf_pick_keys | mmf_repo_pool | ofr_repo_depth | onshore_funding | us_core_keys | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'ok_blocks': 2, 'errors': {}} |  | {'AG': {'pick': 'MMF-MMF_AG_TOT-M', 'n': 1, 'latest': 1193881007522.37}, 'BRA': {'pick': 'MMF-MMF_BRA_TOT-M', 'n': 1, 'latest': 482875622526.13}, 'OA': {'pick': 'MMF-MMF_OA_TOT-M', 'n': 1, 'latest': 341623929663.74}, 'RP': {'pick': 'MMF-MMF_RP_TOT-M', 'n': 37, 'latest': 3004242276349.22}, 'T': {'pick': 'MMF-MMF_T_TOT-M', 'n': 1, 'latest': 3378363745446.02}, 'TOT': {'pick': 'MMF-MMF_TOT-M', 'n': 1, 'latest': 8400986581507.48}} | ['agency_holdings', 'bra', 'oa', 'repo_holdings', 't', 'total_net_assets'] |  |  |  |  |  |
| None |  | {'id': 'gcf_tri', 'label': 'GCF − Triparty repo (interdealer premium)', 'value': 4.0, 'unit': 'bp', 'status': 'yellow', 'detail': 'Interdealer GC trading above triparty = dealer balance-sheet scarcity; collateral velocity jamming (OFR STFM)', 'pctile': None, 'asof': None} |  |  |  |  | {'id': 'mmf_repo_pool', 'label': 'MMF cash lent into repo', 'value': 3004.0, 'unit': '$bn', 'status': 'info', 'detail': 'Money-fund cash on the other side of dealer repo books (OFR)', 'pctile': None, 'asof': None} | {'id': 'ofr_repo_depth', 'label': 'US repo volume (DVP+TRI+GCF)', 'value': 5.77, 'unit': '$tn', 'status': 'info', 'detail': 'Onshore funding-market depth from OFR — the water level under dealer balance sheets', 'pctile': None, 'asof': None} |  | ['sofr_iorb', 'sofr99_iorb', 'on_rrp', 'reserves', 'tga', 'effr_iorb', 'gcf_tri', 'ofr_repo_depth', 'mmf_repo_pool'] |  |
|  |  |  |  | 42.9 |  |  |  |  | {'gcf_minus_tri_bp': 4.0, 'tone': 'WATCH', 'dvp_t': 3.37, 'tri_t': 2.12, 'gcf_b': 283.0, 'mmf_repo_b': 3004.0, 'note': 'Onshore repo plumbing (OFR): interdealer premium 4.0bp — WATCH'} |  |  |
|  | [] |  |  |  |  |  |  |  |  |  | [] |

## Log
- `17:38:47`   zip: 76378 bytes
## 1. Lambda

- `17:38:48`   Lambda exists — updating
- `17:38:53` ✅   ✓ updated justhodl-ofr-stfm
- `17:38:54`   zip: 84769 bytes
## 1. Lambda

- `17:38:54`   Lambda exists — updating
- `17:38:59` ✅   ✓ updated justhodl-eurodollar-plumbing
- `17:39:00`   zip: 99988 bytes
## 1. Lambda

- `17:39:00`   Lambda exists — updating
- `17:39:06` ✅   ✓ updated justhodl-liquidity-inflection
## 2. OFR v1.1 — MMF families

## 3. Plumbing us_core joins

## 4. Liquidity-inflection context block

- `17:40:17` OPS 3304 PASS — OFR wired into the funding stack: plumbing scores the interdealer premium, liquidity carries onshore context, MMF curation is grammar-driven (6 families).

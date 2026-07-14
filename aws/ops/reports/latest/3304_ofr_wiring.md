## 1. Deploy the three engines

**Status:** failure  
**Duration:** 84.7s  
**Finished:** 2026-07-14T17:37:17+00:00  

## Error

```
SystemExit: FAILS: gcf_tri metric missing from us_core; ofr_repo_depth metric missing
```

## Data

| composite_health | fails | gcf_tri | health | liq_score | mmf_families | mmf_pick_keys | mmf_repo_pool | ofr_repo_depth | onshore_funding | us_core_keys | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | {'ok_blocks': 2, 'errors': {}} |  | {'AG': {'pick': 'MMF-MMF_AG_TOT-M', 'n': 1, 'latest': 1193881007522.37}, 'BRA': {'pick': 'MMF-MMF_BRA_TOT-M', 'n': 1, 'latest': 482875622526.13}, 'OA': {'pick': 'MMF-MMF_OA_TOT-M', 'n': 1, 'latest': 341623929663.74}, 'OTHER': {'pick': 'MMF-MY_MMF_RP_TOT-M', 'n': 16, 'latest': 3.62}, 'RP': {'pick': 'MMF-MMF_RP_TOT-M', 'n': 21, 'latest': 3004242276349.22}, 'T': {'pick': 'MMF-MMF_T_TOT-M', 'n': 1, 'latest': 3378363745446.02}, 'TOT': {'pick': 'MMF-MMF_TOT-M', 'n': 1, 'latest': 8400986581507.48}} | ['agency_holdings', 'bra', 'oa', 'other', 'repo_holdings', 't', 'total_net_assets'] |  |  |  |  |  |
| None |  | None |  |  |  |  | None | None |  | [None, None, None, None, None, None, None, None, None] |  |
|  |  |  |  | 42.9 |  |  |  |  | {'gcf_minus_tri_bp': 4.0, 'tone': 'WATCH', 'dvp_t': 3.37, 'tri_t': 2.12, 'gcf_b': 283.0, 'mmf_repo_b': 3004.0, 'note': 'Onshore repo plumbing (OFR): interdealer premium 4.0bp — WATCH'} |  |  |
|  | ['gcf_tri metric missing from us_core', 'ofr_repo_depth metric missing'] |  |  |  |  |  |  |  |  |  | ['mmf_repo_pool missing (pick may lack latest)'] |

## Log
- `17:35:52`   zip: 76369 bytes
## 1. Lambda

- `17:35:53`   Lambda exists — updating
- `17:35:58` ✅   ✓ updated justhodl-ofr-stfm
- `17:35:58`   zip: 84769 bytes
## 1. Lambda

- `17:35:58`   Lambda exists — updating
- `17:36:01` ✅   ✓ updated justhodl-eurodollar-plumbing
- `17:36:02`   zip: 99988 bytes
## 1. Lambda

- `17:36:02`   Lambda exists — updating
- `17:36:07` ✅   ✓ updated justhodl-liquidity-inflection
- `17:36:07` ✅   ✓ Function URL: https://nugw7kvtktbz7qrs2twpckato40idtvs.lambda-url.us-east-1.on.aws/
## 2. OFR v1.1 — MMF families

## 3. Plumbing us_core joins

## 4. Liquidity-inflection context block


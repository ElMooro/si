# ops 4738 -- overnight-repo super-monitor recon (read-only)

**Status:** success  
**Duration:** 3.4s  
**Finished:** 2026-08-16T14:50:56+00:00  

## Data

| check | value |
|---|---|
| ofr_state_banked | 442 |
| ofr_state_worklist_remaining | 0 |
| ofr_state_keys | as_of,catalog,catalog_checked_at,catalog_source,done,progress_pct,status |
| nccbr_banked_count | 0 |
| ofr_series_objects_on_s3 | 442 |
| catalog_endpoint_used | https://data.financialresearch.gov/v1/metadata/mnemonics |
| ofr_catalog_total_live | 442 |
| nccbr_in_live_catalog | 0 |
| haircut_named_mnemonics | 0 |
| in_catalog_not_banked | 0 |
| depth_probe_mnemonic | REPO-TRIV1_AR_AG-F |
| depth_probe_n_obs | 0 |
| depth_probe_earliest | None |
| depth_probe_latest | None |
| deny_delete_on_warm | True |
| bucket_versioning | Enabled |

## Log
## A. warm/ofr state -- banked vs worklist, NCCBR presence

## B. Live OFR catalog -- total, prefixes, NCCBR, haircut mentions

- `14:50:54` prefix breakdown: NYPD=194, REPO=164, MMF=42, FNYR=30, TYLD=12
- `14:50:56` metadata/search?query=haircut -> status=200 body[:300]=[]
## C. Depth proof on one banked series

## D. NY Fed securities lending -- live probe (no engine pulls this)

- `14:50:56` seclending/all/results/last/5.json -> ok=False status=400 body[:260]=
- `14:50:56` seclending/all/results/latest.json -> ok=False status=400 body[:260]=
## E. Permanence -- bucket policy + versioning

- `14:50:56` deny stmt: actions=['s3:DeleteObject', 's3:DeleteObjectVersion'] resource=['arn:aws:s3:::justhodl-dashboard-live/data/warm/*', 'arn:aws:s3:::justhodl-dashboard-live/data/providers/*']
## Summary

- `14:50:56` Read-only. Findings decide the build: if NCCBR/haircut series exist in the live catalog but aren't banked, the fix is convergence + a haircut panel, not a new engine; seclending result decides whether a small securities-lending pull gets added for the rehypo picture.

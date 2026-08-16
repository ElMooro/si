# ops 4763 -- justhodl-repo: wait for deploy, invoke, verify

**Status:** success  
**Duration:** 34.4s  
**Finished:** 2026-08-16T18:29:16+00:00  

## Data

| check | value |
|---|---|
| function_exists | True |
| invoke_status | 200 |
| invoke_error | None |
| invoke_secs | 33.9 |
| series_total | 296 |
| skipped | 4 |
| groups | 7 |
| barometer_score | 54.6 |
| barometer_label | NORMAL |
| dollar_series_resolved | 0 |
| history_probe_id | REPO-TRIV1_AR_OO-P |
| history_probe_n | 2958 |
| history_probe_span | 2014-08-22 -> 2026-08-03 |

## Log
## Wait for the function (parallel deploy)

- `18:28:41` ✅ function present after 0s
## Invoke synchronously

- `18:29:15` payload[:350]: {"statusCode": 200, "body": "{\"ok\": true, \"series\": 296, \"skipped\": 4, \"barometer\": 54.6, \"label\": \"NORMAL\", \"secs\": 33.4}"}
## Verify data/repo.json

- `18:29:16`   comp SOFR−TGCR spread: 2.0 bps z=0.67
- `18:29:16`   comp DVP−Triparty rate: -2.0 bps z=-0.5
- `18:29:16`   comp Corp−Tsy collateral spread: 23.0 bps z=2.5
- `18:29:16`   comp Total fails Δ30d: -0.099 % z=-0.0
- `18:29:16`   comp Treasury fails Δ30d: -0.083 % z=-0.0
- `18:29:16`   comp Overnight triparty volume Δ30d: -0.637 % z=0.08
- `18:29:16`   comp Broad dollar Δ30d: None % z=None
- `18:29:16`   group 'DVP bilateral cleared': 36 series
- `18:29:16`   group 'GCF': 44 series
- `18:29:16`   group 'Primary dealer FAILS': 18 series
- `18:29:16`   group 'Primary dealer financing': 88 series
- `18:29:16`   group 'Reference rates & volumes': 30 series
- `18:29:16`   group 'Tri-party (TRI legacy)': 40 series
- `18:29:16`   group 'Tri-party (TRIV1, ex-Fed)': 40 series
- `18:29:16`   skipped: REPO-GCF_AR_B27-F -- no_pairs_extracted
- `18:29:16`   skipped: REPO-GCF_AR_B27-P -- no_pairs_extracted
- `18:29:16`   skipped: REPO-GCF_TV_B27-F -- no_pairs_extracted
- `18:29:16`   skipped: REPO-GCF_TV_B27-P -- no_pairs_extracted
## Verify one history file + the dollar rows

- `18:29:16` ✅ engine + page data path verified end-to-end -- repo.html renders from these exact files

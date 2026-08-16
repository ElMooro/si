# ops 4762 -- soma / v2 / HFM triple-verify (on-demand ticks)

**Status:** success  
**Duration:** 66.5s  
**Finished:** 2026-08-16T18:04:54+00:00  

## Data

| catalog_generated_at | fxs_n_rows | fxs_refreshed_at | hfm_as_of | hfm_catalog | hfm_done | hfm_progress | hfm_status | invoke_cusip | invoke_deep | invoke_stfm | nyfed_research_on_page | soma_agency_dates | soma_as_of | soma_banked_this_run | soma_done_pairs | soma_errors | soma_progress_pct | soma_total_pairs | soma_tsy_dates | tsy_latest_after_fix | v2_as_of | v2_family_blocks |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | status=200 err=None dur=14.9s |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | status=200 err=None dur=14.4s |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | status=200 err=None dur=36.6s |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 2026-08-16T18:03:49+00:00 | 497 | 25 | 5.0 | converging |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-16T18:04:03+00:00 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-11 |  |  |
|  | 1596 | 2026-08-16T18:04:03+00:00 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-08-16T18:04:18+00:00 | 120 | 120 | 0 | 5.0 | 2412 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 120 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 0 |  |  |  |  |  |  |  |  |  |  |
| 2026-08-16T17:48:53+00:00 |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |

## Log
## Invoke the three engines

- `18:04:03` justhodl-ofr-stfm payload[:300]: {"statusCode": 200, "body": "{\"ok\": true, \"catalog\": 442, \"catalog_source\": \"https://data.financialresearch.gov/v1/metadata/mnemonics\", \"pulled_this_run\": 0, \"failed\": 0, \"done_total\": 442, \"progress_pct\": 100.0, \"hf\": {\"catalog\": 497, \"pulled\": 25, \"failed\": 0, \"done_total\
- `18:04:17` justhodl-nyfed-repo-deep payload[:300]: {"statusCode": 200, "body": "{\"as_of\": \"2026-08-16T18:04:03+00:00\", \"rp_reverserepo\": {\"ok\": true, \"n_ops\": 3191, \"latest_date\": \"2026-08-14\", \"latest_accepted\": 250000000, \"row_keys\": [\"acceptedCpty\", \"auctionStatus\", \"closeTime\", \"details\", \"lastUpdated\", \"maturityDate
- `18:04:54` justhodl-soma-cusip payload[:300]: {"statusCode": 200, "body": "{\"as_of\": \"2026-08-16T18:04:18+00:00\", \"asof_catalog\": 1206, \"pending_before\": 2412, \"banked_this_run\": 120, \"errors_this_run\": 0, \"done_pairs\": 120, \"total_pairs\": 2412, \"progress_pct\": 5.0, \"endpoint\": {\"tsy\": \"/api/soma/tsy/get/asof/{d}.json\"},
## HFM extension -- fresh state

- `18:04:54` hf new mnemonics last run: FICC-SPONSORED_REPO_VOL, FICC-SPONSORED_REVREPO_VOL, FPF-ALLQHF_ALTERNATE_COUNT, FPF-ALLQHF_CANGATE_PERCENT, FPF-ALLQHF_CANSUSPEND_PERCENT, FPF-ALLQHF_CDSDOWN250BPS_P5, FPF-ALLQHF_CDSDOWN250BPS_P50, FPF-ALLQHF_CDSUP250BPS_P5, FPF-ALLQHF_CDSUP250BPS_P50, FPF-ALLQHF_COUNT
- `18:04:54` ✅ HFM self-extension state is live
## repo-deep v2 -- fresh summary + tsy 'latest' fix proof

- `18:04:54`   fxs: {"ok": true, "n_rows": 1596, "added": 0, "latest": "2026-08-12"}
- `18:04:54`   ambs: {"ok": true, "n_rows": 2875, "added": 0, "latest": "2026-08-14"}
- `18:04:54`   tsy: {"ok": true, "n_rows": 1894, "added": 0, "latest": "2026-08-11"}
- `18:04:54`   seclending: {"ok": true, "n_rows": 9302, "added": 0, "latest": "2026-08-14"}
- `18:04:54`   soma_summary: {"ok": true, "n_rows": 1206, "added": 0, "latest": "2026-08-12"}
- `18:04:54` ✅ tsy 'latest' now an operation date (2026-08-11) -- the 2055 maturity artifact is retired
## soma-cusip -- fresh status + sample depth

- `18:04:54` soma endpoints: {"tsy": "/api/soma/tsy/get/asof/{d}.json"}
- `18:04:54` ✅ tsy 2026-08-12: 432 per-CUSIP rows banked
## data.html -- nyfed-research row


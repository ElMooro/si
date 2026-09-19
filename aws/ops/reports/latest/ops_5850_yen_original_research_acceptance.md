
**Status:** success  
**Duration:** 31.2s  
**Finished:** 2026-09-19T13:54:34+00:00  

## Data

| acceptance_scope | calls_eligible | cftc_all_categories_reconciled | cftc_reports | commit | contract | generated_at | history_shards | http_read_did_not_recollect | invoke_status | notifications_sent | original_fred_series | original_replay_reproduced | output_sha256 | paid_ai_calls | portfolio_writes | private_account_reads | protected_legacy_verified | quality | replay | research_generated_at | retained_observations | runtimes | sizing_eligible | source_errors | throttle_rejections |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {'justhodl-yen-carry': {'commit': '5a8e801aff9492647f6204e9118f5096a3768ada', 'code_sha256': 'B/mAsEdoqDR/46t3KfgM/x3N7h+MyumAJ5DpJ0Raw38='}, 'justhodl-bond-desk': {'commit': '5a8e801aff9492647f6204e9118f5096a3768ada', 'code_sha256': 'j3srzQFB0pXb9hZIXC+6XhZXWkfTFxJm3riX/gR5AEs='}} |  |  |  |
|  |  |  |  |  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 |
| Two exact runtimes; yen collector and HTTP read invoked. Bond Desk abstention guard tested offline, not invoked. Research and entered scenarios only; no qualified strategy. | False | True | 1058 | 5a8e801aff9492647f6204e9118f5096a3768ada | yen-research-verification.v1 | 2026-09-19T13:54:34.792127+00:00 | 10 | True |  | 0 | 7 | True | f450e219baad53f69474b93616748185f14e065ec6c92e00f2b25346a13ff81e | 0 | 0 | 0 | True | {'status': 'fresh', 'states': {'fresh': 8}, 'required_source_families': 8, 'available_source_families': 8, 'basis': 'Per-series observation and acquisition clocks. Daily H.10 fixings use nominal weekly release cadence; no verified holiday calendar.'} | {'manifest_key': 'data/yen-research/runs/82279795ced5ecb5347a733682117b309298fe2ba5b5e0318f75bf0647406094.json', 'output_sha256': 'f450e219baad53f69474b93616748185f14e065ec6c92e00f2b25346a13ff81e'} | 2026-09-19T13:54:23.113205+00:00 | 33262 | {'justhodl-yen-carry': {'commit': '5a8e801aff9492647f6204e9118f5096a3768ada', 'code_sha256': 'B/mAsEdoqDR/46t3KfgM/x3N7h+MyumAJ5DpJ0Raw38='}, 'justhodl-bond-desk': {'commit': '5a8e801aff9492647f6204e9118f5096a3768ada', 'code_sha256': 'j3srzQFB0pXb9hZIXC+6XhZXWkfTFxJm3riX/gR5AEs='}} | False | {} |  |

## Log


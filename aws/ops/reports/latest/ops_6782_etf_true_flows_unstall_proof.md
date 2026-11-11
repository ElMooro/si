# ops 6782 - justhodl-etf-true-flows 3.0.1 un-stall proof 2026-11-11T06:13+00:00

**Status:** success  
**Duration:** 382.2s  
**Finished:** 2026-11-11T06:20:15+00:00  

## Data

| generated_at | identity_defects | invoke_seconds | native_histories | output_moved | published | quality | run_manifests | statusCode | tlt | vendor_context |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-11T06:16:45 | {} | 382 | 81/128 | True | True | {"aligned_five_observation_estimates": 81, "all_funds_covered": false, "call": null, "calls_eligible": false, "configured_funds": 128, "execution_eligible": false, "native_histories": 81, "native_obse | 23 -> 24 | 200 | retained_native_history 2026-11-10 flow5d 670984116.5 | None |

## Log
- `06:13:53` before: data/etf-true-flows.json LastModified 2026-10-11T15:52+00:00 (1036429 B)
- `06:13:53` function: code last modified 2026-11-11T06:12:16.000+0000, sha X2a3Dp6fM+BF, timeout 900s
## invoke once (RequestResponse)

- `06:20:14` invoke: status 200, function error None, 382s
- `06:20:14` response: statusCode 200, body {"generated_at": "2026-11-11T06:16:45.789434+00:00", "history_shards": 82, "notifications_sent": 0, "paid_ai_calls": 0, "portfolio_writes": 0, "private_account_reads": 0, "published": true, "quality": {"aligned_five_observation_estimates": 81, "all_funds_covered": false, "call": null, "calls_eligible": false, "configured_funds": 128, "execution_eligible": false, "native_histories": 81, "native_observations": 432185, "sizing_eligible": false, "status": "partial"}, "replay": {"manifest_key": "data/etf-research/runs/dc344e01adf00277bff4c9e5fe80160f9779f7eb90850bcf0216cc6f7cae1928.json", "output_sha256": "43a82ad08230e6eb54e3713b3a37d9dace39b627a20a620569b37d79ddb20b9d"}, "signals_emitted": 0, "source_status_codes": {"vendor_VCSH_quote": "TimeoutError", "vendor_VEA_info": "TimeoutError", "vendor_VEA_quote": "TimeoutError", "vendor_VEA_shares": "TimeoutError", "vendor_VGIT_info": "TimeoutErro
- `06:20:14` source_status_codes: 82 keys; non-vendor: {}; vendor 429: 0; vendor skipped: 0
## verify from S3

- `06:20:15` after: data/etf-true-flows.json LastModified 2026-11-11T06:20+00:00 (949433 B) -> MOVED
- `06:20:15` payload: contract etf-original-research.v1 | generated_at 2026-11-11T06:16:45.789434+00:00 | reference latest 2026-11-10 | quality partial | native_histories 81/128 | aligned 81 | gaps 47
- `06:20:15` TLT: source_status retained_native_history | nav_source iShares_NATIVE_HISTORY | observation_date 2026-11-10 | nav 80.85681 | shares 588400000.0 | net_flow_5d_usd 670984116.5 | history d3b004acf9c1c2f.json | source_error None
- `06:20:15` IVV: retained_native_history obs 2026-11-10 net_flow_5d_usd 7039926548.8 | top inflows [('IVV', 7039926548.8), ('IWM', 1507917819.8), ('TQQQ', 1449632175.0), ('XLK', 1393163934.05), ('SGOV', 1139630060.25)] | top outflows [('SPY', -6816918683.9), ('SSO', -1288308320.0), ('IBIT', -1201044007.84), ('XLF', -1002033282.4), ('XLC', -924100103.45)]
- `06:20:15` run manifests: 23 -> 24; replay ref {"manifest_key": "data/etf-research/runs/dc344e01adf00277bff4c9e5fe80160f9779f7eb90850bcf0216cc6f7cae1928.json", "output_sha256": "43a82ad08230e6eb54e3713b3a37d9dace39b627a20a620569b37d79ddb20b9d"}
- `06:20:15` ✅ un-stalled: a fresh etf-original-research.v1 packet published today with TLT's native history back

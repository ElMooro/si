# ops 6903 - justhodl-nyfed-pd 4.1.0: one run and its snapshot reproduced, 2026-11-22T04:27+00:00

**Status:** success  
**Duration:** 27.7s  
**Finished:** 2026-11-22T04:27:42+00:00  

## Data

| as_of_after | as_of_before | reproduced | reverse_repo_last_published | run_seconds | treasury_repo_out_b | version | withheld_series |
|---|---|---|---|---|---|---|---|
| 2026-11-11 | 2026-10-28 | True | 2026-10-28 3002182 | 23 | 3398.38 | 4.1.0 | 8 |

## Log
## 1. What is deployed

- `04:27:15` justhodl-nyfed-pd updated 2026-11-22T04:26:15 | modules as in this checkout: {'dealer_original.py': True, 'dealer_research.py': True, 'dealer_research_store.py': True} | timeout 420 s
## 2. One run

- `04:27:15` before: written 2026-11-06 21:36 | version 4.0.0 | as_of 2026-10-28 | quality {"definition_unverified": 14, "fresh": 47, "incomplete": 9}
- `04:27:38` invoke: HTTP 200, engine status 200, 23 s | {"published": true, "generated_at": "2026-11-22T04:27:27.378012+00:00", "replay": {"manifest_key": "data/dealer-research/runs/0d37b60fb89090171117af2d2bbe6844e90e3c80382aa2a23bbdb8c81556798a.json", "output_sha256": "344ac5c7f296046a15a2cd53ce959431a7a0be05b50b757f6688d852d4129106"}, "quality": {"status": "degraded", "counts": {"definition_unverified": 14, "fresh": 49, "incomplete": 7}, "total_series": 70}, "original_series": 70, "retained_observations": 46381, "signals_emitted": 0, "paid_ai_calls": 0, "notifications_sent": 0, "private_account_reads": 0, "portfolio_writes": 0}
## 3. Read back

- `04:27:39` after: written 2026-11-22 04:27 | version 4.1.0 | generated_at 2026-11-22T04:27:27.378012+00:00 | source_generated_at 2026-11-22T04:27:21.984820+00:00 | as_of 2026-11-11 | quality {"counts": {"definition_unverified": 14, "fresh": 49, "incomplete": 7}, "status": "degraded", "total_series": 70}
- `04:27:39` 70 series | newest report dates {'2026-11-11': 70} | original responses {'observations_1': 14, 'observations_2': 14, 'observations_3': 14, 'observations_4': 14, 'observations_5': 14} | sources ['observations_1', 'observations_2', 'observations_3', 'observations_4', 'observations_5']
- `04:27:39` Treasury financing 2026-11-11: repo out 3398.38 | reverse repo in None | two-sided None
- `04:27:39`   treasury_repo          2026-11-11: 3398380 USD mn, fresh (missing []) | last published week ['2026-11-11', 3398380] | parts {'PDSORA-UTSETTOT': 'observed 3118983', 'PDSORA-UTSTTOT': 'observed 279397'}
- `04:27:39`   treasury_reverse_repo  2026-11-11: None USD mn, incomplete (missing ['latest_report_suppressed_or_missing']) | last published week ['2026-10-28', 3002182] | parts {'PDSIRRA-UTSETTOT': 'suppressed None', 'PDSIRRA-UTSTTOT': 'observed 226721'}
- `04:27:39` series whose latest week the New York Fed did not publish: 8 ['PDSIOSB-FGMTOT', 'PDSIOSB-OTOT', 'PDSIRRA-ETOT', 'PDSIRRA-FGMTOT', 'PDSIRRA-UTSETTOT', 'PDSOOS-FGEMTOT', 'PDSOOS-OTOT', 'PDTRMBS-TOT']
- `04:27:39` values the source changed since the previous snapshot (2026-11-06): 25
- `04:27:39` corporate bonds 2026-11-11: net 6.976 (fresh)
- `04:27:42` replay from the 13 retained original responses: reproduced True (output sha256 344ac5c7f296046a)
- `04:27:42` ✅ published: the week of 2026-11-11, reproduced from its original responses

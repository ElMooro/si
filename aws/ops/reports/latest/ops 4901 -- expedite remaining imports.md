# ops 4901 — expedite: deep chain · OECD probe · MIDAS discovery

**Status:** success  
**Duration:** 910.3s  
**Finished:** 2026-08-19T14:15:48+00:00  

## Data

| denied_total | hard_403 | mode | n_complete_before | n_complete_now | other | other_detail | results | runs_observed | sampled | stage | unlock_detail | unlockable | v12 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | deep-settle |  |  | True |
|  |  | backfill | 30 | 30 |  |  |  | 1 |  | deep-chain |  |  |  |
| 0 |  |  |  |  |  |  |  |  | 0 | oecd-ledger |  |  |  |
|  | 0 |  |  |  | 0 |  |  |  |  | oecd-probe |  | 0 |  |
|  |  |  |  |  |  |  | {"metrics-page": {"status": 200, "bytes_head": 65536}, "data-page": {"status": 200, "bytes_head": 65536}, "mstr-q-2026q1": {"status": 404, "bytes_head": 0}, "mstr-q-2025q4": {"status": 404, "bytes_head": 0}, "mstr-sec-2025q4": {"status": 404, "bytes_head": 0}, "mstr-index": {"status": 404, "bytes_head": 0}} |  |  | midas-discovery |  |  |  |

## Log
- `14:15:48` VERDICT: PASS_WITH_PENDING · {"deep_chain_live": "PENDING", "oecd_probe": "PASS", "midas_discovery": "PASS"}
- `14:15:48` report written: aws/ops/reports/4901.json

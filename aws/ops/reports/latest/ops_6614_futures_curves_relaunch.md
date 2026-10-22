# ops 6614 - lane manifest, deploy check, futures-curves first paced run, fleet-integrity re-run

**Status:** success  
**Duration:** 319.7s  
**Finished:** 2026-10-22T16:47:18+00:00  

## Data

| d4_rows | datasets | futures_published | futures_quality | futures_status | integrity_new | integrity_version | stops |
|---|---|---|---|---|---|---|---|
|  | 42 | True | {"complete_datasets": 42, "products": 7, "selected_contracts": 21, "status": "descriptive"} | complete |  |  | {"complete_returned_pagination": 42} |
| 0 |  |  |  |  | 248 | 1.2.1 |  |

## Log
## 1. S3 fan-out manifest := committed

- `16:41:59` tick differences S3 -> repo (removed, added): {"session-5min": [["justhodl-polygon-futures-curves"], []]}
- `16:41:59` ✅ fan-out manifest uploaded: session-5min now 11 members (backup backups/fanout-manifest/20261022T164159Z/)
## 2. the deploy landed

- `16:42:00` justhodl-polygon-futures-curves modified 2026-10-22T16:35:15 timeout 900 -> new code: True; justhodl-fleet-integrity modified 2026-10-22T16:35:40 -> new code: True
## 3. futures-curves: one paced run

- `16:42:00` public head before: last modified 2026-10-10 13:20:18
- `16:42:00` async invoke status 202 at 16:42:00
- `16:45:33` request record 3ccab0304d2e1c66: status complete phase complete published True error None failure_class None started 2026-10-22T16:35:30 completed 2026-10-22T16:45:02
- `16:45:33` quality: {"complete_datasets": 42, "products": 7, "selected_contracts": 21, "status": "descriptive"}; provider requests this execution: 46
- `16:45:35` dataset checkpoints: 42; stops: {"complete_returned_pagination": 42}
- `16:45:35` public head after: last modified 2026-10-22 16:45:03
## 4. fleet-integrity re-run

- `16:47:18` fleet-integrity invoke: error None; body {"ok": true, "n_defects": 269, "n_new": 248, "n_fixed": 37, "sev1": 11}
- `16:47:18` fleet-integrity.json generated 2026-10-22T16:47:18.666347+00:00 version 1.2.1: new 248 by class [["D8_errors", 174], ["D7_concurrency_drop", 57], ["D2_timeout_clipped", 7], ["D14_unscanned", 6], ["D6_scheduled_never_ran", 4]]; D4 rows now 0
- `16:47:18` ✅ futures-curves complete and published under the paced run; fleet-integrity v1.2.1 reports 0 orphan rules

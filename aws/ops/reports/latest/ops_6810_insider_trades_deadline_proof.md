# ops 6810 - justhodl-insider-trades wall-clock deadline proof 2026-11-14 (ladder, then one scheduled-path Event invocation)

**Status:** success  
**Duration:** 622.0s  
**Finished:** 2026-11-14T11:45:28+00:00  

## Data

| atom_phase_cut | backfill_days_complete | cluster_count | deployed_last_modified | duration_s | fetch_duration_s | fetch_errors | filings_scanned | output_age_before_h | report_line | stopped_at_deadline | timeout_s | total_buys | written_after_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None | 85 | 4 | 2026-11-14T11:41:44.000+0000 | 198.8 | 197.4 | 84 | 350 | 62.4 | ok | False | 300 | 403 | 200 |

## Log
## 1. the three-rung ladder

- `11:35:06` rung 1: repo has the marker
- `11:35:07`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:35:22`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:35:37`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:35:53`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:36:08`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:36:23`   attempt 6: marker False, State Active / Successful - waiting 15 s
- `11:36:39`   attempt 7: marker False, State Active / Successful - waiting 15 s
- `11:36:54`   attempt 8: marker False, State Active / Successful - waiting 15 s
- `11:37:09`   attempt 9: marker False, State Active / Successful - waiting 15 s
- `11:37:25`   attempt 10: marker False, State Active / Successful - waiting 15 s
- `11:37:40`   attempt 11: marker False, State Active / Successful - waiting 15 s
- `11:37:55`   attempt 12: marker False, State Active / Successful - waiting 15 s
- `11:38:11`   attempt 13: marker False, State Active / Successful - waiting 15 s
- `11:38:26`   attempt 14: marker False, State Active / Successful - waiting 15 s
- `11:38:41`   attempt 15: marker False, State Active / Successful - waiting 15 s
- `11:38:57`   attempt 16: marker False, State Active / Successful - waiting 15 s
- `11:39:12`   attempt 17: marker False, State Active / Successful - waiting 15 s
- `11:39:27`   attempt 18: marker False, State Active / Successful - waiting 15 s
- `11:39:43`   attempt 19: marker False, State Active / Successful - waiting 15 s
- `11:39:58`   attempt 20: marker False, State Active / Successful - waiting 15 s
- `11:40:13`   attempt 21: marker False, State Active / Successful - waiting 15 s
- `11:40:29`   attempt 22: marker False, State Active / Successful - waiting 15 s
- `11:40:44`   attempt 23: marker False, State Active / Successful - waiting 15 s
- `11:40:59`   attempt 24: marker False, State Active / Successful - waiting 15 s
- `11:41:15`   attempt 25: marker False, State Active / Successful - waiting 15 s
- `11:41:30`   attempt 26: marker False, State Active / Successful - waiting 15 s
- `11:41:45` rung 2+3 at attempt 27: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-14T11:41:44.000+0000, timeout 300 s, memory 512 MB
## 2. the scheduled path, once (Event invocation; result read back from S3 and the log)

- `11:41:51` before: data/insider-trades.json LastModified 2026-11-11T21:11:46+00:00 (62.4 h old)
- `11:41:51` backfill cursor before: 92 days tracked, 85 complete, updated 2026-11-11T21:10:20.243400+00:00
- `11:41:51` invoked at 2026-11-14T11:41:51+00:00: HTTP 202
- `11:45:13` after: data/insider-trades.json LastModified 2026-11-14T11:45:11+00:00 (200 s after the invoke)
- `11:45:13` stats: generated_at 2026-11-14T11:45:09+00:00 version 3.0 | fetch_duration_s 197.4 fetch_errors 84 atom_feed_count 100 | diagnostics {"buys_kept": 0, "filings_seen": 100, "no_issuer_or_ticker": 0, "no_nonderivative_table": 0, "sells_kept": 38, "skipped_below_threshold": 0, "skipped_not_buy_or_sell": 12, "txn_seen": 50, "xml_fetched": 16, "xml_parse_failed": 0}
- `11:45:13` fetch errlog (per error type): {"http_404_html": 84, "http_404_index": 84}
- `11:45:13` coverage: {"backfill_days_complete": 85, "backfill_days_target": 30, "filings_scanned_this_run": 350, "sector_mapped": 3105, "stopped_at_deadline": false, "total_leaves": 63033, "universe_size": 4977}
- `11:45:13` counts: buys 403 | sells 2860 | clusters 4 | big_buys 50 | total_buys 403 | fleet ledger [{"feed": "radar", "status": "ok", "age_h": 21.1}, {"feed": "industry_cluster", "status": "ok", "age_h": 21.4}, {"feed": "sell_cluster", "status": "ok", "age_h": 12.2}, {"feed": "aggregate", "status": "stale", "age_h": 277.2}, {"feed": "buyback_confluence", "status": "ok", "age_h": 12.7}]
- `11:45:13` data/insider-trades-full.json LastModified 2026-11-14T11:45:11+00:00 (rewritten)
- `11:45:28`   log: fetch deadline in 224 s (function remaining minus the merge/write reserve)
- `11:45:28`   log: Atom feed returned 100 Form 4 filings
- `11:45:28`   log: Parsed 38 kept transactions; fetch_errors=84; errlog={"http_404_index": 84, "http_404_html": 84}
- `11:45:28`   log: backfill: +268 txns, {'filings_scanned_this_run': 350, 'backfill_days_complete': 85, 'backfill_days_target': 30, 'stopped_at_deadline': False}
- `11:45:28`   log: Wrote 1682931 bytes to s3://justhodl-dashboard-live/data/insider-trades.json
- `11:45:28`   REPORT RequestId: a1721430-bd08-46d7-a38f-20ebb18ef970 Duration: 198770.21 ms Billed Duration: 199120 ms Memory Size: 512 MB Max Memory Used: 114 MB Init Duration: 349.64 ms XRAY TraceId: 1-6af848ff-08c6fbb87eb16b3c388735bb SegmentId: 673902f8a66f72dd Sampled: true
- `11:45:28` ✅ data/insider-trades.json rewritten 200 s after the invoke; fetch 197.4 s, fetch_errors 84, atom cut None, backfill stopped at deadline False (350 filings scanned, 85/30 days complete); REPORT ok

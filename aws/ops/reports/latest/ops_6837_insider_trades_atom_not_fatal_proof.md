# ops 6837 - justhodl-insider-trades writes through a failed EDGAR atom feed (live proof after the deploy)

**Status:** success  
**Duration:** 284.9s  
**Finished:** 2026-11-18T11:33:39+00:00  

## Data

| atom_feed_count | atom_feed_error | coverage | deploy_landed | fetch_duration_s | fetch_errors | generated_at | sell_transactions | total_buys | transactions | version | wrote |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | HTTPError: HTTP Error 400: Bad Request | {"filings_scanned_this_run": 350, "backfill_days_complete": 85, "backfill_days_target": 30, "stopped_at_deadline": false, "sector_mapped": 3161, "universe_size": 4972, "leaves_by_section": {"generated_at": 1, "window_days": 1, "cluster_window_days": 1, "thresholds": 3, "stats": 20, "version": 1, "fleet": 6836, "clusters": 78, "big_buys": 458, "transactions": 6880, "sell_transactions": 45916, "surf | True | 31.8 | 0 | 2026-11-18T11:33:25+00:00 | 2879 | 440 | 440 | 3.0 | True |

## Log
## 1. the deploy

- `11:28:54` at start: CodeSha256 sNCozo2R9LIug9RQsudnzz2t+715VO/hPBjN2D1hfwI= LastModified 2026-11-14T11:41:51.000+0000 LastUpdateStatus Successful State Active
- `11:32:37` ✅ deploy landed after 222 s: CodeSha256 +skaaeb8OvVD0ND5GntAX6KTCrQ758DOrTaMxjFWb6U= LastModified 2026-11-18T11:32:32.000+0000
## 2. one Event invoke, then the write

- `11:32:52` data/insider-trades.json before: LastModified 2026-11-15T21:13:51+00:00 (1706188 B)
- `11:32:52` data/insider-trades-full.json before: LastModified 2026-11-15T21:13:51+00:00 (1378875 B)
- `11:32:52` data/_insider/backfill-cursor.json before: LastModified 2026-11-15T21:13:50+00:00 (6777 B)
- `11:32:52` invoke: HTTP 202 at 2026-11-18T11:32:52+00:00
- `11:33:38` ✅ data/insider-trades.json written 46 s after the invoke
- `11:33:38` data/insider-trades.json after: LastModified 2026-11-18T11:33:27+00:00 (1679732 B)
- `11:33:38` data/insider-trades-full.json after: LastModified 2026-11-18T11:33:27+00:00 (1400620 B)
- `11:33:38` data/_insider/backfill-cursor.json after: LastModified 2026-11-18T11:33:26+00:00 (6838 B)
## 3. the new document and the log tail

- `11:33:39` generated_at 2026-11-18T11:33:25+00:00 version 3.0 | atom_feed_count 0 | atom_feed_error HTTPError: HTTP Error 400: Bad Request
- `11:33:39` stats: total_buys 440 unique_companies 190 clusters 3 big_buys 50 fetch_errors 0 fetch_duration_s 31.8
- `11:33:39` coverage: {"filings_scanned_this_run": 350, "backfill_days_complete": 85, "backfill_days_target": 30, "stopped_at_deadline": false, "sector_mapped": 3161, "universe_size": 4972, "leaves_by_section": {"generated_at": 1, "window_days": 1, "cluster_window_days": 1, "thresholds": 3, "stats": 20, "version": 1, "fleet": 6836, "clusters": 78, "big_buys": 458, "transactions": 6880, "sell_transactions": 45916, "surf
- `11:33:39` transactions (buys) 440 | sell_transactions 2879
- `11:33:39` ⚠ EDGAR's getcurrent atom feed is still failing (HTTPError: HTTP Error 400: Bad Request) - the daily-index backfill carried the run; the atom path itself still wants a look
- `11:33:39`   log 11-18 11:32:52: INIT_START Runtime Version: python:3.12.mainlinev2.v86	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:9d15a516fef11563a8c452bd8d3fe5a5a120b73013e74783e925235557467534
- `11:33:39`   log 11-18 11:32:53: fetch deadline in 224 s (function remaining minus the merge/write reserve)
- `11:33:39`   log 11-18 11:32:54: atom feed fetch failed: HTTPError: HTTP Error 400: Bad Request - continuing with the daily-index backfill alone
- `11:33:39`   log 11-18 11:32:54: Atom feed returned 0 Form 4 filings
- `11:33:39`   log 11-18 11:32:54: DIAG: {"filings_seen": 0, "xml_fetched": 0, "xml_parse_failed": 0, "no_issuer_or_ticker": 0, "no_nonderivative_table": 0, "txn_seen": 0, "skipped_not_buy_or_sell": 0, "skipped_below_threshold": 0, "buys_kept": 0, "sells_kept": 0, "atom_feed_error": "HTTPError: HTTP Error 400: Bad Request"}
- `11:33:39`   log 11-18 11:32:54: Parsed 0 kept transactions; fetch_errors=0; errlog={}
- `11:33:39`   log 11-18 11:33:25: backfill: +173 txns, {'filings_scanned_this_run': 350, 'backfill_days_complete': 85, 'backfill_days_target': 30, 'stopped_at_deadline': False}
- `11:33:39`   log 11-18 11:33:25: After merge+window: 3319 transactions in window
- `11:33:39`   log 11-18 11:33:26: Wrote 1679732 bytes to s3://justhodl-dashboard-live/data/insider-trades.json
- `11:33:39`   log 11-18 11:33:26: Stats: {"total_buys": 440, "total_value_usd": 10611713976.02, "unique_companies": 190, "unique_insiders": 226, "cluster_count": 3, "big_buy_count": 50, "fetch_errors": 0, "fetch_duration_s": 31.8, "diagnostics": {"filings_seen": 0, "xml_fetched": 0, "xml_parse_failed": 0, "no_issuer_or_ticker": 0, "
- `11:33:39`   log 11-18 11:33:26: REPORT RequestId: d92c6921-ae92-4ef5-97f0-4a29686e0338	Duration: 33181.02 ms	Billed Duration: 33539 ms	Memory Size: 512 MB	Max Memory Used: 112 MB	Init Duration: 357.19 ms	 XRAY TraceId: 1-6afd8ce4-0d5776197c0be46c40d58109	SegmentId: 1bd46ff926a69fb5	Sampled: true
- `11:33:39` ✅ insider-trades writes again; nothing else changed

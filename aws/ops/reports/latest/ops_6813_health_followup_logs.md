# ops 6813 - health follow-up logs 2026-11-16: auction-crisis-detector (3/3 errors Nov 14 + 15), portwatch CaptureError, sec-8k-enrich cik map >7 d (read-only)

**Status:** success  
**Duration:** 7.7s  
**Finished:** 2026-11-16T11:22:50+00:00  

## Data

| 8k-by-ticker.json_age_h | 8k-filings-enriched.json_age_h | auction-crisis-detector_errors_7d | auction-crisis-detector_outcomes | auction-crisis.json_age_h | daily-rows.json.gz_age_h | portwatch.json_age_h | portwatch_errors_7d | portwatch_outcomes | sec-8k-enrich_errors_7d | sec-8k-enrich_outcomes | sec-cik-ticker-map.json_age_h |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 23.2 | 23.2 | 6 | {"incomplete": 2, "unhandled": 3} | 63.5 | 72.0 | 72.0 | 0 | {"incomplete": 2} | 0 | {"clean": 1, "incomplete": 1} | 191.2 |

## Log
## auction-crisis-detector

- `11:22:43` config: timeout 240 s | memory 1024 MB | LastModified 2026-10-08T21:44:11.000+0000 | state Active/Successful | runtime python3.12
- `11:22:44` EventBridge rules: rule justhodl-auction-crisis-active ENABLED cron(50 19 * * ? *); rule justhodl-auction-crisis-backstop DISABLED cron(5 13 * * ? *)
- `11:22:44` metrics 7 d: 11-09 n1 e0 t0, 11-10 n1 e0 t0, 11-11 n1 e0 t0, 11-12 n1 e0 t0, 11-13 n1 e0 t0, 11-14 n3 e3 t0, 11-15 n3 e3 t0
- `11:22:45` data/auction-crisis.json: 63.5 h old (2725892 B)
- `11:22:45` stream 53ed421c913a15ceda4c206d: 1 events, 1 invocation(s), last event 11-15 19:57
- `11:22:45`   invocation 11-15 19:57 -> incomplete (1 lines)
- `11:22:46` stream 21d44a6da9b81d8686bd1a2d: 16 events, 4 invocation(s), last event 11-15 19:54
- `11:22:46`   invocation 11-15 19:50 -> unhandled (5 lines)
- `11:22:46`     [auction-crisis] start 2026-11-15T19:50:31.109413+00:00
- `11:22:46`     [ERROR] RuntimeError: Auction acquisition incomplete at page 1; previous publication preserved (TimeoutError) Traceback (most recent call last): File "/var/task/lambda_function.py", line 470, in lambda_handler raw = fetch_fiscal_auctions(start.isoformat(), end.isoformat(), pages_out=original_pages) 
- `11:22:46`     REPORT RequestId: 14e93ecc-0811-4d2b-9e0f-ba78e8ee9272 Duration: 20071.88 ms Billed Duration: 20631 ms Memory Size: 1024 MB Max Memory Used: 94 MB Init Duration: 558.43 ms XRAY TraceId: 1-6afa0d06-3c8662cd2dd0b9a03457eaa8 SegmentId: 45354ed50c9ad6bc Sampled: true
- `11:22:46`   invocation 11-15 19:51 -> unhandled (5 lines)
- `11:22:46`     [auction-crisis] start 2026-11-15T19:51:48.360499+00:00
- `11:22:46`     [ERROR] RuntimeError: Auction acquisition incomplete at page 1; previous publication preserved (TimeoutError) Traceback (most recent call last): File "/var/task/lambda_function.py", line 470, in lambda_handler raw = fetch_fiscal_auctions(start.isoformat(), end.isoformat(), pages_out=original_pages) 
- `11:22:46`     REPORT RequestId: 14e93ecc-0811-4d2b-9e0f-ba78e8ee9272 Duration: 20051.14 ms Billed Duration: 20052 ms Memory Size: 1024 MB Max Memory Used: 95 MB XRAY TraceId: 1-6afa0d06-3c8662cd2dd0b9a03457eaa8 SegmentId: ff3dc80b34013699 Sampled: true
- `11:22:46`   invocation 11-15 19:53 -> unhandled (5 lines)
- `11:22:46`     [auction-crisis] start 2026-11-15T19:53:58.588733+00:00
- `11:22:46`     [ERROR] RuntimeError: Auction acquisition incomplete at page 1; previous publication preserved (TimeoutError) Traceback (most recent call last): File "/var/task/lambda_function.py", line 470, in lambda_handler raw = fetch_fiscal_auctions(start.isoformat(), end.isoformat(), pages_out=original_pages) 
- `11:22:46`     REPORT RequestId: 14e93ecc-0811-4d2b-9e0f-ba78e8ee9272 Duration: 20033.07 ms Billed Duration: 20034 ms Memory Size: 1024 MB Max Memory Used: 95 MB XRAY TraceId: 1-6afa0d06-3c8662cd2dd0b9a03457eaa8 SegmentId: 18be26321a687bfe Sampled: true
- `11:22:47` stream f9e14f0fb7e71a6faf335cc4: 1 events, 1 invocation(s), last event 11-14 19:54
- `11:22:47`   invocation 11-14 19:54 -> incomplete (1 lines)
## portwatch

- `11:22:47` config: timeout 600 s | memory 1024 MB | LastModified 2026-09-27T12:02:28.000+0000 | state Active/Successful | runtime python3.12
- `11:22:47` EventBridge rules: none (Scheduler schedules not listed here)
- `11:22:48` metrics 7 d: 11-09 n1 e0 t0, 11-10 n1 e0 t0, 11-11 n1 e0 t0, 11-12 n1 e0 t0, 11-13 n1 e0 t0, 11-14 n1 e0 t0
- `11:22:48` data/portwatch.json: 72.0 h old (8785205 B)
- `11:22:48` data/warm/portwatch/history/daily-rows.json.gz: 72.0 h old (2697208 B)
- `11:22:49` stream ead445919170e51a7433524b: 2 events, 2 invocation(s), last event 11-16 11:20
- `11:22:49`   invocation 11-16 11:20 -> incomplete (1 lines)
- `11:22:49`   invocation 11-16 11:20 -> incomplete (1 lines)
## sec-8k-enrich

- `11:22:49` config: timeout 600 s | memory 512 MB | LastModified 2026-10-02T01:07:08.000+0000 | state Active/Successful | runtime python3.12
- `11:22:49` EventBridge rules: rule justhodl-sec-8k-enrich-schedule DISABLED cron(10 12 * * ? *)
- `11:22:50` metrics 7 d: 11-09 n1 e0 t0, 11-10 n1 e0 t0, 11-11 n1 e0 t0, 11-12 n1 e0 t0, 11-13 n1 e0 t0, 11-14 n1 e0 t0, 11-15 n1 e0 t0
- `11:22:50` data/8k-by-ticker.json: 23.2 h old (359399 B)
- `11:22:50` data/8k-filings-enriched.json: 23.2 h old (1158799 B)
- `11:22:50` data/sec-cik-ticker-map.json: 191.2 h old (150379 B)
- `11:22:50` stream 38284da79180294d6f332f94: 6 events, 2 invocation(s), last event 11-15 12:11
- `11:22:50`   invocation 11-15 12:10 -> incomplete (1 lines)
- `11:22:50`   invocation 11-15 12:10 -> clean (5 lines)
- `11:22:50`     [8k-enrich] CIK map cache hit (8004 entries, 7.0d old)
- `11:22:50`     [8k-enrich] 513 new, 300 reused, 460 tickers, 236 fulltext, 13 red flags
- `11:22:50`     REPORT RequestId: 556af9a1-188b-4405-a5b3-6ca7e43cc656 Duration: 15669.55 ms Billed Duration: 16160 ms Memory Size: 512 MB Max Memory Used: 113 MB Init Duration: 489.83 ms XRAY TraceId: 1-6af9a148-3b5a17fa5cfe319d62eaacca SegmentId: 2feeeee93c2ad432 Sampled: true
- `11:22:50` ✅ read-only; nothing changed, nothing invoked

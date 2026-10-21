# ops 6598 - chart backbone (symdir concurrency) + refresh of the freeze-stale chart engines

**Status:** success  
**Duration:** 668.7s  
**Finished:** 2026-10-21T14:37:06+00:00  

## Data

| after | before | note | step |
|---|---|---|---|
| 30 | 2 | backup backups/url-caps/ops6598-20261021T142559Z.json | A symdir reserved concurrency |
| refreshed data/cryptoquant-onchain.json, data/cryptoquant-series.json | stale | justhodl-cryptoquant | B refresh |
| not yet | stale | justhodl-dark-pool | B refresh |
| refreshed data/dix.json | stale | justhodl-dix | B refresh |
| refreshed data/gf-value.json | stale | justhodl-gf-value | B refresh |
| refreshed data/etf-derived.json, data/etf-desk.json | stale | justhodl-etf-global-desk | B refresh |
| refreshed data/squeeze-pretrigger.json | stale | justhodl-squeeze-pretrigger | B refresh |
| justhodl-edgar-insiders | - | ru3djltl | D url owner |

## Log
## A. justhodl-symdir reserved concurrency

- `14:25:59` governor stage NORMAL; ledger entries naming symdir: []; anomaly_held: []
- `14:25:59` before: reserved 2; memory 6144 MB; timeout 900 s; worst-case flood at 30 concurrent ~ $10.80/hour
- `14:26:02` ✅ justhodl-symdir reserved concurrency 2 -> 30
## B. one on-demand run of each freeze-stale chart writer

- `14:26:03` justhodl-cryptoquant: async invoke status 202 (payload keys []); outputs before: {'data/cryptoquant-onchain.json': '10-16 21:07', 'data/cryptoquant-series.json': '10-16 21:07'}
- `14:26:03` justhodl-dark-pool: async invoke status 202 (payload keys []); outputs before: {'data/dark-pool.json': '10-16 21:41'}
- `14:26:04` justhodl-dix: async invoke status 202 (payload keys []); outputs before: {'data/dix.json': '10-16 22:00'}
- `14:26:04` justhodl-gf-value: async invoke status 202 (payload keys []); outputs before: {'data/gf-value.json': '10-16 22:00'}
- `14:26:05` justhodl-etf-global-desk: async invoke status 202 (payload keys []); outputs before: {'data/etf-derived.json': '10-16 04:58', 'data/etf-desk.json': '10-16 04:58'}
- `14:26:05` justhodl-squeeze-pretrigger: async invoke status 202 (payload keys []); outputs before: {'data/squeeze-pretrigger.json': '10-16 05:26'}
- `14:34:16` ✅ justhodl-cryptoquant refreshed: [('data/cryptoquant-onchain.json', '14:28:37'), ('data/cryptoquant-series.json', '14:28:36')]
- `14:34:16` ⚠ justhodl-dark-pool: outputs not rewritten within 8 min (still running, or it failed - its log will say; the scheduled run tonight stands)
- `14:34:16` ✅ justhodl-dix refreshed: [('data/dix.json', '14:26:06')]
- `14:34:16` ✅ justhodl-gf-value refreshed: [('data/gf-value.json', '14:26:27')]
- `14:34:16` ✅ justhodl-etf-global-desk refreshed: [('data/etf-derived.json', '14:29:55'), ('data/etf-desk.json', '14:29:54')]
- `14:34:16` ✅ justhodl-squeeze-pretrigger refreshed: [('data/squeeze-pretrigger.json', '14:26:09')]
- `14:34:30` errors logged in the last hour by the re-run engines: {"justhodl-cryptoquant": ["[derived puell] HTTP Error 403: Forbidden "], "justhodl-dark-pool": ["[ERROR] ValueError: Request already attempted; inspect retained status instead of recollecting Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 46, in lambda_handler \u00a0\u00a0\u00a0\u00a0", "[ERROR] ValueError: Request already attempted; inspect retained status instead of recollecting Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 46, in lambda_handler \u00a0\u00a0\u00a0\u00a0", "[ERROR] ValueError: One or more planned sources failed; current head retained Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 46, in lambda_handler \u00a0\u00a0\u00a0\u00a0result=producer.r"], "justhodl-dix": ["telegram err: HTTP Error 401: Unauthorized "]}
## C. the three chart feeders that fail every run (read-only)

- `14:34:33` justhodl-estimate-revisions (Complete): ["[ERROR] ValueError: Original received calendar required Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 368, in lambda_handler \u00a0\u00a0\u00a0\u00a0calendar_acquisition=_estimate_calendar_fetch();calendar_evidence=calendar_projection(calendar_acquisition);calendar=calendar_evidence['rows'] \u00a0\u00a0File \"/var/task/estimate_observations.py\", line 177, "]
- `14:34:35`   runs/day (72 h, Complete): [["2026-10-21", 6, 592, "134000000"]]
- `14:34:35`   config: timeout 120 s, memory 512 MB, last modified 2026-09-28T09:43:20
- `14:34:38` justhodl-share-flows (Complete): ["[ERROR] ValueError: Request already attempted; inspect its retained failure or ambiguous state Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 86, in lambda_handler \u00a0\u00a0\u00a0\u00a0result = producer.run(client, BUCKET, event.get('request_id') or event.get('id') or \u00a0\u00a0File \"/var/task/capital_structure_producer.py\", line 106, in run \u00a0\u00a0\u00a0\u00a0raise", "[ERROR] ValueError: Underlying capital-structure or SEC acquisition is future-dated or older than 48 hours Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 86, in lambda_handler \u00a0\u00a0\u00a0\u00a0result = producer.run(client, BUCKET, event.get('request_id') or event.get('id') or \u00a0\u00a0File \"/var/task/capital_structure_producer.py\", line 130, in r"]
- `14:34:40`   runs/day (72 h, Complete): [["2026-10-21", 3, 553, "128000000"]]
- `14:34:40`   config: timeout 900 s, memory 4096 MB, last modified 2026-09-26T01:13:40
- `14:34:42` justhodl-earnings-pead (Complete): ["[ERROR] ValueError: No selected universe; preserve previous publication Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 474, in lambda_handler \u00a0\u00a0\u00a0\u00a0if not selected:raise ValueError('No selected universe; preserve previous publication')"]
- `14:34:45`   runs/day (72 h, Complete): [["2026-10-21", 3, 972, "238000000"]]
- `14:34:45`   config: timeout 600 s, memory 1024 MB, last modified 2026-09-28T11:12:33
## D. the lambda-url the chart calls directly

- `14:37:03` lambda-url ru3djltl... belongs to justhodl-edgar-insiders
- `14:37:06`   justhodl-edgar-insiders: reserved concurrency None; recent 404-ish log lines (Complete): ["REPORT RequestId: 3fa5b79c-e5f7-498a-96de-db3f7bc591b5\tDuration: 18.06 ms\tBilled Duration: 19 ms\tMemory Size: 512 MB\tMax Memory Used: 104 MB\t\nXRAY TraceId: 1-6a", "REPORT RequestId: b84049ea-26e2-4b17-ac01-1ccd69378242\tDuration: 13.41 ms\tBilled Duration: 14 ms\tMemory Size: 512 MB\tMax Memory Used: 104 MB\t\nXRAY TraceId: 1-6a", "END RequestId: b84049ea-26e2-4b17-ac01-1ccd69378242\n", "START RequestId: b84049ea-26e2-4b17-ac01-1ccd69378242 Version: $LATEST\n", "END RequestId: 96b734d6-3ed9-4958-b404-54ef46bbd412\n"]
- `14:37:06` ✅ symdir at 30; 5/6 stale writers refreshed within the wait; feeder errors read; re-probe with ops 6597 next

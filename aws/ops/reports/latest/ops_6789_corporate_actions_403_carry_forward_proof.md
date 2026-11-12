# ops 6789 - justhodl-corporate-actions 403 carry-forward proof 2026-11-12 (ladder, then two synchronous invocations for AAPL, MSFT, KO)

**Status:** success  
**Duration:** 291.3s  
**Finished:** 2026-11-12T11:48:16+00:00  

## Data

| AAPL_cash | AAPL_p403 | AAPL_with_close | KO_cash | KO_p403 | KO_with_close | MSFT_cash | MSFT_p403 | MSFT_with_close | cursor_after | cursor_before | deployed_last_modified | index_universe | run1_401 | run1_403 | run1_http_failures | run1_s | run1_stopped_early | run1_written | run2_403 | run2_http_failures | run2_s | run2_stopped_early | run2_written |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 58 | 38 | 20 | 95 | 75 | 20 | 91 | 71 | 20 | 0 | 0 | 2026-11-12T11:46:45.000+0000 | 11754 | 0 | 32 | 31 | 70 | None | 3 | 0 | 0 | 9 | None | 3 |

## Log
## 1. the three-rung ladder

- `11:43:25` rung 1: repo has the marker
- `11:43:25`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:43:41`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:43:57`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:44:12`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:44:28`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:44:43`   attempt 6: marker False, State Active / Successful - waiting 15 s
- `11:44:59`   attempt 7: marker False, State Active / Successful - waiting 15 s
- `11:45:14`   attempt 8: marker False, State Active / Successful - waiting 15 s
- `11:45:30`   attempt 9: marker False, State Active / Successful - waiting 15 s
- `11:45:45`   attempt 10: marker False, State Active / Successful - waiting 15 s
- `11:46:01`   attempt 11: marker False, State Active / Successful - waiting 15 s
- `11:46:16`   attempt 12: marker False, State Active / Successful - waiting 15 s
- `11:46:32`   attempt 13: marker False, State Active / Successful - waiting 15 s
- `11:46:47` rung 2+3 at attempt 14: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-12T11:46:45.000+0000
- `11:46:52` sweep state before: {"cursor": 0, "updated_at": "2026-11-12T11:35:09.988954+00:00"}
## 2. run 1 - the 403 dividends are recorded once (one call each)

- `11:48:02` run1: invoke took 70 s | statusCode 200 | stats {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "cursor": "untouched", "duration_s": 69.2}
- `11:48:02` run1: log tail: 36 lines, 31 http failures ({"HTTPError 403": 31}), 0 of them 401, 32 of them 403 (the 4 KB tail may truncate)
- `11:48:02`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/MSFT/range/1/day/2004-11-14/2004-11-14: HTTPError 403
- `11:48:02`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/MSFT/range/1/day/2004-08-22/2004-08-22: HTTPError 403
- `11:48:02`   [corpact] done: {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "cursor": "untouched", "duration_s": 69.2}
- `11:48:02`   END RequestId: 01efe5f2-87c2-4389-943b-83480224fc49
- `11:48:02`   REPORT RequestId: 01efe5f2-87c2-4389-943b-83480224fc49 Duration: 69165.96 ms Billed Duration: 69436 ms Memory Size: 256 MB Max Memory Used: 164 MB Init Duration: 269.22 ms
- `11:48:02`   XRAY TraceId: 1-6af5a72d-362734c13fcda99215b3f2f2 SegmentId: 5002f43ecb1dc57c Sampled: true
- `11:48:02` run1 AAPL: regenerated True (2026-11-12T11:46:58.435223+00:00) | cash dividends 58, with pre_ex_close 20, pending 38 (plan_history_403 38, deferred 0) | factors 25 rows | factor_gaps True | agreement 0.6327
- `11:48:02`   example 403 dividend: {"date": "2012-08-09", "type": "dividend_cash", "cash_amount": 2.65, "currency": "USD", "pay_date": "2012-08-16", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:46:59.257564+00:00", "note": "pre-ex session outside
- `11:48:02` run1 MSFT: regenerated True (2026-11-12T11:47:38.106349+00:00) | cash dividends 91, with pre_ex_close 20, pending 71 (plan_history_403 71, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.8911
- `11:48:02`   example 403 dividend: {"date": "2004-08-23", "type": "dividend_cash", "cash_amount": 0.08, "currency": "USD", "pay_date": "2004-09-14", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:47:38.813325+00:00", "note": "pre-ex session outside
- `11:48:03` run1 KO: regenerated True (2026-11-12T11:47:12.252120+00:00) | cash dividends 95, with pre_ex_close 20, pending 75 (plan_history_403 75, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.0187
- `11:48:03`   example 403 dividend: {"date": "2003-06-11", "type": "dividend_cash", "cash_amount": 0.22, "currency": "USD", "pay_date": "2003-07-01", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:47:12.988105+00:00", "note": "pre-ex session outside
## 3. run 2 - the 403 dividends are carried forward (no call)

- `11:48:15` run2: invoke took 9 s | statusCode 200 | stats {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "cursor": "untouched", "duration_s": 8.9}
- `11:48:15` run2: log tail: 5 lines, 0 http failures ({}), 0 of them 401, 0 of them 403 (the 4 KB tail may truncate)
- `11:48:15`   START RequestId: 9556b5b6-0e00-4298-a21f-52474a32a22a Version: $LATEST
- `11:48:15`   [corpact] done: {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "cursor": "untouched", "duration_s": 8.9}
- `11:48:15`   END RequestId: 9556b5b6-0e00-4298-a21f-52474a32a22a
- `11:48:15`   REPORT RequestId: 9556b5b6-0e00-4298-a21f-52474a32a22a Duration: 8909.24 ms Billed Duration: 9272 ms Memory Size: 256 MB Max Memory Used: 164 MB Init Duration: 362.20 ms
- `11:48:15`   XRAY TraceId: 1-6af5a776-46c52d18750c83f969c891d2 SegmentId: 948ea2310135eaf7 Sampled: true
- `11:48:15` run2 AAPL: regenerated True (2026-11-12T11:48:11.827995+00:00) | cash dividends 58, with pre_ex_close 20, pending 38 (plan_history_403 38, deferred 0) | factors 25 rows | factor_gaps True | agreement 0.6327
- `11:48:15`   example 403 dividend: {"date": "2012-08-09", "type": "dividend_cash", "cash_amount": 2.65, "currency": "USD", "pay_date": "2012-08-16", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:46:59.257564+00:00", "note": "pre-ex session outside
- `11:48:15` run2 MSFT: regenerated True (2026-11-12T11:48:14.356188+00:00) | cash dividends 91, with pre_ex_close 20, pending 71 (plan_history_403 71, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.8911
- `11:48:15`   example 403 dividend: {"date": "2004-08-23", "type": "dividend_cash", "cash_amount": 0.08, "currency": "USD", "pay_date": "2004-09-14", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:47:38.813325+00:00", "note": "pre-ex session outside
- `11:48:16` run2 KO: regenerated True (2026-11-12T11:48:13.107004+00:00) | cash dividends 95, with pre_ex_close 20, pending 75 (plan_history_403 75, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.0187
- `11:48:16`   example 403 dividend: {"date": "2003-06-11", "type": "dividend_cash", "cash_amount": 0.22, "currency": "USD", "pay_date": "2003-07-01", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "pending_reason": "plan_history_403", "checked_at": "2026-11-12T11:47:12.988105+00:00", "note": "pre-ex session outside
- `11:48:16` sweep state after: {"cursor": 0, "updated_at": "2026-11-12T11:35:09.988954+00:00"}
- `11:48:16` index: universe 11754 covered 5577
- `11:48:16` index data/corporate-actions-index.json: 0.0 h old (1089649 B)
- `11:48:16` ✅ fixed: run 1 recorded the plan-history 403 dividends once (32 x 403, 70 s); run 2 carried them forward with 0 http failures in 9 s and regenerated AAPL, MSFT, KO with the same counts

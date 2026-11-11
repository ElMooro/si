# ops 6787 - justhodl-corporate-actions fix proof 2026-11-11 (ladder, then one synchronous invocation for AAPL, MSFT, KO)

**Status:** success  
**Duration:** 335.0s  
**Finished:** 2026-11-11T11:48:12+00:00  

## Data

| AAPL_cash | AAPL_pending | AAPL_with_close | KO_cash | KO_pending | KO_with_close | MSFT_cash | MSFT_pending | MSFT_with_close | deployed_last_modified | factor_gaps | http_401 | http_failures | invoke_s | polygon_key | processed | stopped_early | with_events | written |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 58 | 38 | 20 | 95 | 75 | 20 | 91 | 71 | 20 | 2026-11-11T11:43:51.000+0000 | 3 | 0 | 31 | 252 | True | 3 | None | 3 | 3 |

## Log
## 1. the three-rung ladder

- `11:42:37` rung 1: repo has the marker
- `11:42:37`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:42:53`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:43:08`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:43:24`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:43:39`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:43:54` rung 2+3 at attempt 6: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-11T11:43:51.000+0000
## 2. one synchronous invocation for AAPL, MSFT, KO

- `11:48:12` invoke took 252 s | statusCode 200 | stats {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "duration_s": 251.7}
- `11:48:12` log tail: 36 lines, 31 http failures ({"HTTPError 403": 31}), 0 of them 401
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-11-08/2017-11-08: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-11-07/2017-11-07: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-08-09/2017-08-09: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-08-08/2017-08-08: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-08-07/2017-08-07: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-05-10/2017-05-10: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-05-09/2017-05-09: HTTPError 403
- `11:48:12`   [corpact] http failed https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2017-05-08/2017-05-08: HTTPError 403
- `11:48:12`   [corpact] done: {"universe": 3, "window": 3, "due": 3, "processed": 3, "written": 3, "failed": 0, "with_events": 3, "disagreements": 309, "factor_gaps": 3, "polygon_key": true, "duration_s": 251.7}
- `11:48:12`   END RequestId: 2d2a10f5-d088-4c42-8f6e-31bcf49e8c18
- `11:48:12`   REPORT RequestId: 2d2a10f5-d088-4c42-8f6e-31bcf49e8c18 Duration: 251704.32 ms Billed Duration: 252058 ms Memory Size: 256 MB Max Memory Used: 164 MB Init Duration: 353.38 ms
- `11:48:12`   XRAY TraceId: 1-6af454ff-5da483222144f4c25dce7699 SegmentId: 7c6112b1fbcd5ee8 Sampled: true
## 3. the artifacts the engine just wrote

- `11:48:12` AAPL: regenerated True (2026-11-11T11:47:33.704765+00:00) | n_events 63 | cash dividends 58, with pre_ex_close 20, pending 38 | factors 25 rows | factor_gaps True | agreement 0.6327 | latest 2026-11-12
- `11:48:12`   pending: [{"date": "2012-08-09", "type": "dividend_cash", "cash_amount": 2.65, "currency": "USD", "pay_date": "2012-08-16", "source": "polygon", "factor_pending": true, "note": "factor deferred (time budget)"}, {"date": "2012-11-07", "type": "dividend_cash", "cash_amount": 2.65, "currency": "USD", "pay_date"
- `11:48:12` MSFT: regenerated True (2026-11-11T11:45:54.624212+00:00) | n_events 92 | cash dividends 91, with pre_ex_close 20, pending 71 | factors 21 rows | factor_gaps True | agreement 0.8911 | latest 2026-11-19
- `11:48:12`   pending: [{"date": "2004-08-23", "type": "dividend_cash", "cash_amount": 0.08, "currency": "USD", "pay_date": "2004-09-14", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "note": "factor unavailable (pre_ex_close=None)"}, {"date": "2004-11-15", "type": "dividend_cash", "cash_amount": 0.08
- `11:48:12` KO: regenerated True (2026-11-11T11:44:05.588607+00:00) | n_events 96 | cash dividends 95, with pre_ex_close 20, pending 75 | factors 21 rows | factor_gaps True | agreement 0.0187 | latest 2026-12-01
- `11:48:12`   pending: [{"date": "2003-06-11", "type": "dividend_cash", "cash_amount": 0.22, "currency": "USD", "pay_date": "2003-07-01", "source": "polygon", "pre_ex_close": null, "factor_pending": true, "note": "factor unavailable (pre_ex_close=None)"}, {"date": "2003-09-12", "type": "dividend_cash", "cash_amount": 0.22
- `11:48:12` index data/corporate-actions-index.json: -0.0 h old (1089250 B)
- `11:48:12` ✅ fixed: the engine's own run fetched pre-ex closes with the key (no 401), regenerated AAPL, MSFT, KO, finished in 252 s

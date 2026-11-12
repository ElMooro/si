# ops 6789 - justhodl-corporate-actions 403 carry-forward proof 2026-11-12 (ladder, then two synchronous invocations for AAPL, MSFT, KO)

**Status:** failure  
**Duration:** 143.7s  
**Finished:** 2026-11-12T11:35:10+00:00  

## Error

```
SystemExit: 1
```

## Data

| AAPL_cash | AAPL_p403 | AAPL_with_close | KO_cash | KO_p403 | KO_with_close | MSFT_cash | MSFT_p403 | MSFT_with_close | deployed_last_modified | run1_401 | run1_403 | run1_http_failures | run1_s | run1_stopped_early | run1_written | run2_403 | run2_http_failures | run2_s | run2_stopped_early | run2_written |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 58 | 0 | 20 | 95 | 0 | 20 | 91 | 0 | 20 | 2026-11-12T11:34:40.000+0000 | 0 | 0 | 0 | 6 | None | 0 | 0 | 0 | 6 | None | 0 |

## Log
## 1. the three-rung ladder

- `11:32:46` rung 1: repo has the marker
- `11:32:47`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:33:02`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:33:17`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:33:33`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:33:48`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:34:04`   attempt 6: marker False, State Active / Successful - waiting 15 s
- `11:34:19`   attempt 7: marker False, State Active / Successful - waiting 15 s
- `11:34:34`   attempt 8: marker False, State Active / Successful - waiting 15 s
- `11:34:50` rung 2+3 at attempt 9: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-12T11:34:40.000+0000
## 2. run 1 - the 403 dividends are recorded once (one call each)

- `11:35:01` run1: invoke took 6 s | statusCode 200 | stats {"universe": 3, "window": 3, "due": 0, "processed": 0, "written": 0, "failed": 0, "with_events": 0, "disagreements": 0, "factor_gaps": 0, "polygon_key": true, "duration_s": 5.2}
- `11:35:01` run1: log tail: 5 lines, 0 http failures ({}), 0 of them 401, 0 of them 403 (the 4 KB tail may truncate)
- `11:35:01`   START RequestId: a9e71a61-199f-4fee-9829-308fc9fd8868 Version: $LATEST
- `11:35:01`   [corpact] done: {"universe": 3, "window": 3, "due": 0, "processed": 0, "written": 0, "failed": 0, "with_events": 0, "disagreements": 0, "factor_gaps": 0, "polygon_key": true, "duration_s": 5.2}
- `11:35:01`   END RequestId: a9e71a61-199f-4fee-9829-308fc9fd8868
- `11:35:01`   REPORT RequestId: a9e71a61-199f-4fee-9829-308fc9fd8868 Duration: 5265.22 ms Billed Duration: 5556 ms Memory Size: 256 MB Max Memory Used: 164 MB Init Duration: 290.56 ms
- `11:35:01`   XRAY TraceId: 1-6af5a45f-68fb848778adf1560814021b SegmentId: 590931cec41e4c85 Sampled: true
- `11:35:01` run1 AAPL: regenerated False (2026-11-11T11:47:33.704765+00:00) | cash dividends 58, with pre_ex_close 20, pending 38 (plan_history_403 0, deferred 19) | factors 25 rows | factor_gaps True | agreement 0.6327
- `11:35:01` run1 MSFT: regenerated False (2026-11-11T11:45:54.624212+00:00) | cash dividends 91, with pre_ex_close 20, pending 71 (plan_history_403 0, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.8911
- `11:35:01` run1 KO: regenerated False (2026-11-11T11:44:05.588607+00:00) | cash dividends 95, with pre_ex_close 20, pending 75 (plan_history_403 0, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.0187
## 3. run 2 - the 403 dividends are carried forward (no call)

- `11:35:10` run2: invoke took 6 s | statusCode 200 | stats {"universe": 3, "window": 3, "due": 0, "processed": 0, "written": 0, "failed": 0, "with_events": 0, "disagreements": 0, "factor_gaps": 0, "polygon_key": true, "duration_s": 5.3}
- `11:35:10` run2: log tail: 5 lines, 0 http failures ({}), 0 of them 401, 0 of them 403 (the 4 KB tail may truncate)
- `11:35:10`   START RequestId: a17741d2-5fe0-4381-ab85-a1686567455d Version: $LATEST
- `11:35:10`   [corpact] done: {"universe": 3, "window": 3, "due": 0, "processed": 0, "written": 0, "failed": 0, "with_events": 0, "disagreements": 0, "factor_gaps": 0, "polygon_key": true, "duration_s": 5.3}
- `11:35:10`   END RequestId: a17741d2-5fe0-4381-ab85-a1686567455d
- `11:35:10`   REPORT RequestId: a17741d2-5fe0-4381-ab85-a1686567455d Duration: 5302.89 ms Billed Duration: 5624 ms Memory Size: 256 MB Max Memory Used: 164 MB Init Duration: 321.06 ms
- `11:35:10`   XRAY TraceId: 1-6af5a468-3dfc11951cef324224de28ba SegmentId: 113d20d18e474ccb Sampled: true
- `11:35:10` run2 AAPL: regenerated False (2026-11-11T11:47:33.704765+00:00) | cash dividends 58, with pre_ex_close 20, pending 38 (plan_history_403 0, deferred 19) | factors 25 rows | factor_gaps True | agreement 0.6327
- `11:35:10` run2 MSFT: regenerated False (2026-11-11T11:45:54.624212+00:00) | cash dividends 91, with pre_ex_close 20, pending 71 (plan_history_403 0, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.8911
- `11:35:10` run2 KO: regenerated False (2026-11-11T11:44:05.588607+00:00) | cash dividends 95, with pre_ex_close 20, pending 75 (plan_history_403 0, deferred 0) | factors 21 rows | factor_gaps True | agreement 0.0187
- `11:35:10` ⚠ AAPL: run 2 did not regenerate the artifact with pre-ex closes ({'fresh': False, 'cash': 58, 'with_close': 20, 'pending': 38, 'p403': 0, 'deferred': 19} / {'fresh': False, 'cash': 58, 'with_close': 20, 'pending': 38, 'p403': 0, 'deferred': 19})
- `11:35:10` ⚠ MSFT: run 2 did not regenerate the artifact with pre-ex closes ({'fresh': False, 'cash': 91, 'with_close': 20, 'pending': 71, 'p403': 0, 'deferred': 0} / {'fresh': False, 'cash': 91, 'with_close': 20, 'pending': 71, 'p403': 0, 'deferred': 0})
- `11:35:10` ⚠ KO: run 2 did not regenerate the artifact with pre-ex closes ({'fresh': False, 'cash': 95, 'with_close': 20, 'pending': 75, 'p403': 0, 'deferred': 0} / {'fresh': False, 'cash': 95, 'with_close': 20, 'pending': 75, 'p403': 0, 'deferred': 0})
- `11:35:10` index data/corporate-actions-index.json: -0.0 h old (1089645 B)
- `11:35:10` ✗ the carry-forward did not hold - read sections 2 and 3

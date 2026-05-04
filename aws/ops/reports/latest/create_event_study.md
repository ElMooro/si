# Create justhodl-event-study + smoke test

**Status:** success  
**Duration:** 11.3s  
**Finished:** 2026-05-04T12:27:29+00:00  

## Log
- `12:27:18`   zip size: 4,863b
- `12:27:19` ✅   ✓ created
## EventBridge schedule (daily 14 UTC)

- `12:27:20` ✅   ✓ wired
## Smoke test

- `12:27:29`   status: 200 duration: 1.4s
- `12:27:29`   resp: {"statusCode": 200, "body": "{\"n_event_classes\": 8, \"active_themes\": [], \"expected_21d_return_from_active_pct\": null, \"duration_s\": 0.54}"}
## S3 verify

- `12:27:29`   as_of: 2026-05-01
- `12:27:29`   active_themes: []
- `12:27:29`   expected_21d_return: None%
## 📊 Event class summaries

- `12:27:29`   fed_first_cut             n=  2 days_since= 1612 —
- `12:27:29`     21d: mean=+3.24% median=+3.24% hit=100.0%
- `12:27:29`     63d: mean=+0.49% median=+0.49% hit=50.0%
- `12:27:29`     most recent: 2021-12-01
- `12:27:29`   fed_first_hike            n=  2 days_since= 2281 —
- `12:27:29`     21d: mean=-1.40% median=-1.40% hit=50.0%
- `12:27:29`     63d: mean=-4.10% median=-4.10% hit=50.0%
- `12:27:29`     most recent: 2020-02-01
- `12:27:29`   yield_curve_inverts       n=  5 days_since= 1491 —
- `12:27:29`     21d: mean=-0.37% median=+0.86% hit=80.0%
- `12:27:29`     63d: mean=+1.05% median=+4.30% hit=80.0%
- `12:27:29`     most recent: 2022-04-01
- `12:27:29`   yield_curve_steepens      n=  3 days_since=  612 —
- `12:27:29`     21d: mean=+1.28% median=+0.86% hit=100.0%
- `12:27:29`     63d: mean=+5.01% median=+4.30% hit=100.0%
- `12:27:29`     most recent: 2024-08-27
- `12:27:29`   vix_spike                 n= 31 days_since=   35 —
- `12:27:29`     21d: mean=+1.79% median=+0.86% hit=93.5%
- `12:27:29`     63d: mean=+4.88% median=+4.30% hit=93.3%
- `12:27:29`     most recent: 2026-03-27
- `12:27:29`   vix_normalize             n= 37 days_since=   22 —
- `12:27:29`     21d: mean=+0.69% median=+0.86% hit=91.9%
- `12:27:29`     63d: mean=+3.92% median=+4.30% hit=91.9%
- `12:27:29`     most recent: 2026-04-09
- `12:27:29`   credit_blowout            n=  0 days_since= None —
- `12:27:29`   credit_recover            n=  2 days_since=  907 —
- `12:27:29`     21d: mean=+2.75% median=+2.75% hit=100.0%
- `12:27:29`     63d: mean=+5.33% median=+5.33% hit=50.0%
- `12:27:29`     most recent: 2023-11-06

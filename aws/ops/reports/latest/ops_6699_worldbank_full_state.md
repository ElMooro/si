# ops 6699 - justhodl-worldbank-full: state, drivers, archive impact (read-only)

**Status:** success  
**Duration:** 22.0s  
**Finished:** 2026-10-27T20:17:08+00:00  

## Log
- `20:16:47` state.json 2216751 bytes (LastModified 2026-10-27T08:59:02+00:00): phase DRAIN, n_indicators 29490, have 29468 (fresh 29468, no_data 0), queue 2870, failures 22, as_of 2026-10-27T08:59:01+00:00, lease_until 0
- `20:16:47` bytes banked (state): 0.48 GB; queue head [['per_pr_dp.avt_q3_preT_tot', 0], ['per_pr_dp.avt_q3_preT_totfr', 0], ['per_pr_dp.avt_q3_preT_totmr', 0]]
- `20:16:47` zip sizes: max 0.2 MB, p99 0.09 MB, median 0.014 MB, sum 0.48 GB
- `20:17:04` state.json versions in the live bucket: 56690, 143.0 GB, 2026-10-25 03:20:44+00:00 .. 2026-10-27 08:59:02+00:00
## drivers

- `20:17:05` function: 1024 MB / 850 s, LastModified 2026-09-09T13:29:57.000+0000, reserved concurrency None
- `20:17:05` schedule justhodl-worldbank-full-weekly [ENABLED] rate(7 days) target justhodl-worldbank-full input {"redrain": true}
- `20:17:05` schedule justhodl-worldbank-full-2h [ENABLED] cron(20 3 * * ? *) target justhodl-worldbank-full input {}
- `20:17:05` schedule justhodl-worldbank-wgi-daily [ENABLED] cron(5 3 ? * MON *) target justhodl-worldbank-wgi input {}
- `20:17:06` parked chain tickets: 1 ['2026-10-27T08:59:02']
- `20:17:07` invocations (REPORT) in 48 h: 14, total duration 2.4 h, first 2026-10-26T01:30 last 2026-10-26T03:42
## the sealed archive bucket (every version replicated, kept)

- `20:17:07` archive 2026-10-21: 446.0 GB
- `20:17:07` archive 2026-10-22: 1042.0 GB (+596.0 GB)
- `20:17:07` archive 2026-10-23: 1051.0 GB (+9.0 GB)
- `20:17:07` archive 2026-10-24: 1059.8 GB (+8.8 GB)
- `20:17:07` archive 2026-10-25: 1104.2 GB (+44.4 GB)
- `20:17:07` archive 2026-10-26: 1152.1 GB (+47.9 GB)
- `20:17:08` archive copies of state.json (first page): 1000, 2.07 GB
- `20:17:08` ✅ ops 6699 complete (read-only)

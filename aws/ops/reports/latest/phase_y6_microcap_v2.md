- `11:58:25`     source: 15994 chars

# 1) Wait + force-deploy

- `11:58:28`     ✓ deployed at 2026-05-06T11:58:25.000+0000

# 2) Smoke invoke (1300+ stocks for nano/micro/small/mid)

- `11:58:34`     status: 200, dur: 6.0s
- `11:58:34`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 0, \"n_tier_s\": 0, \"n_tier_a\": 0, \"duration_s\": 5.1}"}
- `11:58:34`       START RequestId: 2466e866-4486-44a5-b830-af42f35fbd91 Version: $LATEST
- `11:58:34`       [float-sq] starting v1.0
- `11:58:34`       [float-sq] universe: 600 stocks
- `11:58:34`       [float-sq] fetching FINRA short volume history...
- `11:58:34`       [float-sq] FINRA tickers: 12487
- `11:58:34`       [float-sq] OK: 0, filtered_out: 600
- `11:58:34`       [float-sq] wrote 337b
- `11:58:34`       [float-sq] tier_s=0 tier_a=0
- `11:58:34`       END RequestId: 2466e866-4486-44a5-b830-af42f35fbd91
- `11:58:34`       REPORT RequestId: 2466e866-4486-44a5-b830-af42f35fbd91	Duration: 5202.24 ms	Billed Duration: 5772 ms	Memory Size: 2048 MB	Max Memory Used: 172 MB	Init Duration: 568.85 ms

# 3) Inspect output

- `11:58:34`     stats: {"n_universe": 600, "n_evaluated": 0, "n_filtered_out": 600, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_finra_tickers": 12487}
- `11:58:34`   
- `11:58:34`     ── TIER_S PARABOLIC SETUPS (rare) ──
- `11:58:34`   
- `11:58:34`     ── TOP 15 OVERALL ──
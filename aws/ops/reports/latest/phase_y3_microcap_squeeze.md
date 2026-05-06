- `11:25:47`     source: 15994 chars

# 1) Deploy

- `11:25:54`     ✓ deployed

# 2) Schedule daily 22 UTC

- `11:25:54`     ✓ permission added

# 3) Smoke invoke (~3-5min — fetches profile + history + FINRA × 600)

- `11:25:59`     status: 200, dur: 4.7s
- `11:25:59`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 0, \"n_tier_s\": 0, \"n_tier_a\": 0, \"duration_s\": 3.6}"}
- `11:25:59`       START RequestId: 9e29924b-a9a5-4d9b-995c-b7d07a5cceb8 Version: $LATEST
- `11:25:59`       [float-sq] starting v1.0
- `11:25:59`       [float-sq] universe: 338 stocks
- `11:25:59`       [float-sq] fetching FINRA short volume history...
- `11:25:59`       [float-sq] FINRA tickers: 12487
- `11:25:59`       [float-sq] OK: 0, filtered_out: 338
- `11:25:59`       [float-sq] wrote 337b
- `11:25:59`       [float-sq] tier_s=0 tier_a=0
- `11:25:59`       END RequestId: 9e29924b-a9a5-4d9b-995c-b7d07a5cceb8
- `11:25:59`       REPORT RequestId: 9e29924b-a9a5-4d9b-995c-b7d07a5cceb8	Duration: 3734.55 ms	Billed Duration: 4329 ms	Memory Size: 2048 MB	Max Memory Used: 171 MB	Init Duration: 593.56 ms

# 4) Inspect output

- `11:25:59`     generated_at: 2026-05-06T11:25:59+00:00
- `11:25:59`     stats: {"n_universe": 338, "n_evaluated": 0, "n_filtered_out": 338, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_finra_tickers": 12487}
- `11:25:59`   
- `11:25:59`     ── TIER_S PARABOLIC SETUPS (rare, score >= 70) ──
- `11:25:59`   
- `11:25:59`     ── TOP 15 OVERALL ──
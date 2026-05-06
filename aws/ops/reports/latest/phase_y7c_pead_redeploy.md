
# 1) Force-deploy

- `13:48:34`     ✓ deployed at 2026-05-06T13:48:32.000+0000

# 2) Smoke invoke

- `13:48:39`     status: 200, dur: 5.2s
- `13:48:39`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 0, \"n_tier_s\": 0, \"n_tier_a\": 0, \"n_pre_earnings\": 0, \"duration_s\": 4.2}"}
- `13:48:39`       START RequestId: f8d5e73c-7f88-4f3c-be67-c01591a532c5 Version: $LATEST
- `13:48:39`       [pead] starting v1.0
- `13:48:39`       [pead] universe: 1500 stocks
- `13:48:39`       [pead] OK: 0, no_data: 1500
- `13:48:39`       [pead] wrote 483b
- `13:48:39`       [pead] tier_s=0 tier_a=0
- `13:48:39`       END RequestId: f8d5e73c-7f88-4f3c-be67-c01591a532c5
- `13:48:39`       REPORT RequestId: f8d5e73c-7f88-4f3c-be67-c01591a532c5	Duration: 4265.35 ms	Billed Duration: 4785 ms	Memory Size: 2048 MB	Max Memory Used: 111 MB	Init Duration: 518.81 ms

# 3) Inspect output

- `13:48:39`     stats: {"n_universe": 1500, "n_evaluated": 0, "n_no_data": 1500, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_pre_earnings_setups": 0, "by_cap_bucket": {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0}}
- `13:48:39`   
- `13:48:39`     ── TIER_S DRIFTING (4Q+ streak, big beats, recent earnings) ──
- `13:48:39`   
- `13:48:39`     ── TOP 20 OVERALL ──
- `13:48:39`   
- `13:48:39`     ── BEST MICROCAP/NANO PEAD ──
- `13:48:39`   
- `13:48:39`     ── BEST SMALLCAP PEAD ──
- `13:48:39`   
- `13:48:39`     ── PRE-EARNINGS SETUPS (2-14d, streak >= 3) ──
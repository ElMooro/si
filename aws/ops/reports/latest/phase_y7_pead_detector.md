- `13:38:00`     source: 18818 chars

# 1) Deploy

- `13:38:04`     ✓ deployed

# 2) Schedule daily 8 UTC

- `13:38:05`     ✓ permission added

# 3) Smoke invoke (heavy — fetches earnings for 1500 stocks)

- `13:38:15`     status: 200, dur: 10.0s
- `13:38:15`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 0, \"n_tier_s\": 0, \"n_tier_a\": 0, \"n_pre_earnings\": 0, \"duration_s\": 9.0}"}
- `13:38:15`       START RequestId: 44e6947c-02b8-4053-87ee-aed1da4fff1f Version: $LATEST
- `13:38:15`       [pead] starting v1.0
- `13:38:15`       [pead] universe: 1500 stocks
- `13:38:15`       [pead] OK: 0, no_data: 1500
- `13:38:15`       [pead] wrote 483b
- `13:38:15`       [pead] tier_s=0 tier_a=0
- `13:38:15`       END RequestId: 44e6947c-02b8-4053-87ee-aed1da4fff1f
- `13:38:15`       REPORT RequestId: 44e6947c-02b8-4053-87ee-aed1da4fff1f	Duration: 9069.94 ms	Billed Duration: 9636 ms	Memory Size: 2048 MB	Max Memory Used: 109 MB	Init Duration: 565.18 ms

# 4) Inspect output

- `13:38:15`     stats: {"n_universe": 1500, "n_evaluated": 0, "n_no_data": 1500, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_pre_earnings_setups": 0, "by_cap_bucket": {"nano": 0, "micro": 0, "small": 0, "mid": 0, "large": 0, "mega": 0}}
- `13:38:15`   
- `13:38:15`     ── TIER_S DRIFTING (4Q+ streak, big beats, recent earnings) ──
- `13:38:15`   
- `13:38:15`     ── TOP 15 OVERALL ──
- `13:38:15`   
- `13:38:15`     ── BEST MICROCAP/NANO PEAD ──
- `13:38:15`   
- `13:38:15`     ── BEST SMALLCAP PEAD ──
- `13:38:15`   
- `13:38:15`     ── PRE-EARNINGS SETUPS (2-14 days out, 3+Q streak) ──
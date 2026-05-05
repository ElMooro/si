
# 1) Verify deep-value is the latest deployed version

- `20:06:02`     modified: 2026-05-05T20:05:07.000+0000
- `20:06:02`     has top_25_excluded_financials: True

# 2) Re-run deep-value

- `20:06:09`     status: 200
- `20:06:09`     inner: {"n_universe": 500, "n_qualifying": 35, "n_tier_a": 18, "duration_s": 5.9}

# 3) Re-aggregate compound signals

- `20:06:09`     nobrainers: 25
- `20:06:09`     insiders: 22
- `20:06:09`     smart_money: 85
- `20:06:09`     deep_value: 25
- `20:06:09`     eps_velocity: 25
- `20:06:09`   
- `20:06:09`     total names: 96
- `20:06:09`     on 2+ lists: 1
- `20:06:09`     on 3+ lists: 0
- `20:06:09`   
- `20:06:09`     ── Compound leaderboard ──
- `20:06:09`     CSGP   #2  systems=['eps_velocity', 'insiders']  compound=220.7
- `20:06:09`     wrote 733b to data/compound-signals.json

# 4) Build Telegram digest

- `20:06:09`     message length: 743 chars

# 5) Send Telegram

- `20:06:10`     ✅ delivered, message_id=671
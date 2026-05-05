- `23:52:43`     source: 12810 chars

# 1) Build zip + create/update Lambda

- `23:52:43`     zip: 12,974b
- `23:52:43`     creating new
- `23:52:45`     ✓ ready, mem=1024MB to=300s

# 2) Schedule daily 13:00 UTC

- `23:52:45`     ✓ permission added

# 3) Smoke invoke (will take ~120-200s — pulls 90d history for 600 tickers)

- `23:52:55`     status: 200, dur: 9.2s
- `23:52:55`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 552, \"n_tier_a\": 0, \"n_tier_b\": 28, \"duration_s\": 8.3}"}
- `23:52:55`       START RequestId: da879fb0-aa55-45f3-9208-d46bca1bb767 Version: $LATEST
- `23:52:55`       [momentum] starting v1.0, max_tickers=600, min_dollar_vol=$5.0M
- `23:52:55`       [momentum] universe: 563 from data/universe.json
- `23:52:55`       [momentum] computing SPY benchmark returns...
- `23:52:55`       [momentum] SPY returns: 20d=9.79%, 60d=4.80%
- `23:52:55`       [momentum] OK: 552, no_history: 0, no_signal: 11
- `23:52:55`       [momentum] wrote 234,357b to data/momentum-breakout.json
- `23:52:55`       [momentum] tier_a=0 tier_b=28 parabolic=16
- `23:52:55`       [momentum] TOP: [('MTSI', 69.6, 'TIER_B_MOMENTUM'), ('ICHR', 68, 'TIER_B_MOMENTUM'), ('AMZN', 65, 'TIER_B_MOMENTUM'), ('PWR', 65, 'TIER_B_MOMENTUM'), ('FIX', 65, 'TIER_B_MOMENTUM'), ('IRM', 65, 'TIER_B_MOMENTUM'), ('MTZ', 65, 'TIER_B_MOMENTUM'), ('GNRC', 65, 'TIER_B_MOMENTUM')]
- `23:52:55`       END RequestId: da879fb0-aa55-45f3-9208-d46bca1bb767
- `23:52:55`       REPORT RequestId: da879fb0-aa55-45f3-9208-d46bca1bb767	Duration: 8457.96 ms	Billed Duration: 8910 ms	Memory Size: 1024 MB	Max Memory Used: 109 MB	Init Duration: 451.06 ms

# 4) Verify output + pump-list coverage

- `23:52:55`     schema: 1
- `23:52:55`     generated_at: 2026-05-05T23:52:54+00:00
- `23:52:55`     stats: {"n_universe": 563, "n_evaluated": 552, "n_tier_a": 0, "n_tier_b": 28, "n_parabolic": 16, "n_no_history": 0, "n_no_signal": 11}
- `23:52:55`   
- `23:52:55`     ── top 15 momentum picks ──
- `23:52:55`       MTSI    69.6 TIER_B_MOMENTUM       20d=28.1% 60d=28.7% volR=1.51
- `23:52:55`       ICHR    68.0 TIER_B_MOMENTUM       20d=38.7% 60d=110.8% volR=2.8
- `23:52:55`       AMZN    65.0 TIER_B_MOMENTUM       20d=28.0% 60d=30.1% volR=0.79
- `23:52:55`       PWR     65.0 TIER_B_MOMENTUM       20d=38.9% 60d=51.9% volR=1.22
- `23:52:55`       FIX     65.0 TIER_B_MOMENTUM       20d=38.1% 60d=59.9% volR=1.08
- `23:52:55`       IRM     65.0 TIER_B_MOMENTUM       20d=25.8% 60d=37.5% volR=1.28
- `23:52:55`       MTZ     65.0 TIER_B_MOMENTUM       20d=29.4% 60d=68.8% volR=0.94
- `23:52:55`       GNRC    65.0 TIER_B_MOMENTUM       20d=38.0% 60d=43.7% volR=0.7
- `23:52:55`       ESI     65.0 TIER_B_MOMENTUM       20d=27.1% 60d=46.9% volR=0.89
- `23:52:55`       MTRN    65.0 TIER_B_MOMENTUM       20d=30.5% 60d=30.1% volR=1.12
- `23:52:55`       EXTR    65.0 TIER_B_MOMENTUM       20d=49.9% 60d=55.6% volR=0.91
- `23:52:55`       MCHP    64.9 TIER_B_MOMENTUM       20d=45.9% 60d=29.6% volR=1.1
- `23:52:55`       CMI     64.8 TIER_B_MOMENTUM       20d=21.2% 60d=16.8% volR=1.8
- `23:52:55`       PI      64.7 TIER_B_MOMENTUM       20d=47.7% 60d=29.2% volR=1.29
- `23:52:55`       AVGO    64.5 TIER_B_MOMENTUM       20d=28.0% 60d=28.4% volR=0.92

# 5) Pump-list — what would have been caught EARLY

- `23:52:55`       AXTI   score= 25.0 MARGINAL               ret60d=347.0% rs60d=342.2
- `23:52:55`       LWLG   score= 29.0 MARGINAL               ret60d=408.2% rs60d=403.4
- `23:52:55`       AAOI   score= 19.0 MARGINAL               ret60d=307.6% rs60d=302.8
- `23:52:55`       AEHR   score= 15.0 MARGINAL               ret60d=245.5% rs60d=240.7
- `23:52:55`       SNDK   not in momentum (probably not in universe yet)
- `23:52:55`       ICHR   score= 68.0 TIER_B_MOMENTUM        ret60d=110.8% rs60d=106.0
- `23:52:55`       MRVL   score= 29.0 MARGINAL               ret60d=110.2% rs60d=105.4
- `23:52:55`       INTC   score= 27.5 MARGINAL               ret60d=113.8% rs60d=109.0
- `23:52:55`       VIAV   not in momentum (probably not in universe yet)
- `23:52:55`       LITE   score= 60.3 TIER_B_MOMENTUM        ret60d=80.2% rs60d=75.4
- `23:52:55`       CRDO   score= 19.0 MARGINAL               ret60d=73.8% rs60d=69.0
- `23:52:55`       MU     score= 27.5 MARGINAL               ret60d=62.2% rs60d=57.4
- `23:52:55`       TER    not in momentum (probably not in universe yet)
- `23:52:55`       WOLF   not in momentum (probably not in universe yet)
- `23:52:55`       ON     score= 30.0 MARGINAL               ret60d=57.5% rs60d=52.7
- `23:52:55`       QRVO   score= 63.4 TIER_B_MOMENTUM        ret60d=15.1% rs60d=10.3
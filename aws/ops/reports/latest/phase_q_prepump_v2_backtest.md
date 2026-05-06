
# 1) Force-deploy pre-pump v2

- `00:15:51`     source: 12444 chars
- `00:15:51`       ✓ v2.0 (calibrated thresholds)
- `00:15:51`       ✓ TIER_A_BREAKING
- `00:15:51`       ✓ OBV_STRONG_ACCUM
- `00:15:51`       ✓ UPTREND_NOT_PARABOLIC
- `00:15:56`     ✓ deployed at 2026-05-06T00:15:53.000+0000

# 2) Smoke invoke

- `00:16:07`     status: 200, dur: 10.5s
- `00:16:07`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 562, \"n_tier_a\": 5, \"n_tier_b\": 25, \"duration_s\": 9.7}"}
- `00:16:07`       START RequestId: ba6d5c7b-aab1-4cc3-821e-e8ad66c4ee22 Version: $LATEST
- `00:16:07`       [prepump-v2] starting v2.0 (calibrated thresholds)
- `00:16:07`       [prepump-v2] universe: 563 tickers
- `00:16:07`       [prepump-v2] OK: 562, no_history: 0
- `00:16:07`       [prepump-v2] wrote 231180b
- `00:16:07`       [prepump-v2] tier_a=5 tier_b=25
- `00:16:07`       [prepump-v2] TOP: [('MMP', 79.0, 'TIER_A_BREAKING'), ('SCWX', 79.0, 'TIER_A_BREAKING'), ('AGEN', 72.0, 'TIER_A_BREAKING'), ('PXD', 71.0, 'TIER_A_BREAKING'), ('CASY', 71.0, 'TIER_A_BREAKING'), ('EXAS', 69.0, 'TIER_B_BUILDING'), ('SATS', 67.0, 'TIER_B_BUILDING'), ('ANSS', 64.0, 'TIER_B_BUILDING')]
- `00:16:07`       END RequestId: ba6d5c7b-aab1-4cc3-821e-e8ad66c4ee22
- `00:16:07`       REPORT RequestId: ba6d5c7b-aab1-4cc3-821e-e8ad66c4ee22	Duration: 9813.94 ms	Billed Duration: 10348 ms	Memory Size: 1024 MB	Max Memory Used: 114 MB	Init Duration: 533.11 ms

# 3) Today's top 15 v2 signals

- `00:16:07`     generated_at: 2026-05-06T00:16:07+00:00
- `00:16:07`     stats: {"n_universe": 563, "n_evaluated": 562, "n_tier_a": 5, "n_tier_b": 25, "n_no_history": 0, "n_no_signal": 1}
- `00:16:07`   
- `00:16:07`     ── top 15 ──
- `00:16:07`       MMP     79.0 TIER_A_BREAKING         obv=+0.17  vc=1.60  liq=2.02  r60d=  +12%  r30d=   +7%
- `00:16:07`       SCWX    79.0 TIER_A_BREAKING         obv=+0.21  vc=19.08  liq=1.41  r60d=   +2%  r30d=   +1%
- `00:16:07`       AGEN    72.0 TIER_A_BREAKING         obv=+0.29  vc=0.86  liq=1.60  r60d=  +42%  r30d=  +13%
- `00:16:07`       PXD     71.0 TIER_A_BREAKING         obv=+0.28  vc=1.43  liq=1.01  r60d=  +18%  r30d=   +6%
- `00:16:07`       CASY    71.0 TIER_A_BREAKING         obv=+0.45  vc=0.93  liq=1.77  r60d=  +30%  r30d=  +27%
- `00:16:07`       EXAS    69.0 TIER_B_BUILDING         obv=+0.25  vc=13.61  liq=0.99  r60d=   +3%  r30d=   +2%
- `00:16:07`       SATS    67.0 TIER_B_BUILDING         obv=+0.33  vc=1.94  liq=0.88  r60d=   +5%  r30d=   +7%
- `00:16:07`       ANSS    64.0 TIER_B_BUILDING         obv=+0.15  vc=1.11  liq=2.20  r60d=  +26%  r30d=  +12%
- `00:16:07`       COP     62.0 TIER_B_BUILDING         obv=+0.26  vc=0.97  liq=1.02  r60d=  +15%  r30d=   -3%
- `00:16:07`       BK      62.0 TIER_B_BUILDING         obv=+0.39  vc=0.83  liq=1.07  r60d=   +6%  r30d=  +14%
- `00:16:07`       OXY     62.0 TIER_B_BUILDING         obv=+0.21  vc=0.84  liq=0.96  r60d=  +28%  r30d=   -2%
- `00:16:07`       HUM     62.0 TIER_B_BUILDING         obv=+0.28  vc=1.19  liq=0.95  r60d=  +24%  r30d=  +41%
- `00:16:07`       NI      62.0 TIER_B_BUILDING         obv=+0.24  vc=0.95  liq=1.31  r60d=   +9%  r30d=   +6%
- `00:16:07`       FRT     62.0 TIER_B_BUILDING         obv=+0.22  vc=0.95  liq=1.23  r60d=   +9%  r30d=  +11%
- `00:16:07`       PLXS    62.0 TIER_B_BUILDING         obv=+0.23  vc=1.00  liq=1.18  r60d=  +29%  r30d=  +31%

# 4) HISTORICAL BACKTEST v2 — pump-list catch capability

- `00:16:07`     At various days BEFORE each name's breakout, what would v2 score?
- `00:16:07`   
- `00:16:07`     ICHR (final +138%, breakout 2025-08-12):
- `00:16:07`       ❌ MISSED
- `00:16:07`   
- `00:16:07`     INTC (final +122%, breakout 2025-09-24):
- `00:16:07`          2025-08-26 (20 days before breakout):  40.0 WATCH              future_to_breakout=+   28% flags=LIQ_FAST,UP_NOT_PAR
- `00:16:07`          2025-09-10 (10 days before breakout):  48.0 WATCH              future_to_breakout=+   26% flags=LIQ_EXP,UP_NOT_PAR
- `00:16:07`          2025-09-17 (5 days before breakout):  38.0 MARGINAL           future_to_breakout=+   25% flags=UP_NOT_PAR
- `00:16:07`       ❌ MISSED
- `00:16:07`   
- `00:16:07`     LITE (final +116%, breakout 2025-10-29):
- `00:16:07`       🎯 2025-08-26 (45 days before breakout):  72.0 TIER_A_BREAKING    future_to_breakout=+   72% flags=OBV_ACCUM,VC_STRONG,LIQ_EXP
- `00:16:07`       🎯 2025-09-17 (30 days before breakout):  80.0 TIER_A_BREAKING    future_to_breakout=+   31% flags=OBV_STRONG,VC_STRONG,LIQ_EXP
- `00:16:07`       🎯 2025-10-01 (20 days before breakout):  67.0 TIER_B_BUILDING    future_to_breakout=+   25% flags=OBV_ACCUM,VC_MOD,LIQ_FAST
- `00:16:07`       🎯 2025-10-15 (10 days before breakout):  55.0 TIER_B_BUILDING    future_to_breakout=+   37% flags=OBV_ACCUM,UP_NOT_PAR
- `00:16:07`       🎯 2025-10-22 (5 days before breakout):  60.0 TIER_B_BUILDING    future_to_breakout=+   36% flags=OBV_ACCUM,UP_NOT_PAR
- `00:16:07`       ✓ CAUGHT (>= TIER_B at some point)
- `00:16:07`   
- `00:16:07`     CRDO (final +101%, breakout 2025-09-10):
- `00:16:07`          2025-08-26 (10 days before breakout):  30.0 MARGINAL           future_to_breakout=+   37% flags=
- `00:16:07`          2025-09-03 (5 days before breakout):  40.0 WATCH              future_to_breakout=+   31% flags=VC_MOD
- `00:16:07`       ❌ MISSED
- `00:16:07`   
- `00:16:07`     MRVL — no clear breakout (smooth uptrend)
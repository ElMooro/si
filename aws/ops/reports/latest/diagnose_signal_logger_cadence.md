# Signal-logger cadence investigation

**Status:** success  
**Duration:** 2.7s  
**Finished:** 2026-04-25T10:05:41+00:00  

## Data

| eb_rules | last_24h_logs | recent_avg_per_day | signal_types |
|---|---|---|---|
| 1 | 274 | 5.3 | 15 |

## Log
## 1. EventBridge rules for signal-logger

- `10:05:38`   Rules targeting justhodl-signal-logger: 1
- `10:05:38`     justhodl-signal-logger-6h                          state=ENABLED    schedule=rate(6 hours)
## 2. CloudWatch invocation history (last 30 days)

- `10:05:39`   30-day total: 129 invocations, 1 errors
- `10:05:39`   Daily breakdown:
- `10:05:39`     2026-04-10: inv=  4 err=0 ████
- `10:05:39`     2026-04-11: inv=  4 err=0 ████
- `10:05:39`     2026-04-12: inv=  4 err=0 ████
- `10:05:39`     2026-04-13: inv=  4 err=0 ████
- `10:05:39`     2026-04-14: inv=  4 err=0 ████
- `10:05:39`     2026-04-15: inv=  4 err=0 ████
- `10:05:39`     2026-04-16: inv=  4 err=0 ████
- `10:05:39`     2026-04-17: inv=  4 err=0 ████
- `10:05:39`     2026-04-18: inv=  4 err=0 ████
- `10:05:39`     2026-04-19: inv=  4 err=0 ████
- `10:05:39`     2026-04-20: inv=  4 err=0 ████
- `10:05:39`     2026-04-21: inv=  4 err=0 ████
- `10:05:39`     2026-04-22: inv=  6 err=1 ██████
- `10:05:39`     2026-04-23: inv=  4 err=0 ████
- `10:05:39`     2026-04-24: inv= 11 err=0 ███████████
- `10:05:39` 
  Recent 7-day avg: 5.3 invocations/day
- `10:05:39` ⚠   ⚠  Cadence unexpected (expected 4/day for 6h schedule)
## 3. Most recent log stream — what does the logger do?

- `10:05:40`   Stream: 2026/04/25/[$LATEST]ff4f02c5f6d24222bd586e52c84a6528
- `10:05:40`   Last event: 2026-04-25 09:10:22.160000+00:00
- `10:05:40`   Last 80 log lines:
- `10:05:40`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `10:05:40`     START RequestId: 1d83e0a8-c4df-48cf-9f3b-2ee234065f8f Version: $LATEST
- `10:05:40`     [REGIME] snapshot: regime=BEAR, score=43
- `10:05:40`     [LOG] khalid_index=MODERATE NEUTRAL conf=0.14 baseline=$713.94
- `10:05:40`     [LOG] edge_regime=BEAR DOWN conf=0.70 baseline=$713.94
- `10:05:40`     [LOG] crypto_fear_greed=FEAR UP conf=0.60 baseline=$77630.00
- `10:05:40`     [LOG] crypto_risk_score=HIGH DOWN conf=0.32 baseline=$77630.00
- `10:05:40`     [LOG] edge_composite=60.0 NEUTRAL conf=0.20 baseline=$713.94
- `10:05:40`     [LOG] momentum_uso=-1.72% DOWN conf=0.57 baseline=$132.40
- `10:05:40`     [LOG] plumbing_stress=MODERATE NEUTRAL conf=0.50 baseline=$713.94
- `10:05:40`     [LOG] ml_risk=60.0 NEUTRAL conf=0.20 baseline=$713.94
- `10:05:40`     [LOG] carry_risk=25.0 UP conf=0.50 baseline=$713.94
- `10:05:40`     [LOG] market_phase=PRE-CRISIS DOWN conf=0.75 baseline=$713.94
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.92 baseline=$799.55
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.11 baseline=$117.50
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.89 baseline=$336.09
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.78 baseline=$881.64
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.92 baseline=$323.46
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.78 baseline=$118.00
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.92 baseline=$520.80
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.92 baseline=$409.05
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.92 baseline=$1722.37
- `10:05:40`     [LOG] screener_top_pick=TOP_10 OUTPERFORM conf=0.89 baseline=$115.23
- `10:05:40`     [LOG] screener_top_pick=TOP_25 OUTPERFORM conf=0.56 baseline=$989.90
- `10:05:40`     [LOG] screener_top_pick=TOP_25 OUTPERFORM conf=0.89 baseline=$448.29
- `10:05:40`     [LOG] screener_top_pick=TOP_25 OUTPERFORM conf=0.89 baseline=$144.46
- `10:05:40`     [LOG] screener_top_pick=TOP_25 OUTPERFORM conf=0.89 baseline=$869.90
- `10:05:40`     [LOG] screener_top_pick=TOP_25 OUTPERFORM conf=0.89 baseline=$84.71
- `10:05:40`     [DONE] Logged 25 signals
- `10:05:40`     END RequestId: 1d83e0a8-c4df-48cf-9f3b-2ee234065f8f
- `10:05:40`     REPORT RequestId: 1d83e0a8-c4df-48cf-9f3b-2ee234065f8f	Duration: 8855.35 ms	Billed Duration: 9398 ms	Memory Size: 256 MB	Max Memory Used: 113 MB	Init Duration: 542.64 ms
## 4. Source code — what signal types does logger write?

- `10:05:40`   aws/lambdas/justhodl-signal-logger/source/lambda_function.py — 299 LOC
- `10:05:40`   Hardcoded signal_types: []
- `10:05:40`   put_item calls: 1, batch_writer calls: 0
## 5. Signal-logger output cadence (from DDB timestamps)

- `10:05:41`   Total signals scanned: 4829
- `10:05:41` 
  Per-signal-type cadence:
- `10:05:41`   signal_type                   count        first         last    median_gap    expected
- `10:05:41`   screener_top_pick              2860  03-11 09:13  04-25 09:10            0s      6 hour
- `10:05:41`   edge_regime                     188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   market_phase                    188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   edge_composite                  188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   carry_risk                      188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   khalid_index                    188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   crypto_fear_greed               188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   ml_risk                         188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   crypto_risk_score               188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   plumbing_stress                 188  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   momentum_uso                    120  03-12 00:51  04-25 09:10          6.0h      6 hour
- `10:05:41`   momentum_gld                     75  03-12 21:10  04-24 09:10          6.0h      6 hour
- `10:05:41`   momentum_spy                     51  03-12 21:10  04-23 09:10          6.0h      6 hour
- `10:05:41`   momentum_tlt                     28  03-12 00:51  04-19 21:10          6.0h      6 hour
- `10:05:41`   momentum_uup                      3  03-20 03:10  04-08 21:10          9.9d      6 hour
## 6. Cadence verdict

- `10:05:41`   EB schedule: rate(6 hours) → expected median gap ~21600s
- `10:05:41` 
  Aligned with EB schedule (13):
- `10:05:41`     ✅ edge_regime
- `10:05:41`     ✅ market_phase
- `10:05:41`     ✅ edge_composite
- `10:05:41`     ✅ carry_risk
- `10:05:41`     ✅ khalid_index
- `10:05:41`     ✅ crypto_fear_greed
- `10:05:41`     ✅ ml_risk
- `10:05:41`     ✅ crypto_risk_score
- `10:05:41`     ✅ plumbing_stress
- `10:05:41`     ✅ momentum_uso
- `10:05:41`     ✅ momentum_gld
- `10:05:41`     ✅ momentum_spy
- `10:05:41`     ✅ momentum_tlt
- `10:05:41` 
  Misaligned (2):
- `10:05:41`     ⚠  screener_top_pick            gap=        0s (0.0x of expected)
- `10:05:41`     ⚠  momentum_uup                 gap=      9.9d (39.5x of expected)
## 7. Burst analysis — recency of last-24h signals

- `10:05:41`   Last 24h: 274 signal logs total
- `10:05:41`   By hour:
- `10:05:41`     2026-04-24 15:   24 signals  ████
- `10:05:41`     2026-04-24 21:   25 signals  █████
- `10:05:41`     2026-04-24 23:   75 signals  ███████████████
- `10:05:41`     2026-04-25 00:  100 signals  ████████████████████
- `10:05:41`     2026-04-25 03:   25 signals  █████
- `10:05:41`     2026-04-25 09:   25 signals  █████
## 8. Save audit doc

- `10:05:41` ✅   Wrote aws/ops/audit/signal_logger_cadence_2026-04-25.md
- `10:05:41` Done

- `10:41:13`     source: 15635 chars

# 1) Build zip + create/update Lambda

- `10:41:13`     zip: 15821b
- `10:41:13`     creating new
- `10:41:17`     ✓ deployed at 2026-05-06T10:41:13.645+0000

# 2) Schedule daily 13:15 UTC

- `10:41:17`     ✓ permission added

# 3) Smoke invoke

- `10:41:19`     status: 200, dur: 1.7s
- `10:41:19`     body: {"statusCode": 200, "body": "{\"regime\": \"REFLATION\", \"risk_score\": 31.0, \"risk_label\": \"STRONG_RISK_ON\", \"n_breaks\": 6, \"n_alerts\": 3, \"duration_s\": 0.8}"}
- `10:41:19`       START RequestId: e58c9c20-dbe7-46ba-8d78-7c97af513dbd Version: $LATEST
- `10:41:19`       [regime] starting v1.0
- `10:41:19`       [regime] fetched 8/8 histories
- `10:41:19`       [regime] wrote 6270b
- `10:41:19`       [regime] regime_20d=REFLATION conf=85 risk=31.0 (STRONG_RISK_ON)
- `10:41:19`       [regime] top breaks: [(['USO', 'BITO'], -0.498), (['USO', 'GLD'], -0.462), (['TLT', 'BITO'], 0.406)]
- `10:41:19`       END RequestId: e58c9c20-dbe7-46ba-8d78-7c97af513dbd
- `10:41:19`       REPORT RequestId: e58c9c20-dbe7-46ba-8d78-7c97af513dbd	Duration: 913.47 ms	Billed Duration: 1466 ms	Memory Size: 512 MB	Max Memory Used: 108 MB	Init Duration: 552.35 ms

# 4) Inspect output — current macro regime

- `10:41:19`     generated_at: 2026-05-06T10:41:19+00:00
- `10:41:19`     stats: {"n_assets_loaded": 8, "n_correlation_breaks": 6, "n_alerts": 3}
- `10:41:19`   
- `10:41:19`     ── REGIMES (multi-horizon) ──
- `10:41:19`       regime_5d: RISK_ON conf=65  risk_score=6.06 (STRONG_RISK_ON)
- `10:41:19`         → Equities + crypto rallying together
- `10:41:19`       regime_20d: REFLATION conf=85  risk_score=31.0 (STRONG_RISK_ON)
- `10:41:19`         → Risk assets rallying, bonds + dollar declining
- `10:41:19`       regime_60d: RISK_ON conf=65  risk_score=39.92 (STRONG_RISK_ON)
- `10:41:19`         → Equities + crypto rallying together
- `10:41:19`   
- `10:41:19`     ── 20D ASSET RETURNS ──
- `10:41:19`       SPY       +9.79%
- `10:41:19`       TLT       -1.40%
- `10:41:19`       GLD       -3.14%
- `10:41:19`       UUP       -0.90%
- `10:41:19`       HYG       +0.25%
- `10:41:19`       USO       +4.41%
- `10:41:19`       BITO     +18.06%
- `10:41:19`       VIXY     -18.19%
- `10:41:19`   
- `10:41:19`     ── TOP 8 CORRELATION BREAKS (30d vs 90d baseline) ──
- `10:41:19`       USO <-> BITO  c30d=-0.64  c90d=-0.14  Δ=-0.50  (shift)
- `10:41:19`       USO <-> GLD  c30d=-0.48  c90d=-0.02  Δ=-0.46  (shift)
- `10:41:19`       TLT <-> BITO  c30d=0.40  c90d=-0.01  Δ=+0.41  (shift)
- `10:41:19`       VIXY <-> UUP  c30d=0.61  c90d=0.24  Δ=+0.37  (shift)
- `10:41:19`       SPY <-> UUP  c30d=-0.72  c90d=-0.36  Δ=-0.35  (shift)
- `10:41:19`       GLD <-> HYG  c30d=0.57  c90d=0.24  Δ=+0.33  (shift)
- `10:41:19`   
- `10:41:19`     ── 30D CORRELATION MATRIX (compact) ──
- `10:41:19`            SPY     TLT     GLD     UUP     HYG     USO     BITO    VIXY  
- `10:41:19`       SPY    +1.00   +0.38   +0.52   -0.72   +0.83   -0.68   +0.69   -0.87 
- `10:41:19`       TLT    +0.38   +1.00   +0.35   -0.06   +0.60   -0.30   +0.40   -0.34 
- `10:41:19`       GLD    +0.52   +0.35   +1.00   -0.54   +0.57   -0.48   +0.34   -0.34 
- `10:41:19`       UUP    -0.72   -0.06   -0.54   +1.00   -0.62   +0.66   -0.38   +0.61 
- `10:41:19`       HYG    +0.83   +0.60   +0.57   -0.62   +1.00   -0.44   +0.48   -0.79 
- `10:41:19`       USO    -0.68   -0.30   -0.48   +0.66   -0.44   +1.00   -0.64   +0.50 
- `10:41:19`       BITO    +0.69   +0.40   +0.34   -0.38   +0.48   -0.64   +1.00   -0.60 
- `10:41:19`       VIXY    -0.87   -0.34   -0.34   +0.61   -0.79   +0.50   -0.60   +1.00 
- `10:41:19`   
- `10:41:19`     ── ALERTS ──
- `10:41:19`       [MEDIUM] CORRELATION_BREAK: USO/BITO correlation: -0.137 → -0.635 (Δ-0.498, shift)
- `10:41:19`       [MEDIUM] CORRELATION_BREAK: USO/GLD correlation: -0.018 → -0.48 (Δ-0.462, shift)
- `10:41:19`       [MEDIUM] CORRELATION_BREAK: TLT/BITO correlation: -0.009 → 0.397 (Δ0.406, shift)
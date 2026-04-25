# Phase 1B — Cross-Asset Divergence Scanner

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-04-25T16:00:19+00:00  

## Data

| invoke_s | n_alert | n_extreme | n_processed | top_divergence | top_z | zip_size |
|---|---|---|---|---|---|---|
| 1.4 | 0 | 3 | 10 | Nasdaq vs 10Y Yield | 2.727 | 14405 |

## Log
## 1. Verify data dependencies

- `16:00:08`   data/report.json: 1,724,366B, age 5.0min
- `16:00:08`   data/fred-cache-secretary.json: 9,358B, age 154.2min
- `16:00:09` 
  Stock history depth check:
- `16:00:09`     GLD    120 bars
- `16:00:09`     IWM    120 bars
- `16:00:09`     EEM    120 bars
- `16:00:09`     QQQ    120 bars
- `16:00:09`     XLF    120 bars
- `16:00:09`     IBIT   0 bars ← short
- `16:00:09`     XLP    120 bars
- `16:00:09`     XLY    120 bars
- `16:00:09`     XLE    120 bars
- `16:00:09`     TIP    120 bars
- `16:00:09`     IEF    120 bars
- `16:00:09`     XLV    120 bars
- `16:00:09`     SPY    120 bars
- `16:00:09` 
  FRED history depth check:
- `16:00:09`     DGS10           30 pts
- `16:00:09`     T10YIE          30 pts
- `16:00:09`     T10Y2Y          30 pts
- `16:00:09`     DTWEXBGS        30 pts
- `16:00:09`     T5YIE           30 pts
- `16:00:09`     VIXCLS          30 pts
- `16:00:09`     BAMLH0A0HYM2    30 pts
## 2. Set up justhodl-divergence-scanner Lambda

- `16:00:09` ✅   Wrote source: 14,248B, 377 LOC
- `16:00:13` ✅   Created justhodl-divergence-scanner
## 3. Test invoke

- `16:00:17` ✅   Invoked in 1.4s
- `16:00:17` 
  Response body:
- `16:00:17`     n_processed               10
- `16:00:17`     n_extreme                 3
- `16:00:17`     n_alert                   0
- `16:00:17`     top_divergence            Nasdaq vs 10Y Yield
- `16:00:17`     top_z                     2.727
## 4. divergence/current.json — full report

- `16:00:18`   Processed: 10/12
- `16:00:18`   Missing data: 2
- `16:00:18`   At >2σ extreme: 3
- `16:00:18`   At >3σ alert-worthy: 0
- `16:00:18` 
  All relationships (sorted by |z|):
- `16:00:18`     Nasdaq vs 10Y Yield                 z=+2.73 R²=0.19  slope=✓ ← EXTREME
- `16:00:18`       → QQQ appears RICH vs DGS10
- `16:00:18`     TIP vs IEF                          z=+2.71 R²=0.42  slope=✓ ← EXTREME
- `16:00:18`       → TIP appears RICH vs IEF
- `16:00:18`     XLV vs SPY                          z=-2.52 R²=0.15  slope=✓ ← EXTREME
- `16:00:18`       → XLV appears CHEAP vs SPY
- `16:00:18`     Small Caps vs 2s10s Curve           z=+1.49 R²=0.07  slope=✓
- `16:00:18`       → IWM appears RICH vs T10Y2Y
- `16:00:18`     XLP vs XLY (defensive/cyclical)     z=-1.02 R²=0.19  slope=✓
- `16:00:18`       → XLP appears CHEAP vs XLY
- `16:00:18`     Banks vs 2s10s Curve                z=+0.62 R²=0.06  slope=✓
- `16:00:18`       → XLF appears RICH vs T10Y2Y
- `16:00:18`     EM vs Dollar                        z=+0.50 R²=0.39  slope=✓
- `16:00:18`       → EEM appears RICH vs DTWEXBGS
- `16:00:18`     Energy vs 5Y Breakevens             z=-0.48 R²=0.12  slope=⚠
- `16:00:18`       → XLE appears CHEAP vs T5YIE
- `16:00:18`     Gold vs Real Rates                  z=-0.45 R²=0.60  slope=✓
- `16:00:18`       → GLD appears CHEAP vs real_rate_10y
- `16:00:18`     VIX vs HY OAS                       z=+0.34 R²=0.86  slope=✓
- `16:00:18`       → VIXCLS appears RICH vs BAMLH0A0HYM2
- `16:00:18`     BTC vs Nasdaq                       missing_data
- `16:00:18`     Gold vs BTC                         missing_data
## 5. EventBridge schedule cron(0 13 ? * MON-FRI *)

- `16:00:18` ✅   Created rule cron(0 13 ? * MON-FRI *)
- `16:00:19` ✅   Added invoke permission
- `16:00:19` Done

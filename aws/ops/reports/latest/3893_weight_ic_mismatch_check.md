# ops 3893 — complete factor-ic + backtest-harness + the weight-vs-IC cross-check

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-07-25T23:28:45+00:00  

## Data

| composite_mean_ic | composite_t_stat | horizon_days | maturity | n_engines_total | n_factors_overweighted_despite_negative_ic | n_graded_with_real_data | n_insufficient_data | n_pass_field | n_rules_total | panels_matured | universe_n | universe_priced | which |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| -0.1219 | -15.11 |  | MATURE |  |  |  |  |  |  | 35 |  | 508 |  |
|  |  |  |  |  | 3 |  |  |  |  |  |  |  | ['growth', 'momentum', 'sentiment'] |
|  |  | 21 |  |  |  |  |  | 4 | 8 |  | 1500 |  |  |
|  |  |  |  | 10 |  | 1 | 9 |  |  |  |  |  |  |

## Log
## 1. factor-ic — full 8-factor table + composite (re-confirm, complete)

- `23:28:44`   analysts       mean_ic=0.1844     t_stat=12.61      n_dates=35 quintile_spread=4.4
- `23:28:44`   growth         mean_ic=-0.111     t_stat=-9.9       n_dates=35 quintile_spread=-3.72
- `23:28:44`   insiders       mean_ic=0.0806     t_stat=7.8        n_dates=35 quintile_spread=3.2
- `23:28:44`   momentum       mean_ic=-0.1859    t_stat=-10.65     n_dates=35 quintile_spread=-5.58
- `23:28:44`   options_flow   mean_ic=None       t_stat=None       n_dates=0 quintile_spread=None
- `23:28:44`   quality        mean_ic=-0.05      t_stat=-10.11     n_dates=35 quintile_spread=-1.16
- `23:28:44`   sentiment      mean_ic=-0.1133    t_stat=-16.84     n_dates=35 quintile_spread=-3.5
- `23:28:44`   smart_money    mean_ic=0.0612     t_stat=7.6        n_dates=35 quintile_spread=0.6
## 2. calibration-latest — current model weights, direct comparison against factor-ic

- `23:28:44`   factor          weight    measured_IC   t_stat    mismatch?
- `23:28:44`   growth         0.17      -0.111        -9.9      *** OVERWEIGHTED DESPITE NEGATIVE IC ***
- `23:28:44`   quality        0.16      -0.05         -10.11    
- `23:28:44`   smart_money    0.16      0.0612        7.6       
- `23:28:44`   momentum       0.14      -0.1859       -10.65    *** OVERWEIGHTED DESPITE NEGATIVE IC ***
- `23:28:44`   insiders       0.11      0.0806        7.8       
- `23:28:44`   sentiment      0.1       -0.1133       -16.84    *** OVERWEIGHTED DESPITE NEGATIVE IC ***
- `23:28:44`   analysts       0.08      0.1844        12.61     
- `23:28:44`   options_flow   0.08      None          None      
- `23:28:44`   calibration-latest's OWN internal IC computation (separate from factor-ic.json): {"quality": {"1d": {"n": 0, "insufficient": true}, "7d": {"n": 0, "insufficient": true}, "30d": {"n": 0, "insufficient": true}, "90d": {"n": 0, "insufficient": true}}, "growth": {"1d": {"n": 0, "insufficient": true}, "7d": {"n": 0, "insufficient": true}, "30d": {"n": 0, "insufficient": true}, "90d": {"n": 0, "insufficient": true}}, "momentum": {"1d": {"n": 0, "insufficient": true}, "7d": {"n": 0, "insufficient": true}, "30d": {"n": 0, "insufficient": true}, "90d": {"n": 0, "insufficient": true}}, "smart_money": {"1d": {"n": 0, "insufficient": true}, "7d": {"n": 0, "insufficient": true}, "30d":
## 3. backtest-harness — COMPLETE rule list, every PASS/FAIL

- `23:28:44`   basing_breakout        family=pre-pump/basing      n=1050 sr=0.65 hit=56.4% avg=1.53% maxdd=-37.3% gate=0.05 PASS=True
- `23:28:44`   momentum_breakout      family=trend/ignition       n=3312 sr=0.47 hit=51.8% avg=2.52% maxdd=-25.6% gate=0.03 PASS=True
- `23:28:44`   relative_strength      family=momentum/leaders     n=3596 sr=0.38 hit=49.5% avg=2.82% maxdd=-65.1% gate=0.03 PASS=False
- `23:28:44`   ma_cross               family=trend                n=1230 sr=0.37 hit=50.9% avg=2.09% maxdd=-38.9% gate=0.04 PASS=True
- `23:28:44`   momentum_consistency   family=quality-momentum     n=2454 sr=0.32 hit=50.0% avg=1.51% maxdd=-47.7% gate=0.03 PASS=False
- `23:28:44`   vol_squeeze_break      family=volatility-squeeze   n=2468 sr=0.27 hit=51.6% avg=1.73% maxdd=-79.6% gate=0.03 PASS=False
- `23:28:44`   pullback_in_uptrend    family=upside-radar         n=1390 sr=0.15 hit=49.2% avg=0.77% maxdd=-30.2% gate=0.03 PASS=True
- `23:28:44`   deep_drawdown_buy      family=mean-reversion       n=1374 sr=0.11 hit=47.7% avg=0.75% maxdd=-68.7% gate=0.04 PASS=False
## 4. calibration-fleet — the summary block (never reached in ops 3892's truncation)

- `23:28:45`   summary: {"engines_total": 10, "predictive": 0, "weak": 0, "noise": 1, "contrarian": 0, "insufficient": 9, "top_ic": [["Global Stress Matrix", -0.0195]]}
- `23:28:45`   graded engines (n_paired>0), sorted by IC:
- `23:28:45`     global_stress        n=269    ic=-0.0195 hit=52.1% rating=NOISE
- `23:28:45` ✅ PROBE COMPLETE

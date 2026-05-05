# 1) horizons.html live + checks

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-05-05T10:37:13+00:00  

## Log
- `10:37:12`   ✓ status=200, size=17,019b
- `10:37:12`     ✓ title
- `10:37:12`     ✓ nav active
- `10:37:12`     ✓ uplift list
- `10:37:12`     ✓ matrix table
- `10:37:12`     ✓ loads calibration
- `10:37:12`     ✓ weight color logic
- `10:37:12`     ✓ auto-refresh
# 2) Calibration JSON has multi-horizon fields

- `10:37:13`   generated_at:        2026-05-05T10:29:44.898506+00:00
- `10:37:13`   total_outcomes:      1620
- `10:37:13`   window_weights:      27 signals
- `10:37:13`   recommended_horizon: 27 signals
- `10:37:13`   flat weights:        39 signals
- `10:37:13` 
- `10:37:13`   Top 13 horizon-uplifts (best-window weight - flat weight):
- `10:37:13`     edge_composite                flat=0.51 → day_1: w=1.29  acc=80% n=44  Δ+0.78
- `10:37:13`     crypto_fear_greed             flat=0.86 → day_14: w=1.44  acc=97% n=37  Δ+0.58
- `10:37:13`     plumbing_stress               flat=0.99 → day_14: w=1.41  acc=92% n=25  Δ+0.42
- `10:37:13`     crisis_sofr_iorb              flat=0.39 → day_7: w=0.70  acc=25% n=4  Δ+0.31
- `10:37:13`     crisis_obfr_iorb              flat=0.40 → day_7: w=0.70  acc=25% n=4  Δ+0.30
- `10:37:13`     crisis_hy_oas_vs_hyg          flat=0.96 → day_3: w=1.22  acc=75% n=20  Δ+0.26
- `10:37:13`     momentum_spy                  flat=1.06 → day_7: w=1.31  acc=100% n=8  Δ+0.25
- `10:37:13`     momentum_uup                  flat=0.54 → day_1: w=0.70  acc=0% n=2  Δ+0.16
- `10:37:13`     crisis_rate_diff_jpy_3m       flat=0.54 → day_7: w=0.70  acc=0% n=4  Δ+0.16
- `10:37:13`     crisis_broad_dollar_vs_spy    flat=0.54 → day_7: w=0.70  acc=0% n=4  Δ+0.16
- `10:37:13`     crisis_broad_dollar_vs_eem    flat=0.54 → day_7: w=0.70  acc=0% n=4  Δ+0.16
- `10:37:13`     crisis_rate_diff_eur_3m       flat=0.54 → day_7: w=0.70  acc=0% n=4  Δ+0.16
- `10:37:13`     ml_risk                       flat=1.30 → day_30: w=1.45  acc=100% n=30  Δ+0.15
- `10:37:13` 
- `10:37:13`   Spotcheck: crisis_sofr_iorb (was floored at 0.40)
- `10:37:13`     day_3: weight=0.40  accuracy=30%  n=20
- `10:37:13`     day_7: weight=0.70  accuracy=25%  n=4
# 3) Horizons tab visible across key pages

- `10:37:13`   ✓ today.html                 Horizons link: True
- `10:37:13`   ✓ brief.html                 Horizons link: True
- `10:37:13`   ✓ calls.html                 Horizons link: True
- `10:37:13`   ✓ performance.html           Horizons link: True
- `10:37:13`   ✓ backtest.html              Horizons link: True
- `10:37:13`   ✓ weights.html               Horizons link: True
- `10:37:13`   ✓ accuracy.html              Horizons link: True

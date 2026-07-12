# ops 3170 — Thesis Engine v2 (1990-2026)

**Status:** success  
**Duration:** 112.3s  
**Finished:** 2026-07-12T22:20:13+00:00  

## Error

```
SystemExit: 0
```

## Data

| elapsed_s | families | fdr_pass | fdr_survivors | firing | firing_now | history_start | n_fails | n_warns | pass1_series | pass1_status | pass1_theses | pass1_weeks | regime_now | series_cached | signals_logged | stable | theses | verdict | weeks | weeks_easing | weeks_neutral | weeks_tightening |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  | 128 | LIVE | 25 | 1746 |  |  |  |  |  |  |  |  |  |  |
|  | 2 | 0 |  | 0 |  |  |  |  |  |  |  |  |  |  |  | 0 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | NEUTRAL |  |  |  |  |  |  | 0 | 1746 | 0 |
| 96.4 |  |  | 0 |  | 1 | 1990-01-01 |  |  |  |  |  |  |  | 128 | 0 |  | 25 |  | 1746 |  |  |  |
|  |  |  |  |  |  |  | 0 | 3 |  |  |  |  |  |  |  |  |  | PASS |  |  |  |  |

## Log
## 1. Deploy v2

- `22:18:21`   zip: 66044 bytes
## 1. Lambda

- `22:18:22`   Lambda exists — updating
- `22:18:30` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `22:18:31`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `22:18:31` ✅   ✓ target → justhodl-thesis-engine
- `22:18:31` ✅   ✓ added invoke permission
## 2. Deep run (first pass backfills 1990→today)

- `22:18:31` invoke 1 fired
## 3. FAMILY COMPOSITES — the aggregated evidence

- `22:20:13`   STRESS     n=6  comp -0.234 | SPY13w  -0.82% hit-edge  -4.6pp t= -0.42 n_eff= 26.0 | halves   -3.3/  1.67 | sign 6/6 p=0.0312
- `22:20:13`   LIQUIDITY  n=4  comp -1.143 | SPY13w  -0.15% hit-edge  -1.2pp t= -0.07 n_eff= 18.0 | halves  -4.85/   2.0 | sign 0/4 p=0.125
- `22:20:13` ── REGIME-CONDITIONAL (each vs that regime's own base rate):
- `22:20:13`   STRESS     NEUTRAL    SPY13w   2.05% vs regime base   2.87% → excess  -0.82% t= -0.42 n_eff=26.0
- `22:20:13`   LIQUIDITY  NEUTRAL    SPY13w   2.72% vs regime base   2.87% → excess  -0.15% t= -0.07 n_eff=18.0
## 4. Per-thesis detail (36 years of evidence)

- `22:20:13` ── SPY base rates since 1990: 4w 0.89% · 13w 2.87% · 26w 5.8%
- `22:20:13` ── ranked by overlap-corrected t on 13w forward SPY:
- `22:20:13`   Red list                             hist  1993-34 act   7.7% | excess    2.0% hit-edge   8.5pp t=  1.44 n_eff= 34.2
- `22:20:13`   Commercial Banks                     hist  1993-42 act  22.2% | excess  -1.91% hit-edge -12.2pp t= -1.17 n_eff= 30.2
- `22:20:13`   71699273                             hist  1993-42 act  30.0% | excess  -1.96% hit-edge -11.2pp t= -1.09 n_eff= 29.8
- `22:20:13`   Financial Conditions                 hist  1994-29 act   9.1% | excess  -2.01% hit-edge -13.7pp t= -0.99 n_eff= 26.9
- `22:20:13`   Draining Liquidity                   hist  2014-16 act   0.0% | excess   1.63% hit-edge   8.5pp t=  0.91 n_eff= 12.5
- `22:20:13`   Economy                              hist  1993-34 act  40.0% | excess  -1.77% hit-edge -10.6pp t= -0.88 n_eff= 28.1
- `22:20:13`   fed plumbing                         hist  1993-42 act   4.8% | excess  -1.68% hit-edge -11.4pp t= -0.87 n_eff= 27.2
- `22:20:13`   Feds Monetary Policy: Fed tighten mo hist  2003-28 act   0.0% | excess   1.51% hit-edge   7.0pp t=  0.84 n_eff= 21.0
- `22:20:13`   Financial Crisis Signs               hist  1993-42 act  14.3% | excess  -1.41% hit-edge -10.3pp t= -0.76 n_eff= 29.7
- `22:20:13`   Commercial banks                     hist  1993-42 act  20.0% | excess  -1.24% hit-edge -11.7pp t= -0.75 n_eff= 27.3
- `22:20:13`   Danger                               hist  2003-35 act  22.2% | excess  -1.64% hit-edge  -8.6pp t= -0.71 n_eff= 20.5
- `22:20:13`   Fed Interest Rates                   hist  1997-30 act  10.0% | excess  -1.37% hit-edge -11.4pp t= -0.71 n_eff= 23.8
- `22:20:13`   Financial Crisis                     hist  1993-34 act  11.1% | excess  -1.02% hit-edge  -6.5pp t= -0.55 n_eff= 27.2
- `22:20:13`   Financial Stress                     hist  1993-34 act  10.0% | excess  -0.98% hit-edge  -8.5pp t= -0.53 n_eff= 29.9
- `22:20:13`   Bitcoin : Nikkei TOP and Bottom in U hist  1993-34 act   3.7% | excess   0.91% hit-edge   3.9pp t=   0.5 n_eff= 29.4
- `22:20:13`   Bitcoin - Global Liquidity: GOLD ALW hist  1993-34 act  12.9% | excess    0.8% hit-edge   3.3pp t=  0.44 n_eff= 30.2
## 5. Page

- `22:20:13` ✅ theses.html live
- `22:20:13` ⚠ no regime-gated edge either — the panels are context, not timing, in every policy state
- `22:20:13` ⚠ no composite clears FDR+stability — aggregation did not rescue the signal; these panels describe regimes rather than time them
- `22:20:13` ⚠ still zero FDR survivors even on 36y — the honest read: these panels describe regimes, they do not time SPY. Keep them as context, not as timing signals.

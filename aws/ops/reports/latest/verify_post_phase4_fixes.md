# Verify post-Phase-4 fixes — IBIT, divergence, sizing, brief

**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-04-25T18:47:23+00:00  

## Log
## A. IBIT/GBTC/ETHA history in data/report.json

- `18:47:22`   data/report.json: 1,724,518B, age 1.5min, 188 stocks
- `18:47:22` ⚠     IBIT   not yet populated — daily-report-v3 may not have re-run
- `18:47:22` ⚠     GBTC   not yet populated — daily-report-v3 may not have re-run
- `18:47:22` ⚠     ETHA   not yet populated — daily-report-v3 may not have re-run
- `18:47:22` ⚠     FBTC   not yet populated — daily-report-v3 may not have re-run
- `18:47:22` ⚠     ARKB   not yet populated — daily-report-v3 may not have re-run
## B. Divergence scanner BTC/Nasdaq + Gold/BTC pairs

- `18:47:22` ⚠     BTC vs Nasdaq             status=missing_data (a_len=0 b_len=120)
- `18:47:22` ⚠     Gold vs BTC               status=missing_data (a_len=120 b_len=0)
- `18:47:22` 
  If both pairs say 'missing_data', either daily-report-v3
- `18:47:22`   hasn't re-run yet OR scanner needs to be re-invoked.
- `18:47:22`   daily-report-v3 schedule: cron(*/5 * * * ? *) — every 5 min
## C. Risk-sizer producing differentiated sizes (post-step-151)

- `18:47:23`   risk/recommendations.json: age 1.7min
- `18:47:23`   Size range: 3.39% — 5.15%, spread 1.76%
- `18:47:23`   Total: 75.01%
- `18:47:23` ✅   ✅ Sizing differentiated (step 151 patch active)
- `18:47:23`     FSLR   comp= 89.1 w=1.151 size= 5.15%
- `18:47:23`     INCY   comp= 93.3 w=1.205 size= 5.02%
- `18:47:23`     DECK   comp= 82.8 w= 1.07 size= 4.79%
- `18:47:23`     PTC    comp= 82.1 w=1.061 size= 4.75%
- `18:47:23`     FOXA   comp= 81.8 w=1.057 size= 4.72%
## D. Lambda configurations — confirm post-batch deploys

- `18:47:23`   justhodl-daily-report-v3         sha=iABc+H9z8r20BaQX... last_modified=2026-04-25T18:45:01
- `18:47:23`   justhodl-morning-intelligence    sha=rJ+PMLAVXeixpbnU... last_modified=2026-04-25T18:45:06
- `18:47:23`   justhodl-risk-sizer              sha=z0azDhiNVCzzzKEs... last_modified=2026-04-25T18:45:30
## E. Homepage Desk navigation

- `18:47:23`   Fetched 55,098B from justhodl.ai/index.html
- `18:47:23`     ✅ Desk in top nav
- `18:47:23`     ✅ Desk in secondary nav badge
- `18:47:23` ✅   ✅ Both Desk links present on production homepage
## F. Bond regime data freshness

- `18:47:23`   regime/current.json: age 166.8min
- `18:47:23`   Regime: NEUTRAL strength 57.9
- `18:47:23`   Indicators: 7, extreme 0
- `18:47:23` Done

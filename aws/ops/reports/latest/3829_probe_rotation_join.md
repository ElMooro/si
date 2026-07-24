# ops 3829 — PROBE: rotation -> setups/ranker join forecast

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-24T22:10:29+00:00  

## Data

| best-setups_forecast | master-ranker_forecast | sector_etfs_available |
|---|---|---|
| 24/50 (48.0%) | 0/25 (0.0%) | 11 |

## Log
## A. rotation-dashboard sector ETFs — what a row would receive

- `22:10:29`     XLV   LEADING    gate=PASS conf=  38.7 rank #1
- `22:10:29`     XLF   LEADING    gate=PASS conf=  25.4 rank #2
- `22:10:29`     XLK   LEADING    gate=PASS conf=   9.9 rank #7
- `22:10:29`     XLRE  WEAKENING  gate=PASS conf=   1.1 rank #11
- `22:10:29`     XLU   LAGGING    gate=PASS conf=  -1.6 rank #12
- `22:10:29`     XLE   WEAKENING  gate=PASS conf=  -4.4 rank #13
- `22:10:29`     XLB   LAGGING    gate=PASS conf=  -6.8 rank #15
- `22:10:29`     XLI   WEAKENING  gate=PASS conf=  -8.8 rank #16
- `22:10:29`     XLP   LAGGING    gate=PASS conf= -12.4 rank #17
- `22:10:29`     XLY   LAGGING    gate=FAIL conf= -40.7 rank #31
- `22:10:29`     XLC   LAGGING    gate=FAIL conf= -77.1 rank #36
- `22:10:29` ✅   11/11 sector ETFs present in rotation-dashboard
- `22:10:29`   regime=STAGFLATION generated_at=2026-07-24T20:44:56+00:00
## B. best-setups — live sector vocabulary

- `22:10:29` ✅   container 'top_setups' -> 50 rows
- `22:10:29`   row keys: ['why', 'ticker', 'name', 'conviction', 'khalid_panels', 'khalid_panel_multiplier', 'khalid_panel_audit', 'earnings_date', 'earnings_in_days', 'earnings_flag', 'squeeze_fuel', 'khalid_note', 'industry_flow_quadrant', 'industry_flow_z', 'risk_regime_mult', 'industry_mult', 'industry_etf', 'industry_score', 'industry_tag', 'factor_regime_mult', 'nowcast_regime_mult', 'cycle_phase']
- `22:10:29`     <none>                     n=26  -> UNMAPPED ✗
- `22:10:29`     Technology                 n=6   -> XLK    ✓
- `22:10:29`     Healthcare                 n=5   -> XLV    ✓
- `22:10:29`     Industrials                n=4   -> XLI    ✓
- `22:10:29`     Energy                     n=3   -> XLE    ✓
- `22:10:29`     Consumer Defensive         n=2   -> XLP    ✓
- `22:10:29`     Communication Services     n=2   -> XLC    ✓
- `22:10:29`     Consumer Cyclical          n=1   -> XLY    ✓
- `22:10:29`     Real Estate                n=1   -> XLRE   ✓
- `22:10:29` ⚠   FORECAST JOIN: 24/50 rows = 48.0%
- `22:10:29` ⚠   WOULD BE DROPPED: ['<none>(26)']
## B. master-ranker — live sector vocabulary

- `22:10:29` ✅   container 'top_tickers' -> 25 rows
- `22:10:29`   row keys: ['ticker', 'score', 'n_systems', 'systems', 'contributions', 'capital_flow_mult', 'risk_regime_mult', 'liquidity_regime_mult', 'nowcast_regime_mult', 'cycle_phase', 'cycle_warning', 'red_flags', 'rationale', 'details', 'census', 'capture_gap', 'global_capture_gap', 'capture_tier', 'mcap_share_pct', 'undervaluation_score', 'catchup_pct', 'catchup_basis']
- `22:10:29`     <none>                     n=25  -> UNMAPPED ✗
- `22:10:29` ⚠   FORECAST JOIN: 0/25 rows = 0.0%
- `22:10:29` ⚠   WOULD BE DROPPED: ['<none>(25)']
- `22:10:29` ✅ PROBE COMPLETE — no code written; forecast supports wiring

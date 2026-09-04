# ops 5203 -- KATLIN launch

**Status:** success  
**Duration:** 244.2s  
**Finished:** 2026-09-04T19:08:09+00:00  

## Error

```
SystemExit: 0
```

## Log
- `19:04:05`    env keys from justhodl-equity-research: ['FMP_KEY', 'POLYGON_API_KEY']
- `19:04:05`   Lambda exists — updating
- `19:04:08` ✅   ✓ updated justhodl-katlin
- `19:04:13`    function state Active / Successful, 8192MB / 900s
- `19:04:13`    invoked async (prior generated_at=None); polling data/katlin.json
- `19:07:34`    fresh artifact after 201s: v1.0.0 session 2026-09-03 elapsed 193.6s
## war room

- `19:07:34`    posture CASH_OR_TBILLS cap 10% thermometer 31.6 legs 14 red 0 vetoes ['black-swan barometer 14.0 (RED)']
- `19:07:34`    missing legs: ['Options tail risk', 'Global business cycle']
- `19:07:34`       Bond heartbeat                     5.0 GREEN CALM -- Bond markets are calm: UK 10Y (Gilt) -10.7bp (z -2.1); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) 
- `19:07:34`       Bond volatility -> stocks           40 GREEN CALM: Bond prices and yields are inside their normal daily range (TLT +0.3% (z +0.6); 10Y +0.2bp (z -0.0); MOV
- `19:07:34`       Eurodollar shortage                 20 GREEN NONE (0 pts)
- `19:07:34`       Treasury auction desk               20 GREEN $12.5B buyback at max · $185.5B bills -> risk-on supportive [LIQUIDITY EASY, EASY-POLICY SIGNAL, RISK-ASSET BU
- `19:07:34`       Risk gate (brain)                   40 GREEN NEUTRAL -- sizing x0.75
- `19:07:34`       Black-swan watch                   14.0 GREEN barometer 14.0, 2 red extremes -- RED
- `19:07:34`       Crisis composite                   27.0 GREEN NORMAL (DEFCON 4.0) -- no single component in stress territory
- `19:07:34`       Regime composite                    55 AMBER LATE_CYCLE / late-cycle -- Risk-on continues (risk=+1.00) BUT policy hawkish (pol=-1.00) + reflation elevated 
- `19:07:34`       Volatility regime                   25 GREEN CONCERNED (score 42.2)
- `19:07:34`       VIX term structure                  25 GREEN NORMAL_CONTANGO -- VIX 14.32
- `19:07:34`       Credit spreads (visible liquidity)  25 GREEN MELTUP_PRONE
- `19:07:34`       Global recession probability       44.0 GREEN 29% (BENIGN — expansion intact)
- `19:07:34`       Dollar (view first)                 45 AMBER NEUTRAL -- Dollar Pressure +14/100 -- NEUTRAL. 7 canaries lean pump, 3 lean dump. Risk-asset transmission: NEU
- `19:07:34`       Global liquidity impulse           58.0 AMBER NEUTRAL -- 13w impulse -1.3%
- `19:07:34`    brief: Risk thermometer 32/100 across 14 fleet legs (0 red, 3 amber). Posture CASH OR TBILLS: stand aside in cash / T-bills -- the bond and crisis desks say the floor can drop. Hard vetoes active: black-swan barometer 14.0 (RED). Loudest warnings: Global liquidity impulse (NEUTRAL -- 13w impulse -1.3%); Regime composite (LATE_CYCLE / late-cycle -- Risk-on continues (risk=+1.00) BUT policy hawkish (po); Dollar (view first) (NEUTRAL -- Dollar Pressure +14/100 -- NEUTRAL. 7 canaries lean pump, 3 lean dump). Supportive: Bond heartbeat (CALM -- Bond markets are calm: UK 10Y (Gilt) -10.7bp (z -2.1); Austra
## universe / tiers / gates

- `19:07:34`    universe {"stocks_in_universe": 3847, "etfs_in_universe": 3844, "crypto_symbols": 93, "scored": 5867, "stocks_scored": 3098, "etfs_scored": 2685, "crypto_scored": 84, "sessions": 1260, "first_session": "2021-08-27", "published": 1749}
- `19:07:34`    tiers {"KATLIN_PRIME": 0, "READY": 150, "BASING": 273, "WATCH": 1326, "SCREENED": 4118}
- `19:07:34`    gates {"location": 1996, "oversold": 1655, "accumulation": 1939, "inflows": 2045, "structure": 3633, "catalyst": 10, "not_knife": 5838, "quality": 5713}
- `19:07:34`    degraded []
## joins on the picks (PROBE-THEN-WIRE: a silent zero here is a bug, not a quiet market)

- `19:07:34`    inflow_legs       298 / 300 picks (99%)
- `19:07:34`    inflow_evidence   122 / 300 picks (41%)
- `19:07:34`    catalysts         102 / 300 picks (34%)
- `19:07:34`    fleet_accum        92 / 300 picks (31%)
- `19:07:34`    quality_pe         98 / 300 picks (33%)
- `19:07:34`    dilution           18 / 300 picks (6%)
- `19:07:34`    dark_pool          41 / 300 picks (14%)
- `19:07:34`    f13               163 / 300 picks (54%)
- `19:07:34`    industry_flow     169 / 300 picks (56%)
- `19:07:34`    sniper             69 / 300 picks (23%)
- `19:07:34`    why               300 / 300 picks (100%)
- `19:07:34`    plan              300 / 300 picks (100%)
- `19:07:34` ⚠    census dilution joined on <30% of stock picks (census matrix coverage) -- documented, not fatal
## top of the board

- `19:07:34`    USOY   READY        etf    score  73.9 conv  57.7 vs200   -1.7 rsiW 48.8 CONFIRMED acc 69.3 inf 100.0 cat  0.0 rr  1.8 4h WAIT
- `19:07:34`    RNA    READY        stock  score  73.7 conv  68.0 vs200  -56.4 rsiW 35.1 CONFIRMED acc 57.1 inf 60.1 cat 24.0 rr 20.0 4h WAIT
- `19:07:34`    FUBO   READY        stock  score  70.5 conv  68.9 vs200  -31.5 rsiW 44.1 CONFIRMED acc 58.1 inf 51.6 cat  8.0 rr 18.6 4h WAIT_BREAK
- `19:07:34`    PEPG   READY        stock  score  69.0 conv  47.5 vs200  -13.3 rsiW 55.4 CONFIRMED acc 76.4 inf 61.9 cat 16.0 rr  6.3 4h WAIT_BREAK
- `19:07:34`    DXC    READY        stock  score  68.5 conv  52.4 vs200   -1.9 rsiW 54.5 CONFIRMED acc 61.9 inf 54.9 cat 18.4 rr  1.6 4h WAIT
- `19:07:34`    OCS    READY        stock  score  68.4 conv  70.5 vs200  -42.3 rsiW 35.8 CONFIRMED acc 62.5 inf 70.1 cat 16.0 rr 11.5 4h WAIT_BREAK
- `19:07:34`    CELC   READY        stock  score  68.3 conv  44.0 vs200   -9.2 rsiW 48.5 CONFIRMED acc 69.7 inf 64.3 cat 24.0 rr  2.5 4h WAIT
- `19:07:34`    SNY    READY        stock  score  68.0 conv  54.9 vs200   -3.2 rsiW 48.0 CONFIRMED acc 70.7 inf 71.9 cat 15.9 rr  1.9 4h WAIT
- `19:07:34`    ARVN   READY        stock  score  67.9 conv  57.2 vs200  -11.7 rsiW 49.9 CONFIRMED acc 76.9 inf 62.1 cat 16.0 rr  2.4 4h SNIPE_PULLBACK
- `19:07:34`    KBR    READY        stock  score  67.7 conv  53.9 vs200   -2.6 rsiW 49.6 CONFIRMED acc 80.0 inf 56.7 cat 24.6 rr  1.7 4h SNIPE_PULLBACK
- `19:07:34`    IMRX   READY        stock  score  67.3 conv  45.8 vs200   -4.3 rsiW 50.6 CONFIRMED acc 73.4 inf 70.1 cat 24.0 rr  4.2 4h WAIT
- `19:07:34`    OLMA   READY        stock  score  67.3 conv  63.5 vs200  -39.5 rsiW 40.8 FORMING   acc 67.2 inf 65.1 cat 24.0 rr 11.7 4h WAIT
- `19:07:34`    WHY[USOY]: Defiance Oil Enhanced Options Income ETF (USOY) is a fund trading 2% below its 200-day average and 2% below the 250-day. It has been under that average for 29 sessions, so this is a long downtrend, not a dip. On the long-term chart the bottom looks CONFIRMED: weekly double bottom forming, weekly downtrend line broken, first higher low on the weekly chart. Momentum is washed out: monthly RSI 27. Accumulation fingerprints: volume has dried up: last 20 days run 51% below the 6-month average; on-balance volume is rising while price is flat/down -- shares are being absorbed on the way down; the accumulation/distribution line is rising against a flat tape (closes keep landing near the highs of the day). Money is flowing in: major inflows into the fund itself: 7.4% of AUM over 21 days (z None, $0.0M true 20d). Momentum is starting to arrive: relative strength vs the S&P has turned up over the l
## field coverage (artifact keys)

- `19:07:34`    top-level: ['as_of', 'base_rates', 'changes', 'definitions', 'degraded', 'elapsed_s', 'engine', 'feeds_asof', 'gates', 'generated_at', 'log', 'market', 'panels', 'params', 'picks', 'schema', 'session', 'tiers', 'top_picks', 'universe', 'validation', 'version', 'war_room', 'watch', 'weights']
- `19:07:34`    panels: {'war_room': 14, 'prime': 0, 'ready': 60, 'basing': 60, 'etfs': 40, 'crypto': 40}
- `19:07:34`    pick row keys (109): ['above_ema10_w', 'absorption_clv', 'accum_evidence', 'accum_legs', 'ad_slope_40', 'adv_usd', 'asset_class', 'asymmetry', 'atr_pctile', 'base_weeks', 'bbw_pctile', 'below_sma200', 'below_sma250', 'beta_1y', 'catalysts', 'composite', 'conviction', 'country', 'cvar5_pct', 'days_below_sma200', 'dd_52w_pct', 'dist_ema200_pct', 'dist_ema250_pct', 'dist_sma200_pct', 'dist_sma250_pct', 'double_bottom_d', 'double_bottom_w', 'ema200', 'ema250', 'evidence_coverage_pct', 'fleet_accumulation', 'gap_risk_pct', 'gates', 'gates_passed', 'high_52w', 'higher_lows_w', 'industry', 'industry_etf', 'inflow_evidence', 'inflow_legs', 'knife', 'knife_why', 'last', 'location_legs', 'low_52w', 'lower_half_days_20', 'lower_highs_w', 'lower_lows_w', 'lt_downtrend', 'lt_trend_break', 'm_close_vs_sma12_pct', 'm_higher_low', 'm_lower_lows', 'm_ret_pct', 'macd_w_turn', 'max_dd_1y_pct', 'mcap', 'mom_evidence', 'mom_legs', 'n_months', 'n_quarters', 'n_weeks', 'name', 'obv_slope_40', 'oversold_legs', 'pct_b', 'pillars', 'plan', 'pos_52w_pct', 'posture_note', 'q_break', 'q_lower_highs', 'q_ret_pct', 'quality', 'ret_12m_pct', 'ret_1m_pct', 'ret_3m_pct', 'ret_6m_pct', 'roc20_cross_days', 'rr', 'rs_63_pct', 'rs_slope_20', 'rsi_d', 'rsi_div_d', 'rsi_div_w', 'rsi_m', 'rsi_w', 'rsi_w_turning_up', 'sector', 'sessions', 'sma200', 'sma200_slope_20_pct', 'sma250', 'sma40w_falling', 'sniper', 'squeeze', 'squeeze_lean', 'structure_legs', 'structure_state', 'sub_class', 'ticker', 'tier', 'trendline_level', 'updown_vol_20', 'vol_ann_pct', 'vol_ratio_20_120', 'weeks_since_break', 'why', 'worst_day_1y_pct']
## schedules (EventBridge Scheduler, UTC)

- `19:07:34` ✅    justhodl-katlin-daily updated cron(10 4 ? * TUE-SAT *)
- `19:07:34` ✅    justhodl-katlin-backtest-weekly updated cron(30 9 ? * SUN *)
## page

- `19:07:35`    katlin.html carries marker KATLIN_DESK_V1 at the edge: True
- `19:07:57`    1440px: {"score": "32", "posture": "CASH OR TBILLS", "headline": "stand aside in cash / T-bills -- the bond and crisis desks say the floor can dro", "legs": 14, "board": 150, "evid": 1238, "helps": 25, "defs": 16, "overflow": 0, "err": ""} errors=[]
- `19:08:08`     390px: {"score": "32", "posture": "CASH OR TBILLS", "headline": "stand aside in cash / T-bills -- the bond and crisis desks say the floor can dro", "legs": 14, "board": 150, "evid": 1238, "helps": 25, "defs": 16, "overflow": 0, "err": ""} errors=[]
- `19:08:09` ✅    first walk-forward backtest kicked async (data/katlin-backtest.json)
- `19:08:09` ✅    GREEN: KATLIN live -- engine, feed, schedules, page

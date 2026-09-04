# ops 5207 -- KATLIN v1.2.0 audit (no redeploy)

**Status:** success  
**Duration:** 48.8s  
**Finished:** 2026-09-04T19:34:23+00:00  

## Error

```
SystemExit: 0
```

## Log
- `19:33:34`    fn state Active mem 8192 timeout 900
- `19:33:35`    artifact v1.2.0 generated 2026-09-04T19:27:02Z session 2026-09-03 elapsed 182.8s
## war room

- `19:33:35`    posture FULL_RISK cap 75% thermometer 34.3 legs 16 red 0 vetoes []
- `19:33:35`    missing legs: []
- `19:33:35`       Bond heartbeat                     5.0 GREEN CALM -- Bond markets are calm: UK 10Y (Gilt) -10.7bp (z -2.1); Australia 3Y +10.1bp (z +2.0); Japan 30Y (JGB) 
- `19:33:35`       Bond volatility -> stocks           40 GREEN CALM: Bond prices and yields are inside their normal daily range (TLT +0.3% (z +0.6); 10Y +0.2bp (z -0.0); MOV
- `19:33:35`       Eurodollar shortage                 20 GREEN NONE (0 pts)
- `19:33:35`       Treasury auction desk               20 GREEN $12.5B buyback at max · $185.5B bills -> risk-on supportive [LIQUIDITY EASY, EASY-POLICY SIGNAL, RISK-ASSET BU
- `19:33:35`       Risk gate (brain)                   40 GREEN NEUTRAL -- sizing x0.75
- `19:33:35`       Black-swan watch                    55 AMBER barometer 14.0, 2 red extremes today -- strip RED
- `19:33:35`       Crisis composite                   27.0 GREEN NORMAL (DEFCON 4.0) -- no single component in stress territory
- `19:33:35`       Options tail risk (P[-10%])         20 GREEN 2% option-implied chance of a 10% drop; system tail gauge 44/100 (WATCH)
- `19:33:35`       Regime composite                    55 AMBER LATE_CYCLE / late-cycle -- Risk-on continues (risk=+1.00) BUT policy hawkish (pol=-1.00) + reflation elevated 
- `19:33:35`       Volatility regime                   25 GREEN CONCERNED (score 42.2)
- `19:33:35`       VIX term structure                  25 GREEN NORMAL_CONTANGO -- VIX 14.32
- `19:33:35`       Credit spreads (visible liquidity)  25 GREEN MELTUP_PRONE
- `19:33:35`       Global recession probability       44.0 GREEN 29% (BENIGN — expansion intact)
- `19:33:35`       Global business cycle              35.0 GREEN phase GLOBAL_EXPANSION -- CLI 100.0 -- 6m downturn prob 35%
- `19:33:35`       Dollar (view first)                 45 AMBER NEUTRAL -- Dollar Pressure +14/100 -- NEUTRAL. 7 canaries lean pump, 3 lean dump. Risk-asset transmission: NEU
- `19:33:35`       Global liquidity impulse           58.0 AMBER NEUTRAL -- 13w impulse -1.3%
- `19:33:35`    brief: Risk thermometer 34/100 across 16 fleet legs (0 red, 4 amber). Posture FULL RISK: green light -- deploy into the best asymmetric setups. Loudest warnings: Global liquidity impulse (NEUTRAL -- 13w impulse -1.3%); Black-swan watch (barometer 14.0, 2 red extremes today -- strip RED); Regime composite (LATE_CYCLE / late-cycle -- Risk-on continues (risk=+1.00) BUT policy hawkish (po). Supportive: Bond heartbeat (CALM -- Bond markets are calm: UK 10Y (Gilt) -10.7bp (z -2.1); Austral); Eurodollar shortage (NONE (0 pts)). Cycle: global phase GLOBAL_EXPANSION, CLI 100.0. Crypto dump-risk 36/100 from th
## universe / tiers / gates

- `19:33:35`    universe {"stocks_in_universe": 3847, "etfs_in_universe": 3647, "crypto_symbols": 93, "scored": 5759, "stocks_scored": 3098, "etfs_scored": 2577, "crypto_scored": 84, "sessions": 1260, "first_session": "2021-08-27", "published": 1546}
- `19:33:35`    tiers {"KATLIN_PRIME": 3, "READY": 84, "BASING": 136, "WATCH": 1323, "SCREENED": 4213}
- `19:33:35`    gates {"location": 1945, "oversold": 1609, "accumulation": 1889, "inflows": 1979, "structure": 3580, "catalyst": 336, "not_knife": 5730, "quality": 5568}
- `19:33:35`    degraded []
## joins on the picks (PROBE-THEN-WIRE: a silent zero here is a bug, not a quiet market)

- `19:33:35`    inflow_legs       220 / 223 picks (99%)
- `19:33:35`    inflow_evidence    52 / 223 picks (23%)
- `19:33:35`    catalysts         101 / 223 picks (45%)
- `19:33:35`    fleet_accum        85 / 223 picks (38%)
- `19:33:35`    quality_pe        112 / 223 picks (50%)
- `19:33:35`    dilution          167 / 223 picks (75%)
- `19:33:35`    dark_pool          53 / 223 picks (24%)
- `19:33:35`    f13               160 / 223 picks (72%)
- `19:33:35`    industry_flow     167 / 223 picks (75%)
- `19:33:35`    sniper             70 / 223 picks (31%)
- `19:33:35`    why               223 / 223 picks (100%)
- `19:33:35`    plan              223 / 223 picks (100%)
## top of the board

- `19:33:35`    RNA    KATLIN_PRIME stock  score  75.9 conv  68.0 vs200  -56.4 rsiW 35.1 CONFIRMED acc 57.1 inf 60.1 cat 38.4 rr 10.0 4h WAIT
- `19:33:35`    SNY    KATLIN_PRIME stock  score  67.9 conv  55.7 vs200   -3.2 rsiW 48.0 CONFIRMED acc 70.7 inf 71.9 cat 37.1 rr  3.3 4h WAIT
- `19:33:35`    KBR    KATLIN_PRIME stock  score  66.7 conv  54.9 vs200   -2.6 rsiW 49.6 CONFIRMED acc 80.0 inf 56.7 cat 39.4 rr  2.0 4h SNIPE_PULLBACK
- `19:33:35`    UPWK   READY        stock  score  69.4 conv  65.9 vs200  -28.7 rsiW 42.1 CONFIRMED acc 59.7 inf 44.7 cat 38.4 rr 10.0 4h SNIPE_PULLBACK
- `19:33:35`    ARVN   READY        stock  score  67.9 conv  58.4 vs200  -11.7 rsiW 49.9 CONFIRMED acc 76.9 inf 62.1 cat 25.6 rr  3.1 4h SNIPE_PULLBACK
- `19:33:35`    ADMA   READY        stock  score  67.8 conv  65.5 vs200  -24.4 rsiW 44.2 CONFIRMED acc 55.9 inf 62.8 cat 25.6 rr 10.0 4h SNIPE_PULLBACK
- `19:33:35`    DXC    READY        stock  score  67.6 conv  53.0 vs200   -1.9 rsiW 54.5 CONFIRMED acc 61.9 inf 54.9 cat 29.4 rr  2.4 4h WAIT
- `19:33:35`    TIGR   READY        stock  score  66.1 conv  65.4 vs200  -25.2 rsiW 42.3 CONFIRMED acc 56.0 inf 52.9 cat  0.0 rr 10.0 4h WAIT_BREAK
- `19:33:35`    KROS   READY        stock  score  65.8 conv  50.1 vs200  -18.8 rsiW 45.9 CONFIRMED acc 61.6 inf 64.6 cat 25.6 rr 10.0 4h WAIT
- `19:33:35`    CRK    READY        stock  score  65.5 conv  59.7 vs200  -17.5 rsiW 47.1 CONFIRMED acc 59.4 inf 62.7 cat 12.8 rr  6.4 4h SNIPE_PULLBACK
- `19:33:35`    TRDA   READY        stock  score  64.7 conv  44.8 vs200  -25.9 rsiW 44.4 CONFIRMED acc 55.1 inf 66.3 cat 25.6 rr  5.5 4h SNIPE_PULLBACK
- `19:33:35`    TSCO   READY        stock  score  63.6 conv  61.6 vs200  -17.6 rsiW 43.4 CONFIRMED acc 72.6 inf 40.9 cat 25.6 rr  4.3 4h WAIT
- `19:33:35`    WHY[RNA]: Atrium Therapeutics Inc (RNA) is a stock trading 56% below its 200-day average and 61% below the 250-day. It has been under that average for 131 sessions, so this is a long downtrend, not a dip. On the long-term chart the bottom looks CONFIRMED: weekly double bottom confirmed, weekly downtrend line broken, weekly RSI bullish divergence. Momentum is washed out: weekly RSI 35 (oversold), monthly RSI 40, weekly RSI turning up from 22. Accumulation fingerprints: volume has dried up: last 20 days run 49% below the 6-month average; up days carry 2.2x the volume of down days over the last month; the 20-day low is holding above the prior 2-month low on shrinking volume (Wyckoff test of support). Money is flowing in: its industry ETF XBI is seeing major inflows (5.3% of AUM over 21d, z 0.2). Catalysts the engine can name: its industry is booming: Biotechnology revenue growth 42% y/y with 100% of 
## v1.2.0 lanes

- `19:33:35`    washout gate passes: None of 5759 scored
- `19:33:35`    ports catalysts on picks: 15; T1 labels: ['100-day average', '200-day average', '50-day average', '52-week high', 'neckline']
- `19:33:35`    [katlin] feeds in 3.8s: finviz=11647 census=492 boom=124 rotation=40 flows=300/128 f13=7526 dark=936 insider=13 congress=32 options=58 blocks=18 catalyst=196 calendar=87 contracts=79 backlog=121 floor=168 ports=16 fleet_accum=817 warroom=bond_warroom,auction,risk_gate,blackswan,crisis,tail,regime,vo
- `19:33:35`    [katlin] war room: FULL_RISK (therm 34.3, 16 legs, vetoes [], missing [])
- `19:33:35`    [katlin] stocks scored: 3098 (thin 393, hygiene 356) in 53s
- `19:33:35`    [katlin] crypto lane: 93 symbols banked, 89 with >=200 days, 0 errors (BTC=ok:+0, ETH=ok:+0, SOL=ok:+0, XRP=ok:+0, BNB=ok:+0)
- `19:33:35`    [katlin] dilution lane: 181 names, {'banked': 181}, 1s
- `19:33:35`    [katlin] sniper: 70/70 shortlist names got 4h bars (0 errors)
- `19:33:35`    picks with a NAMED catalyst: 66 / 223; catalyst gate passes: 336
- `19:33:35`    catalyst kinds across picks: [('industry_boom', 62), ('squeeze', 37), ('ports', 15), ('scheduled', 7), ('contracts', 5), ('backlog', 3), ('earnings', 2)]
- `19:33:35`    dilution coverage on stock picks: 167 / 168 (fmp-sourced 143)
- `19:33:35`    RNA catalysts: ['its industry is booming: Biotechnology revenue growth 42% y/y with 100', '20% of the float is short with 19.2 days to cover -- fuel for a violen']
- `19:33:35`    RNA plan: {'entry': 14.6, 'stop': 11.67, 'target_1': 33.46, 'target_1_label': '200-day average', 'target_2': 72.93, 'target_2_label': '52-week high', 'upside_1_pct': 129.2, 'upside_2_pct': 399.5, 'downside_pct': 20.1, 'rr_1': 6.4, 'rr_2': 10.0, 'asymmetry': 13.2, 'confirmation_trigger': None, 'rr_best': 10.0} | 3m 20.0 | days<200 131 | knife False
- `19:33:35`    SNY catalysts: ['its industry is booming: Drug Manufacturers - General revenue growth 4', 'port traffic tied to Drug Manufacturers - General is up 10% y/y (Shang']
- `19:33:35`    SNY plan: {'entry': 44.33, 'stop': 41.94, 'target_1': 52.34, 'target_1_label': '52-week high', 'target_2': None, 'target_2_label': None, 'upside_1_pct': 18.1, 'upside_2_pct': None, 'downside_pct': 5.4, 'rr_1': 3.3, 'rr_2': None, 'asymmetry': 3.3, 'confirmation_trigger': 45.66, 'rr_best': 3.3} | 3m 4.6 | days<200 141 | knife False
- `19:33:35`    KBR catalysts: ['2 contract/licensing win(s) in 90 days worth $268M = 5.8% of market ca', 'industry boom score 79/100 (Engineering & Construction)']
- `19:33:35`    KBR plan: {'entry': 37.29, 'stop': 31.17, 'target_1': 49.81, 'target_1_label': '52-week high', 'target_2': None, 'target_2_label': None, 'upside_1_pct': 33.6, 'upside_2_pct': None, 'downside_pct': 16.4, 'rr_1': 2.0, 'rr_2': None, 'asymmetry': 2.0, 'confirmation_trigger': 38.04, 'rr_best': 2.0} | 3m 4.4 | days<200 9 | knife False
- `19:33:35`    UPWK catalysts: ['its industry is booming: Internet Content & Information revenue growth', '16% of the float is short with 5.6 days to cover -- fuel for a violent']
- `19:33:35`    UPWK plan: {'entry': 9.19, 'stop': 8.17, 'target_1': 9.91, 'target_1_label': 'neckline', 'target_2': 22.11, 'target_2_label': '52-week high', 'upside_1_pct': 7.8, 'upside_2_pct': 140.6, 'downside_pct': 11.1, 'rr_1': 0.7, 'rr_2': 10.0, 'asymmetry': 6.7, 'confirmation_trigger': 9.91, 'rr_best': 10.0} | 3m 7.4 | days<200 143 | knife False
- `19:33:35`    ARVN catalysts: ['its industry is booming: Biotechnology revenue growth 42% y/y with 100']
- `19:33:35`    ARVN plan: {'entry': 9.31, 'stop': 7.66, 'target_1': 10.55, 'target_1_label': '200-day average', 'target_2': 14.51, 'target_2_label': 'neckline', 'upside_1_pct': 13.3, 'upside_2_pct': 55.9, 'downside_pct': 17.7, 'rr_1': 0.8, 'rr_2': 3.1, 'asymmetry': 1.9, 'confirmation_trigger': 14.51, 'rr_best': 3.1} | 3m 20.9 | days<200 84 | knife False
- `19:33:35`    ADMA catalysts: ['its industry is booming: Biotechnology revenue growth 42% y/y with 100']
- `19:33:35`    ADMA plan: {'entry': 9.58, 'stop': 8.96, 'target_1': 12.67, 'target_1_label': '200-day average', 'target_2': 20.38, 'target_2_label': '52-week high', 'upside_1_pct': 32.3, 'upside_2_pct': 112.7, 'downside_pct': 6.4, 'rr_1': 5.0, 'rr_2': 10.0, 'asymmetry': 9.3, 'confirmation_trigger': 14.84, 'rr_best': 10.0} | 3m 23.5 | days<200 160 | knife False
- `19:33:35`    ETF names on the board flagged by a naive substring check (for the record): [('GSKH', 'GSK plc ADRhedged')]
## field coverage (artifact keys)

- `19:33:35`    top-level: ['as_of', 'base_rates', 'changes', 'definitions', 'degraded', 'elapsed_s', 'engine', 'feeds_asof', 'gates', 'generated_at', 'log', 'market', 'panels', 'params', 'picks', 'schema', 'session', 'tiers', 'top_picks', 'universe', 'validation', 'version', 'war_room', 'watch', 'weights']
- `19:33:35`    panels: {'war_room': 16, 'prime': 3, 'ready': 60, 'basing': 60, 'etfs': 40, 'crypto': 40}
- `19:33:35`    pick row keys (112): ['above_ema10_w', 'absorption_clv', 'accum_evidence', 'accum_legs', 'ad_slope_40', 'adv_usd', 'asset_class', 'asymmetry', 'atr_pctile', 'base_weeks', 'bbw_pctile', 'below_sma200', 'below_sma250', 'beta_1y', 'catalysts', 'composite', 'conviction', 'country', 'cvar5_pct', 'days_below_sma200', 'dd_52w_pct', 'dist_ema200_pct', 'dist_ema250_pct', 'dist_sma200_pct', 'dist_sma250_pct', 'double_bottom_d', 'double_bottom_w', 'ema200', 'ema250', 'evidence_coverage_pct', 'fleet_accumulation', 'gap_risk_pct', 'gates', 'gates_passed', 'high_52w', 'higher_lows_w', 'industry', 'industry_etf', 'inflow_evidence', 'inflow_legs', 'knife', 'knife_why', 'last', 'location_legs', 'low_52w', 'lower_half_days_20', 'lower_highs_w', 'lower_lows_w', 'lt_downtrend', 'lt_trend_break', 'm_close_vs_sma12_pct', 'm_higher_low', 'm_lower_lows', 'm_ret_pct', 'macd_w_turn', 'max_dd_1y_pct', 'mcap', 'mom_evidence', 'mom_legs', 'n_months', 'n_named_catalysts', 'n_quarters', 'n_weeks', 'name', 'obv_slope_40', 'oversold_legs', 'pct_b', 'pillars', 'plan', 'pos_52w_pct', 'posture_note', 'q_break', 'q_lower_highs', 'q_ret_pct', 'quality', 'ret_12m_pct', 'ret_1m_pct', 'ret_3m_pct', 'ret_6m_pct', 'roc20_cross_days', 'rr', 'rs_63_pct', 'rs_slope_20', 'rsi_d', 'rsi_div_d', 'rsi_div_w', 'rsi_m', 'rsi_w', 'rsi_w_turning_up', 'sector', 'sessions', 'sma100', 'sma200', 'sma200_slope_20_pct', 'sma250', 'sma40w_falling', 'sma50', 'sniper', 'squeeze', 'squeeze_lean', 'structure_legs', 'structure_state', 'sub_class', 'ticker', 'tier', 'trendline_level', 'updown_vol_20', 'vol_ann_pct', 'vol_ratio_20_120', 'weeks_since_break', 'why', 'worst_day_1y_pct']
## schedules

- `19:33:36`    justhodl-katlin-daily cron(10 4 ? * TUE-SAT *) ENABLED
- `19:33:36`    justhodl-katlin-backtest-weekly cron(30 9 ? * SUN *) ENABLED
## page

- `19:33:36`    katlin.html carries marker KATLIN_DESK_V1 at the edge: True
- `19:34:10`    1440px: {"score": "34", "posture": "FULL RISK", "headline": "green light -- deploy into the best asymmetric setups", "legs": 16, "board": 3, "evid": 1303, "helps": 25, "defs": 17, "overflow": 0, "err": ""} errors=[]
- `19:34:22`     390px: {"score": "34", "posture": "FULL RISK", "headline": "green light -- deploy into the best asymmetric setups", "legs": 16, "board": 3, "evid": 1305, "helps": 25, "defs": 17, "overflow": 0, "err": ""} errors=[]
- `19:34:23` ✅    GREEN: KATLIN live -- engine, feed, schedules, page

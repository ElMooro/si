# Diagnose why 4377 outcomes have correct=None

**Status:** success  
**Duration:** 3.3s  
**Finished:** 2026-04-25T19:08:31+00:00  

## Data

| n_outcomes_sampled | n_signals_total | types_with_baseline | types_without_baseline |
|---|---|---|---|
| 500 | 4854 | 1 | 14 |

## Log
## A. Sample 5 outcomes — what fields do they have?

- `19:08:28`   Sampled 500 outcomes
- `19:08:28` 
  Outcome 1:
- `19:08:28`     checked_at           = 2026-04-24T23:17:41.907006+00:00
- `19:08:28`     correct              = None
- `19:08:28`     logged_at            = 2026-03-12T03:10:14.100624+00:00
- `19:08:28`     outcome              = {'price_at_check': 713.94, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'ch
- `19:08:28`     outcome_id           = fff190b1-93d8-4e81-92e6-718b3f22f824_day_30
- `19:08:28`     predicted_dir        = UP
- `19:08:28`     signal_id            = fff190b1-93d8-4e81-92e6-718b3f22f824
- `19:08:28`     signal_type          = carry_risk
- `19:08:28`     signal_value         = 0.0
- `19:08:28`     ttl                  = 1808608661.0
- `19:08:28`     window_key           = day_30
- `19:08:28` 
  Outcome 2:
- `19:08:28`     checked_at           = 2026-04-24T23:17:41.907006+00:00
- `19:08:28`     correct              = None
- `19:08:28`     logged_at            = 2026-03-12T15:10:14.111761+00:00
- `19:08:28`     outcome              = {'benchmark_price': 713.94, 'asset_price': 520.8, 'checked_at': '2026-04-24T23:1
- `19:08:28`     outcome_id           = ffe70e0e-7809-49d4-90cf-1a478ce1a2fd_day_30
- `19:08:28`     predicted_dir        = OUTPERFORM
- `19:08:28`     signal_id            = ffe70e0e-7809-49d4-90cf-1a478ce1a2fd
- `19:08:28`     signal_type          = screener_top_pick
- `19:08:28`     signal_value         = TOP_10
- `19:08:28`     ttl                  = 1808608661.0
- `19:08:28`     window_key           = day_30
- `19:08:28` 
  Outcome 3:
- `19:08:28`     checked_at           = 2026-04-05T08:00:12.057710+00:00
- `19:08:28`     correct              = None
- `19:08:28`     logged_at            = 2026-03-26T15:10:13.761356+00:00
- `19:08:28`     outcome              = {'price_at_check': 66805.4, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'c
- `19:08:28`     outcome_id           = ffb3cd05-0a17-4969-9e8f-0c3d664d1e24_day_7
- `19:08:28`     predicted_dir        = UP
- `19:08:28`     signal_id            = ffb3cd05-0a17-4969-9e8f-0c3d664d1e24
- `19:08:28`     signal_type          = crypto_fear_greed
- `19:08:28`     signal_value         = EXTREME_FEAR
- `19:08:28`     ttl                  = 1806912012.0
- `19:08:28`     window_key           = day_7
- `19:08:28` 
  Outcome 4:
- `19:08:28`     checked_at           = 2026-04-24T23:17:41.907006+00:00
- `19:08:28`     correct              = None
- `19:08:28`     logged_at            = 2026-03-23T15:10:14.389025+00:00
- `19:08:28`     outcome              = {'benchmark_price': 713.94, 'asset_price': 520.8, 'checked_at': '2026-04-24T23:1
- `19:08:28`     outcome_id           = ff937359-74ee-4748-b5e3-afce44db85b3_day_30
- `19:08:28`     predicted_dir        = OUTPERFORM
- `19:08:28`     signal_id            = ff937359-74ee-4748-b5e3-afce44db85b3
- `19:08:28`     signal_type          = screener_top_pick
- `19:08:28`     signal_value         = TOP_10
- `19:08:28`     ttl                  = 1808608661.0
- `19:08:28`     window_key           = day_30
- `19:08:28` 
  Outcome 5:
- `19:08:28`     checked_at           = 2026-04-24T23:17:41.907006+00:00
- `19:08:28`     correct              = None
- `19:08:28`     logged_at            = 2026-03-22T09:10:13.744536+00:00
- `19:08:28`     outcome              = {'price_at_check': 713.94, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'ch
- `19:08:28`     outcome_id           = fefe577b-53fc-482d-b239-32cec443de72_day_7
- `19:08:28`     predicted_dir        = NEUTRAL
- `19:08:28`     signal_id            = fefe577b-53fc-482d-b239-32cec443de72
- `19:08:28`     signal_type          = edge_composite
- `19:08:28`     signal_value         = 43.0
- `19:08:28`     ttl                  = 1808608661.0
- `19:08:28`     window_key           = day_7
## B. Look at signals_table for the SIGNAL_IDs we just sampled

- `19:08:28` 
  Signal fff190b1-93d8-4e81-92e6-718b3f:
- `19:08:28`     signal_type               = carry_risk
- `19:08:28`     predicted_direction       = UP
- `19:08:28`     baseline_price            = None
- `19:08:28`     measure_against           = SPY
- `19:08:28`     status                    = complete
- `19:08:28`     check_timestamps          ['day_14', 'day_30']
- `19:08:28`     outcomes                  2 entries
- `19:08:28` 
  Signal ffe70e0e-7809-49d4-90cf-1a478c:
- `19:08:28`     signal_type               = screener_top_pick
- `19:08:28`     predicted_direction       = OUTPERFORM
- `19:08:28`     baseline_price            = 339.95
- `19:08:28`     measure_against           = CIEN
- `19:08:28`     status                    = partial
- `19:08:28`     check_timestamps          ['day_30', 'day_60', 'day_90']
- `19:08:28`     outcomes                  1 entries
- `19:08:29` 
  Signal ffb3cd05-0a17-4969-9e8f-0c3d66:
- `19:08:29`     signal_type               = crypto_fear_greed
- `19:08:29`     predicted_direction       = UP
- `19:08:29`     baseline_price            = None
- `19:08:29`     measure_against           = BTC-USD
- `19:08:29`     status                    = complete
- `19:08:29`     check_timestamps          ['day_14', 'day_3', 'day_7']
- `19:08:29`     outcomes                  3 entries
- `19:08:29` 
  Signal ff937359-74ee-4748-b5e3-afce44:
- `19:08:29`     signal_type               = screener_top_pick
- `19:08:29`     predicted_direction       = OUTPERFORM
- `19:08:29`     baseline_price            = 383.89
- `19:08:29`     measure_against           = CIEN
- `19:08:29`     status                    = partial
- `19:08:29`     check_timestamps          ['day_30', 'day_60', 'day_90']
- `19:08:29`     outcomes                  1 entries
- `19:08:29` 
  Signal fefe577b-53fc-482d-b239-32cec4:
- `19:08:29`     signal_type               = edge_composite
- `19:08:29`     predicted_direction       = NEUTRAL
- `19:08:29`     baseline_price            = None
- `19:08:29`     measure_against           = SPY
- `19:08:29`     status                    = complete
- `19:08:29`     check_timestamps          ['day_14', 'day_7']
- `19:08:29`     outcomes                  2 entries
## C. Count signals by status

- `19:08:31`   Total signals: 4854
- `19:08:31`     status=pending         2035
- `19:08:31`     status=partial         1612
- `19:08:31`     status=complete        889
- `19:08:31`     status=unscoreable     318
- `19:08:31` 
  Pending signals: 2035
- `19:08:31`     with baseline_price>0: 2035
- `19:08:31`     without (or zero):     0
## D. Latest signal of each type — does it have baseline_price?

- `19:08:31`     ❌ carry_risk                     ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ crypto_fear_greed              ticker=BTC-USD  baseline=$0 captured=?
- `19:08:31`     ❌ crypto_risk_score              ticker=BTC-USD  baseline=$0 captured=?
- `19:08:31`     ❌ edge_composite                 ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ edge_regime                    ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ khalid_index                   ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ market_phase                   ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ ml_risk                        ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ momentum_gld                   ticker=GLD      baseline=$0 captured=?
- `19:08:31`     ❌ momentum_spy                   ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ❌ momentum_tlt                   ticker=TLT      baseline=$0 captured=?
- `19:08:31`     ❌ momentum_uso                   ticker=USO      baseline=$0 captured=?
- `19:08:31`     ❌ momentum_uup                   ticker=UUP      baseline=$0 captured=?
- `19:08:31`     ❌ plumbing_stress                ticker=SPY      baseline=$0 captured=?
- `19:08:31`     ✅ screener_top_pick              ticker=SATS     baseline=$128.59 captured=?
- `19:08:31` ⚠ 
  ⚠ 14 signal types still missing baseline_price
- `19:08:31` ⚠   These will continue producing correct=None outcomes until fixed.
## E. When were these correct=None outcomes created?

- `19:08:31`   Outcomes with checked_at timestamps: 500
- `19:08:31`     Oldest: 2026-03-22T08:00:12.016437+00:00
- `19:08:31`     Newest: 2026-04-25T09:41:57.840179+00:00
- `19:08:31` 
  correct=None by signal_type:
- `19:08:31`     screener_top_pick              100
- `19:08:31`     crypto_fear_greed              56
- `19:08:31`     plumbing_stress                43
- `19:08:31`     crypto_risk_score              42
- `19:08:31`     ml_risk                        38
- `19:08:31`     khalid_index                   36
- `19:08:31`     edge_composite                 34
- `19:08:31`     momentum_uso                   32
- `19:08:31`     edge_regime                    27
- `19:08:31`     momentum_gld                   22
- `19:08:31`     market_phase                   21
- `19:08:31`     carry_risk                     20
- `19:08:31`     momentum_spy                   16
- `19:08:31`     momentum_tlt                   11
- `19:08:31`     momentum_uup                   2
- `19:08:31` Done

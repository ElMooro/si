# 1) Count outcomes by correct-value for screener_top_pick (last 60d)

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-05-04T21:11:42+00:00  

## Log
- `21:11:42`   total screener_top_pick outcomes 60d: 450 (6 pages)
- `21:11:42` 
- `21:11:42`   Distribution of 'correct' field:
- `21:11:42`     correct=None        n=450
- `21:11:42` 
- `21:11:42`   Sample 3 items:
- `21:11:42`   [0] keys: ['checked_at', 'correct', 'logged_at', 'outcome', 'outcome_id', 'predicted_dir', 'signal_id', 'signal_type', 'signal_value', 'ttl', 'window_key']
- `21:11:42`       signal_type               = screener_top_pick
- `21:11:42`       predicted_dir             = OUTPERFORM
- `21:11:42`       correct                   = None
- `21:11:42`       outcome                   = {'benchmark_price': Decimal('711.69'), 'asset_price': Decimal('473.69'), 'checked_at': '2026-04-28T22:30:03.016350+00:00
- `21:11:42`       logged_at                 = 2026-03-29T09:10:14.124773+00:00
- `21:11:42`       checked_at                = 2026-04-28T22:30:03.016350+00:00
- `21:11:42`       window_key                = day_30
- `21:11:42`       measure_against           = None
- `21:11:42`       baseline_price            = None
- `21:11:42`       is_legacy                 = None
- `21:11:42`   [1] keys: ['checked_at', 'correct', 'logged_at', 'outcome', 'outcome_id', 'predicted_dir', 'signal_id', 'signal_type', 'signal_value', 'ttl', 'window_key']
- `21:11:42`       signal_type               = screener_top_pick
- `21:11:42`       predicted_dir             = OUTPERFORM
- `21:11:42`       correct                   = None
- `21:11:42`       outcome                   = {'benchmark_price': Decimal('713.94'), 'asset_price': Decimal('118'), 'checked_at': '2026-04-26T08:00:11.868203+00:00', 
- `21:11:42`       logged_at                 = 2026-03-26T21:10:14.261752+00:00
- `21:11:42`       checked_at                = 2026-04-26T08:00:11.868203+00:00
- `21:11:42`       window_key                = day_30
- `21:11:42`       measure_against           = None
- `21:11:42`       baseline_price            = None
- `21:11:42`       is_legacy                 = None
- `21:11:42`   [2] keys: ['checked_at', 'correct', 'logged_at', 'outcome', 'outcome_id', 'predicted_dir', 'signal_id', 'signal_type', 'signal_value', 'ttl', 'window_key']
- `21:11:42`       signal_type               = screener_top_pick
- `21:11:42`       predicted_dir             = OUTPERFORM
- `21:11:42`       correct                   = None
- `21:11:42`       outcome                   = {'benchmark_price': Decimal('715.17'), 'asset_price': Decimal('859.68'), 'checked_at': '2026-04-27T22:30:03.042818+00:00
- `21:11:42`       logged_at                 = 2026-03-28T03:10:14.480976+00:00
- `21:11:42`       checked_at                = 2026-04-27T22:30:03.042818+00:00
- `21:11:42`       window_key                = day_30
- `21:11:42`       measure_against           = None
- `21:11:42`       baseline_price            = None
- `21:11:42`       is_legacy                 = None
# 2) Total screener_top_pick (all-time, non-legacy)

- `21:11:42`   total all-time non-legacy: 450 (6 pages)
- `21:11:42`     correct=None        n=450
# 3) For correct=None records, inspect outcome dict

- `21:11:42`   Sample 5 correct=None records:
- `21:11:42`   [0] outcome keys: ['benchmark_price', 'asset_price', 'checked_at', 'correct', 'excess_return']
- `21:11:42`       predicted_dir: OUTPERFORM
- `21:11:42`       window_key: day_30
- `21:11:42`       logged_at: 2026-03-29T09:10:14.124773+00:00
- `21:11:42`       checked_at: 2026-04-28T22:30:03.016350+00:00
- `21:11:42`         outcome.benchmark_price: 711.69
- `21:11:42`         outcome.asset_price: 473.69
- `21:11:42`         outcome.checked_at: 2026-04-28T22:30:03.016350+00:00
- `21:11:42`         outcome.correct: None
- `21:11:42`         outcome.excess_return: 0
- `21:11:42`   [1] outcome keys: ['benchmark_price', 'asset_price', 'checked_at', 'correct', 'excess_return']
- `21:11:42`       predicted_dir: OUTPERFORM
- `21:11:42`       window_key: day_30
- `21:11:42`       logged_at: 2026-03-26T21:10:14.261752+00:00
- `21:11:42`       checked_at: 2026-04-26T08:00:11.868203+00:00
- `21:11:42`         outcome.benchmark_price: 713.94
- `21:11:42`         outcome.asset_price: 118
- `21:11:42`         outcome.checked_at: 2026-04-26T08:00:11.868203+00:00
- `21:11:42`         outcome.correct: None
- `21:11:42`         outcome.excess_return: 0
- `21:11:42`   [2] outcome keys: ['benchmark_price', 'asset_price', 'checked_at', 'correct', 'excess_return']
- `21:11:42`       predicted_dir: OUTPERFORM
- `21:11:42`       window_key: day_30
- `21:11:42`       logged_at: 2026-03-28T03:10:14.480976+00:00
- `21:11:42`       checked_at: 2026-04-27T22:30:03.042818+00:00
- `21:11:42`         outcome.benchmark_price: 715.17
- `21:11:42`         outcome.asset_price: 859.68
- `21:11:42`         outcome.checked_at: 2026-04-27T22:30:03.042818+00:00
- `21:11:42`         outcome.correct: None
- `21:11:42`         outcome.excess_return: 0
- `21:11:42`   [3] outcome keys: ['benchmark_price', 'asset_price', 'checked_at', 'correct', 'excess_return']
- `21:11:42`       predicted_dir: OUTPERFORM
- `21:11:42`       window_key: day_30
- `21:11:42`       logged_at: 2026-03-30T21:10:14.253493+00:00
- `21:11:42`       checked_at: 2026-04-29T22:30:03.226648+00:00
- `21:11:42`         outcome.benchmark_price: 711.58
- `21:11:42`         outcome.asset_price: 396.73
- `21:11:42`         outcome.checked_at: 2026-04-29T22:30:03.226648+00:00
- `21:11:42`         outcome.correct: None
- `21:11:42`         outcome.excess_return: 0
- `21:11:42`   [4] outcome keys: ['benchmark_price', 'asset_price', 'checked_at', 'correct', 'excess_return']
- `21:11:42`       predicted_dir: OUTPERFORM
- `21:11:42`       window_key: day_30
- `21:11:42`       logged_at: 2026-03-29T15:10:14.181104+00:00
- `21:11:42`       checked_at: 2026-04-28T22:30:03.016350+00:00
- `21:11:42`         outcome.benchmark_price: 711.69
- `21:11:42`         outcome.asset_price: 406.42
- `21:11:42`         outcome.checked_at: 2026-04-28T22:30:03.016350+00:00
- `21:11:42`         outcome.correct: None
- `21:11:42`         outcome.excess_return: 0

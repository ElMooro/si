# earnings-tracker pead_signals[0] keys

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-05-04T18:17:46+00:00  

## Log
- `18:17:46`   count: 10
- `18:17:46`   keys: ['ticker', 'filing_date', 'period_end', 'eps_actual', 'eps_prior_quarter', 'revenue_actual', 'eps_yoy_pct', 'returns', 'pead_label', 'pead_score']
- `18:17:46`   full sample: {
  "ticker": "ROKU",
  "filing_date": "2026-05-01",
  "period_end": "2026-03-31",
  "eps_actual": 0.58,
  "eps_prior_quarter": 0.5499999999999999,
  "revenue_actual": 1248879000.0,
  "eps_yoy_pct": 5.45,
  "returns": {
    "1d": 2.45,
    "5d": null,
    "20d": null
  },
  "pead_label": "STRONG_POSITIVE_DRIFT",
  "pead_score": 80
}
- `18:17:46`   -- distinct values in 'signal' field --
- `18:17:46`     None: 10
# momentum-scanner top_50_composite[0] keys

- `18:17:46`   count: 50
- `18:17:46`   keys: ['last_close', 'ret_1m', 'ret_3m', 'ret_6m', 'ret_12m', 'mom_12_1', 'vol_60d_annualized', 'vol_adj_mom_3m', 'pct_from_52w_high', 'high_252w', 'acceleration', 'symbol', 'name', 'sector', 'market_cap', 'rank_1m', 'rank_3m', 'rank_6m', 'rank_12m', 'composite_score']
- `18:17:46`   full sample[0]: {
  "last_close": 1244.42,
  "ret_1m": 77.371,
  "ret_3m": 87.063,
  "ret_6m": 535.492,
  "ret_12m": 3718.411,
  "mom_12_1": 2006.483,
  "vol_60d_annualized": 89.7,
  "vol_adj_mom_3m": 0.971,
  "pct_from_52w_high": -2.407,
  "high_252w": 1275.11,
  "acceleration": 92921.224,
  "symbol": "SNDK",
  "name": "Sandisk Corporation",
  "sector": "Technology",
  "market_cap": 175202351390.0,
  "rank_1m": 99.6,
  "rank_3m": 99.2,
  "rank_6m": 99.8,
  "rank_12m": 99.8,
  "composite_score": 99.6
}
- `18:17:46`   full sample[2]: {
  "last_close": 97.49,
  "ret_1m": 93.509,
  "ret_3m": 99.734,
  "ret_6m": 142.754,
  "ret_12m": 387.938,
  "mom_12_1": 146.266,
  "vol_60d_annualized": 76.48,
  "vol_adj_mom_3m": 1.304,
  "pct_from_52w_high": -2.947,
  "high_252w": 100.45,
  "acceleration": 275096.268,
  "symbol": "INTC",
  "name": "Intel Corporation",
  "sector": "Technology",
  "market_cap": 500639860000.0,
  "rank_1m": 99.8,
  "rank_3m": 99.4,
  "rank_6m": 98.4,
  "rank_12m": 98.4,
  "composite_score": 99.0
}
# event-study active themes

- `18:17:46`   top-level keys: ['version', 'generated_at', 'duration_s', 'as_of_date', 'horizons_trading_days', 'studies', 'active_themes', 'expected_21d_return_from_active_pct', 'data_sources', 'methodology']
- `18:17:46`   active_themes: []
- `18:17:46`   expected_21d_return_from_active_pct: None
- `18:17:46`     relevant: active_themes: []
- `18:17:46`     relevant: expected_21d_return_from_active_pct: None

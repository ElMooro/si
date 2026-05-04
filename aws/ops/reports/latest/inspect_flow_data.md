# flow-data.json shape

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-05-04T17:09:49+00:00  

## Log
- `17:09:49`   top-level keys: ['success', 'timestamp', 'engine', 'data', 'meta']
- `17:09:49`   success: True
- `17:09:49`   timestamp: 2026-05-04T17:07:42.573948Z
- `17:09:49`   engine: JustHodl Options Flow & Sentiment Engine v3.0
- `17:09:49`   data: dict keys=['vix_complex', 'skew', 'put_call', 'gamma_exposure', 'fund_flows', 'sentiment', 'unusual_activity', 'market_internals', 'trading_signals']
- `17:09:49`   meta: dict keys=['data_sources', 'refresh_interval', 'execution_ms']
# vix-curve.json

- `17:09:49`   full: {
  "generated_at": "2026-05-04T14:10:55+00:00",
  "vix_9d": 15.89,
  "vix_30d": 17.53,
  "vix_3m": 20.53,
  "vix_6m": 22.74,
  "vvix": 96.14,
  "slopes": {
    "slope_9_30": 0.1032,
    "slope_30_3m": 0.1711,
    "slope_3m_6m": 0.1076,
    "avg_slope": 0.1273
  },
  "regime": "steep_contango",
  "interpretation": "Steep contango (avg slope +12.7%). Markets calm at front month, pricing risk further out. The 'vol selling' trade structurally pays roll. Often associated with stable bull markets.",
  "fetch_errors": [],
  "fetch_duration_s": 1.4
}

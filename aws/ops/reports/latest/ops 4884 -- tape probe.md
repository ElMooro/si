- `02:16:04` ✅ polygon donor justhodl-spx-beaters env POLYGON_API_KEY
- `02:16:04` [poly-min] 200 bytes=85271
**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-08-18T02:16:06+00:00  

## Log
- `02:16:04`   minute bars n=835 sample={"v": 182633.738021, "vw": 776.0043, "o": 774.94, "c": 774.905, "h": 775.015, "l": 774.89, "t": 1786981320000, "n": 1217}
- `02:16:04` [poly-opt-snap] 200 bytes=2137
- `02:16:04`   opt results n=5 status=OK
- `02:16:04` [finra-2026-08-17] 200 bytes=545398
- `02:16:04`   lines=12284 hdr='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market' row1='20260817|A|170119.148454|94|322779.332031|B,Q,N'
- `02:16:05` [cboe-SPY] 200 bytes=6479897
- `02:16:05`   SPY options n=14546 keys=['option', 'bid', 'bid_size', 'ask', 'ask_size', 'iv', 'open_interest', 'volume', 'delta', 'gamma', 'vega', 'theta', 'rho', 'theo']
- `02:16:05`   sample={"option": "SPY261002P00810000", "bid": 36.18, "bid_size": 10.0, "ask": 39.7, "ask_size": 10.0, "iv": 0.111, "open_interest": 0.0, "volume": 0.0, "delta": -0.9003, "gamma": 0.0093, "vega": 0.5101, "theta": -0.0845, "rho": -0.4082, "theo": 37.9078, "change": 0.0, "open": 0.0, "high": 0.0, "low": 0.0, "tick": "no_change", "last_trade_price"
- `02:16:05`   data keys=['options', 'symbol', 'security_type', 'exchange_id', 'current_price', 'price_change', 'price_change_percent', 'bid', 'ask', 'bid_size', 'ask_size', 'open'] close=772.67
- `02:16:05` [cboe-_SPX] 200 bytes=13585384
- `02:16:05`   _SPX options n=30558 keys=['option', 'bid', 'bid_size', 'ask', 'ask_size', 'iv', 'open_interest', 'volume', 'delta', 'gamma', 'vega', 'theta', 'rho', 'theo']
- `02:16:05`   sample={"option": "SPXW260828P06790000", "bid": 0.65, "bid_size": 59.0, "ask": 0.9, "ask_size": 28.0, "iv": 0.3092, "open_interest": 151.0, "volume": 6.0, "delta": -0.0061, "gamma": 0.0, "vega": 0.2442, "theta": -0.2802, "rho": -0.0152, "theo": 0.7767, "change": 0.05, "open": 0.65, "high": 0.85, "low": 0.65, "tick": "up", "last_trade_price": 0.8
- `02:16:05`   data keys=['options', 'symbol', 'security_type', 'exchange_id', 'current_price', 'price_change', 'price_change_percent', 'bid', 'ask', 'bid_size', 'ask_size', 'open'] close=7745.0601
- `02:16:06` [s3 data/etf-true-flows.json] top keys=['engine', 'version', 'engine_class', 'generated_at', 'duration_s', 'n_etfs', 'maturity', 'evidence_tier', 'method', 'ground_truth']
- `02:16:06` [s3 data/13f-dollar-flows.json] absent (An error occurred (NoSuchKey) when calli)
- `02:16:06` [s3 data/short-squeeze.json] absent (An error occurred (NoSuchKey) when calli)
- `02:16:06` ✅ probe complete -- engine binds only the above

# v3 snapshot direct

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-05-04T17:16:14+00:00  

## Log
- `17:16:13`   url: https://api.polygon.io/v3/snapshot/options/SPY?greeks=true&limit=10&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d
- `17:16:13`   ✗ HTTP Error 403: Forbidden
# v3 reference contracts

- `17:16:13`   url: https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=SPY&limit=10&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBu
- `17:16:14`   status: OK
- `17:16:14`   keys: ['results', 'status', 'request_id', 'next_url']
- `17:16:14`   results len: 10
- `17:16:14`   first result keys: ['cfi', 'contract_type', 'exercise_style', 'expiration_date', 'primary_exchange', 'shares_per_contract', 'strike_price', 'ticker', 'underlying_ticker']
- `17:16:14`   first result: {"cfi": "OCASPS", "contract_type": "call", "exercise_style": "american", "expiration_date": "2026-05-04", "primary_exchange": "BATO", "shares_per_contract": 100, "strike_price": 500, "ticker": "O:SPY260504C00500000", "underlying_ticker": "SPY"}
# v3 unified snapshot

- `17:16:14`   url: https://api.polygon.io/v3/snapshot?ticker.any_of=SPY&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d
- `17:16:14`   status: OK
- `17:16:14`   keys: ['results', 'status', 'request_id']
- `17:16:14`   results len: 1
- `17:16:14`   first result keys: ['market_status', 'name', 'ticker', 'type', 'session', 'last_minute']
- `17:16:14`   first result: {"market_status": "open", "name": "State Street SPDR S&P 500 ETF Trust", "ticker": "SPY", "type": "stocks", "session": {"change": -4, "change_percent": -0.555, "early_trading_change": -0.58, "early_trading_change_percent": -0.0805, "regular_trading_change": -4, "regular_trading_change_percent": -0.555, "close": 716.72, "high": 722.12, "low": 714.99, "open": 720.07, "volume": 28257093.0, "previous_close": 720.65, "price": 716.65, "last_updated": 1777914974217097120, "vwap": 718.9476, "decimal_volume": "28257093.372000"}, "last_minute": {"close": 716.711, "high": 716.8, "low": 716.48, "transacti

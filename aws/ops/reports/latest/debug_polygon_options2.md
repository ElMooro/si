# List SPY contracts (next monthly)

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-05-04T17:17:47+00:00  

## Log
- `17:17:47`   status: OK  len: 50
- `17:17:47`   first 5: ['O:SPY260515C00360000', 'O:SPY260515C00365000', 'O:SPY260515C00370000', 'O:SPY260515C00375000', 'O:SPY260515C00380000']
# Unified snapshot on first option ticker — does it return IV/greeks?

- `17:17:47`   url: https://api.polygon.io/v3/snapshot?ticker.any_of=O:SPY260515C00360000&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d
- `17:17:47`   status: OK
- `17:17:47`   first result keys: ['ticker', 'error', 'message']
- `17:17:47`   full: {
  "ticker": "O:SPY260515C00360000",
  "error": "NOT_ENTITLED",
  "message": "Not entitled to this ticker."
}
# Unified snapshot batch (10 tickers)

- `17:17:47`   results len: 10
- `17:17:47`   ticker=O:SPY260515C00360000  type=None  keys=['ticker', 'error', 'message']
- `17:17:47`   ticker=O:SPY260515C00365000  type=None  keys=['ticker', 'error', 'message']
- `17:17:47`   ticker=O:SPY260515C00370000  type=None  keys=['ticker', 'error', 'message']

# Re-probe Worker with Mozilla UA

**Status:** success  
**Duration:** 11.9s  
**Finished:** 2026-04-26T01:26:45+00:00  

## Log
## A. POST api.justhodl.ai/investor with browser UA

- `01:26:44`   ✅ HTTP 200 len=7468
- `01:26:44`   preview: {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology", "price": 208.27, "metrics": {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology", "industry": "Semiconductors", "price": 208.27, "mktCap": 0.0, "pe": 0, "pb": 32.18, "priceToSales": 23.44, "pfcf": 0, "peg": null, "dcfUpside": 14.3, "analystUpside": 33.4, "buyPct": 76, "roe": 0, "roic": 0, "netMargin": 55.6, "grossMargin": 71.1, "fcfYield": 0, "debtEquity": 0, "currentRatio": 3.91, "piotroski": "N/A", "altmanZ": 0, "ownerEarningsYield": 0.73, "ownerEarningsPS": 1.53, "revenueGrowth": 0.0, "epsGrowth": 0.
## B. /research GET with browser UA

- `01:26:45`   ✅ /research HTTP 200 len=4089
## C. /investor.html static page reachable from GitHub Pages

- `01:26:45`   ✅ /investor.html HTTP 200 len=24625
- `01:26:45`   has tickerInput: False
- `01:26:45`   calls api.justhodl.ai/investor: False
- `01:26:45` Done

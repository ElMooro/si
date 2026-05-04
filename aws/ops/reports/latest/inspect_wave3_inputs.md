# Inspect Wave 3 inputs

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-05-04T18:50:23+00:00  

## Log
# 1. Options/skew availability

- `18:50:22`   ✗ data/options-flow.json missing
- `18:50:22`   ✗ data/flow-data.json missing
- `18:50:22` ✅   ✓ flow-data.json  29,317b
- `18:50:22`   ✗ data/iv-surface.json missing
# 2. News sources

- `18:50:22`   ✗ data/news-feed.json missing
- `18:50:22`   ✗ news.json missing
- `18:50:22`   ✗ data/morning-intelligence.json missing
- `18:50:22`   ✗ data/intel.json missing
- `18:50:22`   ✗ intel.json missing
# 3. Screener tickers (S&P 500)

- `18:50:22` ✅   ✓ screener/data.json — 503 entries
- `18:50:22`   sample keys: ['symbol', 'name', 'sector', 'industry', 'price', 'beta', 'volume', 'marketCap', 'peRatio', 'pbRatio', 'psRatio', 'evEbitda', 'roe', 'roa', 'roic']
# 4. Polygon options entitlement check (test direct snapshot)

- `18:50:23`   ✗ https://api.polygon.io/v3/snapshot/options/SPY?apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0k: HTTP Error 403: Forbidden
# 5. Other Wave 3 inputs

- `18:50:23` ✅   ✓ data/insider-trades.json  15,120b
- `18:50:23`   ✗ data/13f-changes.json missing
- `18:50:23` ✅   ✓ data/earnings-tracker.json  29,894b
- `18:50:23` ✅   ✓ data/whats-changed.json  1,657b
- `18:50:23`   ✗ data/morning-brief.json missing

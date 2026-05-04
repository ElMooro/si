# flow-data.json contents

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-05-04T18:51:51+00:00  

## Log
- `18:51:50`   top-level keys: ['success', 'timestamp', 'engine', 'data', 'meta']
- `18:51:50`   success: True
- `18:51:50`   timestamp: 2026-05-04T18:47:42.585171Z
- `18:51:50`   engine: JustHodl Options Flow & Sentiment Engine v3.0
- `18:51:50`   data: ['vix_complex', 'skew', 'put_call', 'gamma_exposure', 'fund_flows', 'sentiment', 'unusual_activity', 'market_internals']
- `18:51:50`   meta: ['data_sources', 'refresh_interval', 'execution_ms']
# screener/data.json sample

- `18:51:51`   total: 503 stocks
- `18:51:51`   full first sample:
- `18:51:51`     symbol: CASY
- `18:51:51`     name: Casey's General Stores, Inc.
- `18:51:51`     sector: Consumer Cyclical
- `18:51:51`     industry: Specialty Retail
- `18:51:51`     price: 835.92
- `18:51:51`     beta: 0.605
- `18:51:51`     volume: 377678
- `18:51:51`     marketCap: 30984988126.0
- `18:51:51`     peRatio: 47.62
- `18:51:51`     pbRatio: 8.0337
- `18:51:51`     psRatio: 1.8246
- `18:51:51`     evEbitda: 22.124
- `18:51:51`     roe: 0.18
- `18:51:51`     roa: 0.08
- `18:51:51`     roic: 0.11
- `18:51:51`     grossMargin: 0.24
- `18:51:51`     operatingMargin: 0.06
- `18:51:51`     netMargin: 0.04
- `18:51:51`     revenueGrowth: 0.07
- `18:51:51`     epsGrowth: 0.09
- `18:51:51`     fcfGrowth: 0.58
- `18:51:51`     debtToEquity: 0.7515
- `18:51:51`     currentRatio: 1.042
- `18:51:51`     dividendYield: 0.0
- `18:51:51`     interestCoverage: 13.4474
- `18:51:51`     piotroski: 9
- `18:51:51`     altmanZ: 6.952
- `18:51:51`     instSignal: buying
- `18:51:51`     instHolders: None
- `18:51:51`     instChgPct: None
- `18:51:51`     sma50: 720.08
- `18:51:51`     sma200: 598.12
- `18:51:51`     crossSignal: None
- `18:51:51`     crossDaysAgo: None
- `18:51:51`     chg1d: 1.6749
- `18:51:51`     chg1w: 6.3417
- `18:51:51`     chg1m: 11.7884
- `18:51:51`     chg3m: 29.0478
- `18:51:51`     chg6m: 59.847
- `18:51:51`     chg1y: 79.2358
# insider-trades.json shape

- `18:51:51`   top-level keys: ['generated_at', 'window_days', 'cluster_window_days', 'thresholds', 'stats', 'clusters', 'big_buys', 'transactions']
- `18:51:51`   clusters: list len=1
- `18:51:51`     sample keys: ['ticker', 'company', 'cik', 'insider_count', 'transactions', 'total_shares', 'total_value', 'avg_price', 'first_filing', 'last_filing', 'insiders']
- `18:51:51`   big_buys: list len=8
- `18:51:51`     sample keys: ['ticker', 'company', 'cik', 'insider', 'role', 'code', 'code_meaning', 'side', 'shares', 'price', 'value', 'txn_date', 'accession', 'filed_at']
- `18:51:51`   transactions: list len=29
- `18:51:51`     sample keys: ['ticker', 'company', 'cik', 'insider', 'role', 'code', 'code_meaning', 'side', 'shares', 'price', 'value', 'txn_date', 'accession', 'filed_at']

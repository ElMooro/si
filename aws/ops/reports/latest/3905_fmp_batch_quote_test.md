# ops 3905 — test the exact FMP batch-quote-short endpoint signal-backtest uses

**Status:** success  
**Duration:** 2.8s  
**Finished:** 2026-07-26T04:32:40+00:00  

## Data

| all_env_keys | length | n_results | present | response_type |
|---|---|---|---|---|
| ['ANTHROPIC_API_KEY', 'CMC_KEY', 'FMP_KEY', 'FRED_KEY', 'POLYGON_KEY'] | 32 |  | True |  |
|  |  | 3 |  | list |

## Log
## 1. live FMP_KEY on signal-backtest itself

## 2. the EXACT endpoint + real tickers from the FICO sample seen in ops 3899

- `04:32:38` ✅   SUCCESS: [{"symbol": "FICO", "price": 1237.37, "change": 31.18, "volume": 125566}, {"symbol": "AAPL", "price": 333.02, "change": 11.36, "volume": 47402209}, {"symbol": "MSFT", "price": 381.7, "change": 0.12001, "volume": 27544509}]
## 3. sanity check — does /stable/quote (singular, older-style) work as a comparison

- `04:32:39` ✅   /stable/quote works: [{"symbol": "AAPL", "name": "Apple Inc.", "price": 333.02, "changePercentage": 3.53168, "change": 11.36, "volume": 47402209, "dayLow": 321.62, "dayHigh": 334.37, "yearHigh": 334.99, "yearLow": 201.5, "marketCap": 4891183295120, "priceAvg50": 306.2314, "priceAvg200": 275.98044, "exchange": "NASDAQ", "open": 322.04, "previousClose": 321.66, "timestamp": 1784923201}]
## 4. real CloudWatch invocation history for signal-backtest

- `04:32:40`   most recent run log tail:
INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1b20f
START RequestId: 6fd69afe-46e8-4cd0-afb9-eabd35bc2009 Version: $LATEST
[bt] 70 opportunity snapshots
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] quote chunk err: HTTP Error 429: Too Many Requests
[bt] 57997 obs, 0 live prices
[bt] ai_analysis: Validation is impossible because the dataset contains zero observations (n=0).
[bt] DONE 31.5s — 0 obs, maturity BOOTSTRAPPING
[ic] dates_used=35 maturity=MATURE composite_ic=-0.1219
[bt] factor-ic: MATURE dates_used=35
END RequestId: 6fd69afe-46e8-4cd0-afb9-eabd35bc2009
REPORT RequestId: 6fd69afe-46e8-4cd0-afb9-eabd35bc2009	Duration: 35927.43 ms	Billed Duration: 36424 ms	Memory Size: 512 MB	Max Memory Used: 160 MB	Init Duration: 496.48 ms	
XRAY TraceId: 1-6a64dda9-19e029957df6f1ae6722420a	SegmentId: e013e7eec1f346d7	Sampled: true
- `04:32:40` ✅ PROBE COMPLETE

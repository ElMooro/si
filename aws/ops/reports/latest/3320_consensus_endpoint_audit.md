- `01:59:58` ✅ audit complete — see rows for which paths 400
**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-07-15T01:59:58+00:00  

## Data

| RESULT | earnings-surprises | earnings-surprises-bulk | grades-consensus | grades-latest-news | grades-news | key_suffix | price-target-consensus | quote |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | S8xb |  |  |
|  |  |  |  |  |  |  |  | {'http': 200, 'n': 1, 'fields': ['symbol', 'name', 'price', 'changePercentage', 'change', 'volume', 'dayLow', 'dayHigh']} |
|  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - year'} |  |  |  |  |  |  |
|  | {'http': 404, 'err': '[]'} |  |  |  |  |  |  |  |
|  |  |  |  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - symbol'} |  |  |  |
|  |  |  |  | {'http': 200, 'n': 500, 'fields': ['symbol', 'publishedDate', 'newsURL', 'newsTitle', 'newsBaseURL', 'newsPublisher', 'newGrade', 'previousGrade']} |  |  |  |  |
|  |  |  | {'http': 200, 'n': 1, 'fields': ['symbol', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell', 'consensus']} |  |  |  |  |  |
|  |  |  |  |  |  |  | {'http': 200, 'n': 1, 'fields': ['symbol', 'targetHigh', 'targetLow', 'targetConsensus', 'targetMedian']} |  |
| AUDIT_DONE |  |  |  |  |  |  |  |  |

## Log


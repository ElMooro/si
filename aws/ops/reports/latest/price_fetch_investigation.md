# Investigate why outcome-checker returns None for all prices

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-04-24T23:13:46+00:00  

## Log
## A. Test Polygon /v2/last/trade endpoint

- `23:13:45`   SPY: status=403 ok=False
- `23:13:45`     body: {"status":"NOT_AUTHORIZED","request_id":"a32cba9885e469366c897fb0656b1ee1","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}
- `23:13:45`   AAPL: status=403 ok=False
- `23:13:45`     body: {"status":"NOT_AUTHORIZED","request_id":"00393d106eb462cc01b8919a053fc800","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}
- `23:13:45`   NVDA: status=403 ok=False
- `23:13:45`     body: {"status":"NOT_AUTHORIZED","request_id":"ffec7239a77a1910d2830fc928e970a9","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}
- `23:13:45`   X:BTCUSD: status=200 ok=True
- `23:13:45`     preview: {"results":{"T":"X:BTCUSD","c":[2],"i":"1007958951","p":77421.13,"s":1.2e-07,"x":1,"y":1777072424036621000},"status":"OK","request_id":"326fbb6b5df00ebff8abd4a8748c3b37"}
- `23:13:45`   GLD: status=403 ok=False
- `23:13:45`     body: {"status":"NOT_AUTHORIZED","request_id":"2e3eb8ef8dd810741be47009df7d991d","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}
## B. Test FMP /api/v3/quote-short endpoint

- `23:13:45`   SPY: status=403 ok=False
- `23:13:45`     body: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please 
- `23:13:45`   AAPL: status=403 ok=False
- `23:13:45`     body: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please 
- `23:13:45`   NVDA: status=403 ok=False
- `23:13:45`     body: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please 
- `23:13:46`   GLD: status=403 ok=False
- `23:13:46`     body: {
  "Error Message": "Legacy Endpoint : Due to Legacy endpoints being no longer supported - This endpoint is only available for legacy users who have valid subscriptions prior August 31, 2025. Please 
## C. Full outcome-checker error lines from recent run

- `23:13:46`   Found 44 error lines in last run:
- `23:13:46`     [PRICE] Polygon error for CVNA: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for CVNA: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for COIN: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for COIN: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for SPY: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for SPY: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for Q: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for Q: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for TTD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for TTD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for IBKR: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for IBKR: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for HOOD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for HOOD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for GLD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for GLD: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for SNDK: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for SNDK: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] Polygon error for APP: HTTP Error 403: Forbidden
- `23:13:46`     [PRICE] FMP error for APP: HTTP Error 403: Forbidden
## D. Outcome-checker environment config

- `23:13:46`   Env var keys: []
- `23:13:46`   Timeout: 300s, Memory: 256 MB
- `23:13:46` Done

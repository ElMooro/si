## BENZINGA ENDPOINT MATRIX

**Status:** failure  
**Duration:** 1.0s  
**Finished:** 2026-07-15T01:24:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| RESULT | api.massive.com/benzinga/v1/ratings [bearer] | api.massive.com/benzinga/v1/ratings [query] | api.massive.com/v1/account [query] | api.massive.com/v1/benzinga/ratings [bearer] | api.massive.com/v1/benzinga/ratings [query] | api.polygon.io/benzinga/v1/ratings [bearer] | api.polygon.io/benzinga/v1/ratings [query] | api.polygon.io/v1/marketstatus/now [query] | api.polygon.io/v1/reference/benzinga/ratings [bearer] | api.polygon.io/v1/reference/benzinga/ratings [query] | api.polygon.io/v3/reference/tickers [query] |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | {'http': 403, 'err': '{"status":"NOT_AUTHORIZED","request_id":"e30b824b977b8c8f2536092288564051","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}'} |  |  |  |  |
|  |  |  |  |  |  | {'http': 403, 'err': '{"status":"NOT_AUTHORIZED","request_id":"417bd2f3b221e4fbd72683f5fdc0e695","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}'} |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | {'http': 404, 'err': '404 page not found'} |  |
|  |  |  |  |  |  |  |  |  | {'http': 404, 'err': '404 page not found'} |  |  |
|  |  | {'http': 403, 'err': '{"status":"NOT_AUTHORIZED","request_id":"522de5e82defa8251efc1320f838784d","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}'} |  |  |  |  |  |  |  |  |  |
|  | {'http': 403, 'err': '{"status":"NOT_AUTHORIZED","request_id":"aacc3276db92714d8ba28c9f808f80e5","message":"You are not entitled to this data. Please upgrade your plan at https://massive.com/pricing"}'} |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | {'http': 404, 'err': '404 page not found'} |  |  |  |  |  |  |
|  |  |  |  | {'http': 404, 'err': '404 page not found'} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {'http': 200, 'shape': {'keys': ['afterHours', 'currencies', 'earlyHours', 'exchanges', 'indicesGroups', 'market']}} |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | {'http': 200, 'shape': {'keys': ['results', 'status', 'request_id', 'count', 'next_url'], 'status': 'OK', 'count': 1, 'results': 1}} |
|  |  |  | {'http': 404, 'err': '404 page not found'} |  |  |  |  |  |  |  |  |
| WRONG_KEY_FOR_BENZINGA |  |  |  |  |  |  |  |  |  |  |  |

## Log
## IDENTITY / BASE ENTITLEMENT

## VERDICT

- `01:24:23` ✗ Base polygon/massive endpoints authorize (200) but EVERY Benzinga path returns 403 -> this key lacks the Benzinga scope. The paid Benzinga entitlement is under a DIFFERENT key/account than /justhodl/massive-api-key. Need the key tied to the Benzinga-entitled Massive account.

## KEY SOURCE

**Status:** failure  
**Duration:** 1.1s  
**Finished:** 2026-07-15T01:31:03+00:00  

## Error

```
SystemExit: 1
```

## Data

| RESULT | deployed_fp | deployed_key_present | guidance [apikey] | guidance [token] | guidance [x-api-key] | key_fp | ratings [apikey] | ratings [token] | ratings [x-api-key] | using |
|---|---|---|---|---|---|---|---|---|---|---|
|  | {'len': 34, 'prefix': 'bzMJ62'} | True |  |  |  | {'len': 34, 'prefix': 'bzMJ62'} |  |  |  | deployed |
|  |  |  |  |  |  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |  |
|  |  |  |  |  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |  |  |
|  |  |  |  |  |  |  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |
|  |  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |  |  |  |  |  |
|  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |  |  |  |  |  |  |
|  |  |  |  |  | {'http': 401, 'err': '["Access denied for user 0 \\"anonymous\\""]'} |  |  |  |  |  |
| DIRECT_ALSO_DEAD |  |  |  |  |  |  |  |  |  |  |

## Log
## RATINGS (calendar/ratings)

## GUIDANCE (calendar/guidance)

## VERDICT

- `01:31:03` ✗ Direct Benzinga key did not authorize on any method — this key/plan is not live either. Then the paid Benzinga entitlement Khalid confirmed is on an account whose key we don't yet hold; he'll need to paste the current key.

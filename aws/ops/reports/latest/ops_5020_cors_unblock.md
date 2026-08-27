# ops 5020 -- browser auto-upgrade unblock (CORS)

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-08-27T19:57:06+00:00  

## Data

| acao | body_head | status |
|---|---|---|
| * | {"schema_version": "2.9.3", "technicals": {"available": true | 200 |

## Log
## G0 current function-URL config

- `19:57:06` before: {"AllowCredentials": false, "AllowHeaders": ["*"], "AllowMethods": ["*"], "AllowOrigins": ["*"], "ExposeHeaders": ["*"], "MaxAge": 86400}
## G1 config change (only if needed)

- `19:57:06` ✅ origin already allowed — no change made
## P1 prove it with an Origin-header request

- `19:57:06` ✅ browser-origin requests now receive Access-Control-Allow-Origin — stale docs self-upgrade from the page for every ticker and every future schema bump

# ops 4118 — loud deterministic deploy

**Status:** failure  
**Duration:** 727.9s  
**Finished:** 2026-07-30T04:02:31+00:00  

## Error

```
SystemExit: 1
```

## Data

| current_marker | new_zip_files | zip_files |
|---|---|---|
| tradingview-vault v3.15.2 ops4117 indent-true |  | 27 |
|  | 27 |  |

## Log
## A. what is deployed RIGHT NOW

- `03:50:24`   names: lambda_function.py, _fred_shim.py, _sentry_lite.py, anthropic_shim.py, api_auth.py, benzinga.py, calibration.py, capital_flow.py, census_lib.py, claude_compat.py, edgar.py, engine_trust.py
## B. build pipeline-layout zip + update, LOUDLY

- `03:50:25` ✅   update accepted (attempt 0)
- `03:50:25`   [0] state=('Active', 'InProgress', 'The function is being created.')
- `03:50:34` ✅   settled at loop 1
## C. invoke + poll + gates

- `04:02:31` ✗ artifact never moved to v3.15.2

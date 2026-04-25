# Fix justhodl-intelligence: data.json + predictions.json → adapters

**Status:** success  
**Duration:** 3.7s  
**Finished:** 2026-04-25T00:04:25+00:00  

## Data

| downstream_impact | fix | legacy_shape_preserved |
|---|---|---|
| ml_risk + carry_risk signals get real values | reads data/report.json + synthesizes pred from healthy sources | True |

## Log
- `00:04:21` ✅   Replaced load_system_data with adapter version
- `00:04:21` ✅   Source valid (39383 bytes), saved
- `00:04:24` ✅   Deployed justhodl-intelligence (11,231 bytes)
## Trigger fresh justhodl-intelligence run

- `00:04:25` ✅   Async-triggered (status 202)
- `00:04:25`   intelligence-report.json should refresh in ~30s
- `00:04:25`   Verification script next.
- `00:04:25` Done

# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 213.2s  
**Finished:** 2026-08-13T16:48:48+00:00  

## Error

```
SystemExit: 1
```

## Data

| census | fmp | fn_error | gates | mode | scored | tiers | universe |
|---|---|---|---|---|---|---|---|
|  |  | Unhandled |  |  |  |  |  |
| None | True |  | {"below_sma": 0, "eps_seq": 0, "dilution": 0, "margin_floor": 0} | None | 0 | {"EXPLOSIVE-SETUP": 0, "SETUP": 0, "WATCH": 0, "SCREENED": 0} | 0 |

## Log
## FMP key donor -> engine env

- `16:45:15` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## deploy (create-capable) + schedule

- `16:45:28` ✅   [deploy] v1.0.1 live (created=False)
## run + institutional truth

- `16:45:40` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `16:45:40` CW| File "/var/task/lambda_function.py", line 241, in lambda_handler
- `16:45:40` CW| File "/var/task/lambda_function.py", line 161, in sma_state
- `16:45:40` census fields: []
- `16:45:40` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `16:45:40` ✗   [scored] CONTRACT MISS — 0 scored rows
- `16:45:40` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `16:45:40` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `16:45:41` CF purge issued
- `16:48:48` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `16:48:48` ✗ stock-buying: 4 red

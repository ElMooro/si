# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 216.2s  
**Finished:** 2026-08-13T16:42:20+00:00  

## Error

```
SystemExit: 1
```

## Data

| census | fmp | fn_error | gates | mode | scored | tiers | universe |
|---|---|---|---|---|---|---|---|
|  |  | None |  |  |  |  |  |
| None | False |  | {"below_sma": 0, "eps_seq": 0, "dilution": 0, "margin_floor": 0} | None | 0 | {"EXPLOSIVE-SETUP": 0, "SETUP": 0, "WATCH": 0, "SCREENED": 0} | 0 |

## Log
## FMP key donor -> engine env

- `16:38:44` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## deploy (create-capable) + schedule

- `16:39:12` ✅   [deploy] v1.0.0 live (created=False)
- `16:39:12` hourly schedule created
## run + institutional truth

- `16:39:14` census fields: []
- `16:39:14` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `16:39:14` ✗   [scored] CONTRACT MISS — 0 scored rows
- `16:39:14` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `16:39:14` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `16:39:14` CF purge issued
- `16:39:19` edge 1: HTTP Error 404: Not Found
- `16:39:39` edge 2: HTTP Error 404: Not Found
- `16:39:59` edge 3: HTTP Error 404: Not Found
- `16:40:20` edge 4: HTTP Error 404: Not Found
- `16:40:40` edge 5: HTTP Error 404: Not Found
- `16:41:00` edge 6: HTTP Error 404: Not Found
- `16:41:20` edge 7: HTTP Error 404: Not Found
- `16:42:20` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `16:42:20` ✗ stock-buying: 4 red

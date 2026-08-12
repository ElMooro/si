# ops 4630 — barometer + TE join

**Status:** failure  
**Duration:** 17.8s  
**Finished:** 2026-08-12T02:45:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | extreme | label | resolved | shock | stretched | top_extremes |
|---|---|---|---|---|---|---|
| 13.1 | 27.0 | QUIET | 361 | 0.6 | 13.2 | [{"symbol": "FRED:PCOPPUSDM", "range_pos_pct": 100.0}, {"symbol": "NASDAQ:TLT", "range_pos_pct": 0.0}, {"symbol": "FRED:HOANBS", "range_pos_pct": 100.0}, {"symbol": "FRED:RRSFS", "range_pos_pct": 100. |

## Log
## deploy-settle

- `02:45:11` ✅   [deploy] blackswan v1.4.2 + signal v2.1.3
## run + barometer truth

- `02:45:22` ✅   [barometer] barometer 13.1 (QUIET)
- `02:45:22` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `02:45:22` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
- `02:45:22` ✗   [ffill-composite] CONTRACT MISS — SOFR-FEDFUNDS z-based: z=-:only 13 ffilled common dates None
- `02:45:22` ✅   [resolution] 361/500 — structural ceiling until extension vault sync (TE category map exhausted)
## board + edge

- `02:45:27` ✅   [canary-barometer] board carries barometer 13.1 (QUIET)
- `02:45:28` ✅   [edge] edge serves the barometer
## verdict

- `02:45:28` ✗ barometer: 1 red

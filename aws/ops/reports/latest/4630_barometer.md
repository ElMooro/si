# ops 4630 — barometer + TE join

**Status:** failure  
**Duration:** 19.5s  
**Finished:** 2026-08-12T02:42:03+00:00  

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

- `02:41:45` ✅   [deploy] blackswan v1.4.1 + signal v2.1.3
## run + barometer truth

- `02:41:58` ✅   [barometer] barometer 13.1 (QUIET)
- `02:41:58` TE-joined: ['ECONOMICS:CLGDPYY', 'ECONOMICS:CLUR', 'ECONOMICS:CHUR', 'ECONOMICS:HKUR', 'ECONOMICS:USRSYY', 'ECONOMICS:EURSYY', 'ECONOMICS:NLCU', 'ECONOMICS:USBCOI', 'ECONOMICS:FIGDPYY', 'ECONOMICS:CHBCOI', 'ECONOMICS:FIBCOI', 'ECONOMICS:KYGDPYY']
- `02:41:58` ✅   [te-join] 42 ECONOMICS rows via Trading Economics
- `02:41:58` ✗   [ffill-composite] CONTRACT MISS — SOFR-FEDFUNDS z-based: z=None None
- `02:41:58` ✅   [resolution] 361/500 — structural ceiling until extension vault sync (TE category map exhausted)
## board + edge

- `02:42:03` ✅   [canary-barometer] board carries barometer 13.1 (QUIET)
- `02:42:03` ✅   [edge] edge serves the barometer
## verdict

- `02:42:03` ✗ barometer: 1 red

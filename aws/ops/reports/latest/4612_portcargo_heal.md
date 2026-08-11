# ops 4612 — port-cargo self-healing layer resolve

**Status:** failure  
**Duration:** 97.6s  
**Finished:** 2026-08-11T22:30:58+00:00  

## Error

```
SystemExit: 1
```

## Data

| data_age_days | datefield | fetch_status | gaps | latest_data_date | n_ports | n_rows | physical | resolver |
|---|---|---|---|---|---|---|---|---|
| 4 | date | OK | [] | 2026-08-07 | 2065 | 80535 |  | candidate:Daily_Ports_Data |
|  |  |  |  |  |  |  | {"composite": 46.9, "signal": "NEUTRAL", "confidence": "HIGH"} |  |

## Log
## raw upstream probes (ground truth)

- `22:29:20` old layer → 200, 7773 bytes, head: {   "objectIdFieldName" : "ObjectId",    "uniqueIdField" :    {     "name" : "ObjectId",      "isSystemMaintained" : true   },    "globalIdFieldName" : "",    "fi
- `22:29:21` org directory → 200, 46926 bytes, head: {   "currentVersion" : 12,    "services" : [     {       "name" : "2018_BP_Homepage_Indicator",        "type" : "FeatureServer",        "url" : "https://services9.a
- `22:29:21` services containing 'port': ["disruptions_ExportFeatures", "disruptions_with_ports", "FR24_airports", "portsfacts", "PortWatch_boundaries", "PortWatch_chokepoints_database", "PortWatch_countries", "PortWatch_Countries_Simplified", "portwatch_disruptions_database", "PortWatch_ports", "PortWatch_ports_database", "PortWatchPorts_globe"]
## deploy-settle v1.2.0

- `22:29:22` v1.2.0 live (attempt 1)
- `22:29:22` ✅   [deploy] port-cargo v1.2.0
## invoke + healed-fetch contracts

- `22:30:52` invoke status=200
- `22:30:53` ✅   [fetch] fetch_status OK (resolver=candidate:Daily_Ports_Data)
- `22:30:53` ✅   [fresh] data date 2026-08-07, age 4 d (PortWatch lag ~4-6d)
- `22:30:53` ✅   [breadth] 2065 ports carrying data
## fifth leg: physical-economy join

- `22:30:57` ✗   [fifth-leg] CONTRACT MISS — physical signal now 4 legs: ["PJM demand momentum (8d)", "PJM power-price shock canary", "Grid buildout quality (executed-IA share)", "Freight pulse"]
## edge

- `22:30:58` ✅   [edge] edge port-cargo.json shows OK
## verdict

- `22:30:58` ✗ port-cargo heal: 1 red (raw probes above hold the ground truth for the next patch)

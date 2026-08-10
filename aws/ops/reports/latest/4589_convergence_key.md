# ops 4589 — convergence key, polled correctly

**Status:** failure  
**Duration:** 31.0s  
**Finished:** 2026-08-10T22:29:44+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:29:13` fired; polling data/impact/convergence.json
- `22:29:44` convergence refreshed (30s)
- `22:29:44` ✅ relationship note: name-level authority: justhodl-flow-confluence (pre-existing, trust-gated) — consumed here as a vote source; this board 
- `22:29:44` ✗ no flow_confluence votes despite 40 multi-engine names — rows: [('Software - Infrastructure', ['flow_lookthrough', 'dark_pool']), ('Construction Materials', ['flow_lookthrough', 'dark_pool']), ('REIT - Office', ['flow_lookthrough', 'dark_pool']), ('Railroads', ['flow_lookthrough', 'dark_pool'])]
- `22:29:44`   Software - Infrastructure ACCUMULATION score=2.0 sources=['flow_lookthrough', 'dark_pool']
- `22:29:44`   Construction Materials ACCUMULATION score=1.0 sources=['flow_lookthrough', 'dark_pool']
- `22:29:44`   REIT - Office DISTRIBUTION score=0.0 sources=['flow_lookthrough', 'dark_pool']
- `22:29:44`   Railroads DISTRIBUTION score=0.0 sources=['flow_lookthrough', 'dark_pool']
- `22:29:44`   Travel Services DISTRIBUTION score=0.0 sources=['flow_lookthrough', 'dark_pool']
- `22:29:44` ✗ 1 red

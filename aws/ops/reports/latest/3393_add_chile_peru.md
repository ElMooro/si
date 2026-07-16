## Deploy global-sovereign v1.3.0 (+Chile +Peru)

**Status:** success  
**Duration:** 87.9s  
**Finished:** 2026-07-16T15:51:36+00:00  

## Log
- `15:50:09`   zip: 79914 bytes
## 1. Lambda

- `15:50:09`   Lambda exists — updating
- `15:50:14` ✅   ✓ updated justhodl-global-sovereign
## 2. EB rule + permissions

- `15:50:15`   rule already correct: global-sovereign-12h (cron(15 6,18 * * ? *))
- `15:50:15` ✅   ✓ target → justhodl-global-sovereign
- `15:50:15` ✅   ✓ added invoke permission
- `15:50:15` harvesting…
- `15:51:30` ✅ EURODOLLAR-HUB STRESS = 47.4 (n=21 w/CDS, avg 22.9bp)
- `15:51:30`   worst hub (canary): Chile @ 67.0bp → stress 82.7
- `15:51:30`   Chile in set: ✓ · Peru in set: (no CDS — tracked, not in composite)
- `15:51:30`   named hubs all harvested: ✓ all present
- `15:51:30`   top-8 hub ladder (most→least stressed):
- `15:51:30`     Chile: 67.0bp (stress 82.7)
- `15:51:30`     Canada: 39.6bp (stress 46.1)
- `15:51:30`     United States: 37.8bp (stress 43.7)
- `15:51:30`     Hong Kong: 36.2bp (stress 41.6)
- `15:51:30`     France: 31.2bp (stress 35.0)
- `15:51:30`     Italy: 29.9bp (stress 33.2)
- `15:51:30`     Greece: 29.7bp (stress 32.9)
- `15:51:30`     Japan: 26.4bp (stress 28.5)
- `15:51:36` ✅ JSI eurodollar feed now 47.4 (reflects Chile/Peru).

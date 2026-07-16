## Deploy global-sovereign v1.2.0 (eurodollar-hub stress)

**Status:** success  
**Duration:** 31.6s  
**Finished:** 2026-07-16T15:40:57+00:00  

## Log
- `15:40:26`   zip: 79792 bytes
## 1. Lambda

- `15:40:26`   Lambda exists — updating
- `15:40:29` ✅   ✓ updated justhodl-global-sovereign
## 2. EB rule + permissions

- `15:40:30`   rule already correct: global-sovereign-12h (cron(15 6,18 * * ? *))
- `15:40:30` ✅   ✓ target → justhodl-global-sovereign
- `15:40:30` ✅   ✓ added invoke permission
- `15:40:31` harvesting eurodollar hubs…
- `15:40:37` ✅ EURODOLLAR-HUB STRESS = 31.0 (avg CDS 20.7bp, n=20)
- `15:40:37`   worst hub (the canary): Canada @ 39.6bp → stress 46.1
- `15:40:37`   hub detail (most→least stressed):
- `15:40:37`     Canada: 39.6bp (stress 46.1)
- `15:40:37`     United States: 37.8bp (stress 43.7)
- `15:40:37`     Hong Kong: 36.2bp (stress 41.6)
- `15:40:37`     France: 31.2bp (stress 35.0)
- `15:40:37`     Italy: 29.9bp (stress 33.2)
- `15:40:37`     Greece: 29.7bp (stress 32.9)
- `15:40:37`     Japan: 26.4bp (stress 28.5)
- `15:40:37`     Singapore: 25.6bp (stress 27.4)
- `15:40:37`     South Korea: 22.6bp (stress 23.5)
- `15:40:37`     United Kingdom: 18.7bp (stress 18.2)
- `15:40:37`     Belgium: 17.6bp (stress 16.8)
- `15:40:37`     Spain: 16.4bp (stress 15.2)
## Deploy JSI v1.8.0 (Eurodollar Hub Stress feed)

- `15:40:37`   zip: 83120 bytes
## 1. Lambda

- `15:40:37`   Lambda exists — updating
- `15:40:43` ✅   ✓ updated justhodl-stress-index
## 2. EB rule + permissions

- `15:40:43`   rule already correct: jsi-6h (rate(6 hours))
- `15:40:44` ✅   ✓ target → justhodl-stress-index
- `15:40:44` ✅   ✓ added invoke permission
- `15:40:57` JSI v1.8.0 overlay=20 live=20 jsi=35.82
- `15:40:57` ✅ EURODOLLAR HUB STRESS wired into JSI — 31.0 (danger-first: pack-avg + worst-hub, Global Risk group).

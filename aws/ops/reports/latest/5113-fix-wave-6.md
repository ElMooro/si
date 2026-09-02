# ops 5113 -- fix wave 6: post-deploy-only verification

**Status:** failure  
**Duration:** 3027.5s  
**Finished:** 2026-09-02T04:55:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| engine | errors | invocations | ports | requests | since | with_yoy |
|---|---|---|---|---|---|---|
| portwatch |  |  | 58 | 9 |  | 24 |
| justhodl-census-us | 0 | 317 |  |  | 2026-09-02T02:53+00:00 |  |
| justhodl-repo-monitor | 0 | 8 |  |  | 2026-09-02T02:53+00:00 |  |
| justhodl-import-sentinel | 0 | 15 |  |  | 2026-09-02T02:53+00:00 |  |

## Log
- `04:04:50` ✅ justhodl-portwatch deployed (2026-09-02T03:28:56.000+0000) after 0s
- `04:19:57` ⚠ justhodl-census-us: deploy not observed within 900s
- `04:35:04` ⚠ justhodl-repo-monitor: deploy not observed within 900s
## portwatch v1.6.5

- `04:35:12` invoke 200 b'{"ok": true, "chokepoints": 28, "worst": {"name": "Kerch Strait", "z": -1.48, "vs_baseline_pct": -98.9, "status": "DISRUPTED"}, "rows": 10948}'
- `04:35:12` v1.6.5: ports=58 with_yoy=24 requests={"n": 9, "throttled_429": 0, "budget": 140} history_through={"choke": "2026-08-23", "ports": "2026-08-28"} errors=[]
- `04:35:12`   sample: [('Jeddah', 'Saudi Arabia', -68.1, 379), ('Keelung', 'Taiwan Province of China', -53.5, 379), ('Kaohsiung', 'Taiwan Province of China', -24.3, 379), ('Lirquen', 'Chile', -83.3, 374), ('Doha-Umm Said', 'Qatar', -72.2, 379), ('Ras Laffan', 'Qatar', -65.9, 379), ('Coronel', 'Chile', -55.6, 374), ('Abbot Point', 'Australia', -16.7, 379), ('Mejillones', 'Chile', -28.2, 374), ('Puerto Caldera', 'Chile', -33.3, 374)]
- `04:35:12`   n_days: [44, 64, 84, 84, 84, 84] … [379, 379, 379, 379]
## census-us / repo-monitor / import-sentinel: invoke, settle 20 min, count errors after deploy

- `04:55:16` justhodl-census-us: since 2026-09-02T02:53+00:00 invocations=317 errors=0 samples=[]
- `04:55:16` justhodl-repo-monitor: since 2026-09-02T02:53+00:00 invocations=8 errors=0 samples=["HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 40
- `04:55:17` justhodl-import-sentinel: since 2026-09-02T02:53+00:00 invocations=15 errors=0 samples=[]
## verdict

- `04:55:17` ✗ justhodl-repo-monitor: 0 errors after deploy

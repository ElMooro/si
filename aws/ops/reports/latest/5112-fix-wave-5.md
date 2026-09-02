# ops 5112 -- fix wave 5: the tail

**Status:** failure  
**Duration:** 242.0s  
**Finished:** 2026-09-02T03:24:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| dead | distinct_errors | distinct_errors_since_fix | engine | errors | ports | requests | with_yoy |
|---|---|---|---|---|---|---|---|
|  |  |  | portwatch |  | 52 | 9 | 0 |
|  |  |  | manufacturing-global-agent | 0 |  |  |  |
|  |  |  | justhodl-repo-monitor | 4 |  |  |  |
| EFFR(HTTP 429),M2SL(HTTP 429),OBFR(HTTP 429),SOFRVOLUME(HTTP 400) |  |  | repo-monitor-probe |  |  |  |  |
|  |  | 0 | boj-full |  |  |  |  |
|  | 1 |  | justhodl-import-sentinel |  |  |  |  |
|  | 1 |  | justhodl-census-us |  |  |  |  |
|  | 0 |  | justhodl-insider-trades |  |  |  |  |

## Log
## 1. portwatch v1.6.4

- `03:20:50` ✅ justhodl-portwatch deployed (2026-09-02T03:20:40.000+0000) after 20s
- `03:21:04` portwatch invoke 200 b'{"ok": true, "chokepoints": 28, "worst": {"name": "Kerch Strait", "z": -1.48, "vs_baseline_pct": -98.9, "status": "DISRUPTED"}, "rows": 10948}'
- `03:21:04` portwatch v1.6.4: ports=52 with_yoy=0 requests={"n": 9, "throttled_429": 0, "budget": 140} history_through={"choke": "2026-08-23", "ports": "2026-08-28"} errors=[]
- `03:21:04`   sample: []
- `03:21:04`   n_days distribution: [64, 84, 84, 84, 84, 84, 84, 133, 166, 217, 217, 226] … [246, 246, 246, 246, 246]
## 2. manufacturing-global-agent / repo-monitor

- `03:21:25` ✅ manufacturing-global-agent deployed (2026-09-02T03:21:21.000+0000) after 20s
- `03:22:25` manufacturing-global-agent: distinct error lines after deploy: 0 []
- `03:22:25` ✅ justhodl-repo-monitor deployed (2026-09-02T03:21:04.000+0000) after 0s
- `03:23:56` justhodl-repo-monitor: distinct error lines after deploy: 4 ["HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=SOFRVOLUME&]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=WALCL&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=TREAST&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=DRTSCLCC&]:HTTP Error 429: Too Many Requests"]
## 3. repo-monitor ids probe (paced)

- `03:23:56`   EFFR: HTTP 429
- `03:23:58`   M2SL: HTTP 429
- `03:23:59`   OBFR: HTTP 429
- `03:24:02`   OTHL1690: 200 [('2026-08-26', '2568')]
- `03:24:03`   RRPONTSYD: 200 [('2026-09-01', '0.725')]
- `03:24:05`   SOFR: 200 [('2026-08-31', '3.68')]
- `03:24:07`   SOFRVOLUME: HTTP 400
- `03:24:08`   SWPT: 200 [('2026-08-26', '121')]
- `03:24:10`   WLCFLPCL: 200 [('2026-08-26', '4890')]
- `03:24:12`   AMERIBOR: 200 [('2026-08-31', '3.68001')]
- `03:24:14`   SOFR25: 200 [('2026-08-31', '3.67')]
- `03:24:15`   SOFR75: 200 [('2026-08-31', '3.74')]
- `03:24:17`   WDTGAL: 200 [('2026-08-26', '959435')]
- `03:24:19`   RIFSPPFAAD90NB: 200 [('2026-08-28', '3.83')]
- `03:24:21`   RIFSPPNAAD90NB: 200 [('2026-08-27', '3.7')]
- `03:24:22`   DCPN3M: 200 [('2026-08-27', '3.7')]
- `03:24:24`   T10Y2Y: 200 [('2026-09-01', '0.4')]
- `03:24:26`   T10Y3M: 200 [('2026-09-01', '0.87')]
- `03:24:27` repo-monitor dead/stale ids: [('EFFR', 'HTTP 429'), ('M2SL', 'HTTP 429'), ('OBFR', 'HTTP 429'), ('SOFRVOLUME', 'HTTP 400')]
## 4. boj-full after the detach + NameError deploy

- `03:24:27` warm rule targets now: [('1', 'benzinga-news-agent'), ('econdispatch', 'justhodl-census-us'), ('gdelt-full', 'justhodl-gdelt-full'), ('repo', 'justhodl-repo')]
- `03:24:28` boj invocations/errors per 10 min (last 3h): 00:20=134/132 00:30=134/132 00:40=134/132 00:50=134/132 01:00=134/132 01:10=134/132 01:20=134/132 01:30=134/132 01:40=134/132 01:50=134/132 02:00=134/132 02:10=67/66 02:20=1/0 02:30=1/0 02:40=23/0 03:10=20/0
- `03:24:29` boj distinct errors since 02:32: 0 []
## 5. import-sentinel / census-us / insider-trades error samples (90 min)

- `03:24:29` justhodl-import-sentinel: 1 distinct: ["[ERROR] NameError: name 'now' is not defined\nTraceback (most recent call last):\n\u00a0\u00a0File \"/var/task/lambda_function.py\", line 164, in lambda_handler\n\u00a0\u00a0\u00a0\u00a0_cut = (now - timedelta(days=14)).isoformat()"]
- `03:24:31` justhodl-census-us: 1 distinct: ["[ERROR] UnboundLocalError: cannot access local variable 'gr' where it is not associated with a value\nTraceback (most recent call last):\n\u00a0\u00a0File \"/var/task/lambda_function.py\", line 953, in lambda_handler\n\u00a0\u00a0\u00a0\u00a0refresh(state, ctx)\n\u00a0\u00a0File \"/var/task/lambda_function"]
- `03:24:32` justhodl-insider-trades: 0 distinct: []
- `03:24:32` import-health.json: present generated_at=2026-09-02T03:15:04+00:00 keys=['engine', 'version', 'generated_at', 'duration_s', 'overall', 'worst', 'pipelines', 'velocity', 'actions_this_sweep', 'incidents']
## verdict

- `03:24:32` ✗ portwatch: still no ports with yoy

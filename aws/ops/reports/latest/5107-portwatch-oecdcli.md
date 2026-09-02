# ops 5107 -- portwatch v1.6 + oecd-cli v2.0.0 + global-recession OECD leg

**Status:** failure  
**Duration:** 22.8s  
**Finished:** 2026-09-02T01:26:33+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_months | aggregates | as_of | confirmed | countries | engine | errors | oecd_usable | ports | prob | requests | throttled | version | with_yoy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | portwatch | 0 |  | 42 |  | 7 | 0 | 1.6.0 | 0 |
| 3 | 4 | 2026-06-01 |  | 18 | oecd-cli |  |  |  |  |  |  |  |  |
|  |  |  | 7 |  | global-recession |  | True |  | 30.5 |  |  |  |  |

## Log
## 1. portwatch v1.6

- `01:26:11` ✅ justhodl-portwatch deployed (v1.6) after 0s
- `01:26:15` sync invoke status 200 payload b'{"ok": true, "chokepoints": 28, "worst": {"name": "Makassar Strait", "z": -0.71, "vs_baseline_pct": -10.2, "status": "NORMAL"}, "rows": 1000}'
- `01:26:15` portwatch: version=1.6.0 ok=True chokepoints=28 ports=42 with_yoy=0 requests={"n": 7, "throttled_429": 0, "budget": 60} history_through={"choke": "2025-09-02", "ports": "2025-08-21"} daily_rows=1000 errors=[]
- `01:26:16` ⚠ log tail justhodl-portwatch: An error occurred (InvalidParameterException) when calling the FilterLogEvents o
- `01:26:16` portwatch schedules now: ['justhodl-portwatch-daily', 'portwatch-sched']
- `01:26:16` ✅ justhodl-portwatch-daily -> cron(20 11 * * ? *)
- `01:26:16` ✅ deleted duplicate schedule portwatch-sched (11:20)
## 2. oecd-cli v2.0.0

- `01:26:16` ✅ justhodl-oecd-cli deployed (v2.0.0) after 0s
- `01:26:18` invoke status 200 payload b'{"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\\"ok\\": true, \\"period\\": \\"2026-06\\", \\"global_avg\\": 100.83, \\"us_cli\\": 100.8, \\"oecd_cli\\": null, \\"n_countries\\": 18}"}'
- `01:26:18` oecd-cli: version=2.0.0 as_of=2026-06-01 age=3mo countries=18 aggregates=4 avg=100.83 oecd_total=None source=OECD DSD_STES@DF_CLI via cycle-features cache (2026-09-02)
- `01:26:18`   interpretation: US CLI at 100.8 signals expansion phase.
- `01:26:18`   USA: cli=100.8 prior=100.76 phase=expansion composite=102.51/EXPANSION
- `01:26:18`   CHN: cli=98.57 prior=98.6 phase=slowdown composite=93.38/RECESSION
- `01:26:18`   DEU: cli=100.72 prior=100.76 phase=expansion composite=98.99/RECOVERY
- `01:26:18`   JPN: cli=100.3 prior=100.24 phase=neutral composite=102.35/AT_RISK
- `01:26:18`   OECD: cli=None prior=None phase=None composite=None/None
- `01:26:18` ⚠ log tail justhodl-oecd-cli: An error occurred (InvalidParameterException) when calling the FilterLogEvents o
## 3. global-recession OECD leg

- `01:26:33` global-recession: prob=30.5 band=WATCH — pockets of stress oecd_period=2026-06-01 oecd_usable=True age=3 counts={"CONFIRMED": 7, "DIVERGENT": 10, "UNCONFIRMED": 17} ports_countries=0
## verdict

- `01:26:33` ✗ portwatch: no ports with yoy after v1.6 run

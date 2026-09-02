# ops 5114 -- closing wave

**Status:** success  
**Duration:** 64.0s  
**Finished:** 2026-09-02T11:48:32+00:00  

## Data

| confirmed | countries_with_ports | distinct | divergent | done | engine | err24h | err6h | err_pct_6h | err_pct_7d_baseline | errors | failures | http_err_lines | inv24h | inv6h | invocations | scope | truncated |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | repo-monitor |  |  |  |  | 0 |  | 5 |  |  | 18 |  |  |
|  |  |  |  | 1548 | sdmx-walker |  |  |  |  |  | 444 |  |  |  |  |  | 273 |
|  |  | 0 |  |  | justhodl-equity-research | 0 |  |  |  |  |  |  | 193 |  |  |  |  |
|  |  | 0 |  |  | justhodl-cds-proxy | 1 |  |  |  |  |  |  | 4 |  |  |  |  |
|  |  | 0 |  |  | justhodl-ecb-deep | 2 |  |  |  |  |  |  | 146 |  |  |  |  |
|  |  | 0 |  |  | justhodl-fortress | 0 |  |  |  |  |  |  | 7 |  |  |  |  |
|  |  | 0 |  |  | justhodl-outcome-checker | 0 |  |  |  |  |  |  | 2 |  |  |  |  |
|  |  | 0 |  |  | fedliquidityapi | 2 |  |  |  |  |  |  | 33 |  |  |  |  |
|  |  | 0 |  |  | justhodl-a2a-bus | 0 |  |  |  |  |  |  | 938 |  |  |  |  |
|  |  | 0 |  |  | justhodl-cb-injection | 1 |  |  |  |  |  |  | 2 |  |  |  |  |
|  |  | 0 |  |  | justhodl-ecb-derived | 0 |  |  |  |  |  |  | 1 |  |  |  |  |
|  |  | 0 |  |  | justhodl-provider-catalog | 0 |  |  |  |  |  |  | 24 |  |  |  |  |
|  |  | 0 |  |  | justhodl-risk-gate | 0 |  |  |  |  |  |  | 25 |  |  |  |  |
|  |  | 0 |  |  | justhodl-real-economy-collector | 0 |  |  |  |  |  |  | 1 |  |  |  |  |
|  |  | 1 |  |  | cftc-futures-positioning-agent | 1 |  |  |  |  |  |  | 17 |  |  |  |  |
| 2 | 4 |  | 1 |  | gbc |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 13 | 0.36 | 3.27 |  |  |  |  | 3601 |  | fleet |  |

## Log
## A. repo-monitor strictly after its 03:28 deploy

- `11:47:29` invocations=18 errors=0 HTTP_ERR lines: ["HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=SOFR25&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=AMERIBOR&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=RRPONTSYD&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=SOFR&]:HTTP Error 429: Too Many Requests", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_id=BAMLC0A4CBBB&]:HTTP Error 429: Too Many Requests"]
## B. OECD walker retry progress

- `11:47:29` ledgers now: failures=444 (was 479) truncated=273 (was 264) done=1548 (was 1548) state_updated=2026-09-02 11:46:40+00:00
- `11:47:30` walker invocations/errors last 9h: 774/0
- `11:47:30` ✅ retries are shrinking the ledgers; schedules kept
## C. small-error tail (7d samples)

- `11:47:31` justhodl-equity-research: 24h 193/0 · []
- `11:47:31` justhodl-cds-proxy: 24h 4/1 · []
- `11:47:32` justhodl-ecb-deep: 24h 146/2 · []
- `11:47:33` justhodl-fortress: 24h 7/0 · []
- `11:47:34` justhodl-outcome-checker: 24h 2/0 · []
- `11:47:34` fedliquidityapi: 24h 33/2 · []
- `11:47:35` justhodl-a2a-bus: 24h 938/0 · []
- `11:47:36` justhodl-cb-injection: 24h 2/1 · []
- `11:47:37` justhodl-ecb-derived: 24h 1/0 · []
- `11:47:38` justhodl-provider-catalog: 24h 24/0 · []
- `11:47:39` justhodl-risk-gate: 24h 25/0 · []
- `11:47:40` justhodl-real-economy-collector: 24h 1/0 · []
- `11:47:41` cftc-futures-positioning-agent: 24h 17/1 · ["[ERROR] Fetch https://api.polygon.io/v2/aggs/ticker/I:SPX/prev?adjusted=true&apiKey=zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d: HTTP Error 403: Forbidden"]
## D. global cycle physical layer

- `11:47:41` portwatch: generated_at=2026-09-02T11:20:51.382111+00:00 ports=58 with_yoy=24
- `11:48:12` GBC v3.0.3: physical counts={"CONFIRMED": 2, "DIVERGENT": 1, "UNCONFIRMED": 31} countries_with_ports=4 carried=None multi_pillar=34 pillars={"survey": 32, "activity": 32, "trade": 34, "financial": 34, "equity": 34} global=GLOBAL_EXPANSION 100.14 p6m=0.34
- `11:48:12`   CONFIRMED: [('AUS', 5.0), ('FIN', 7.7)] · DIVERGENT: ['CHL']
## E. fleet-wide errors: last 6h vs 7d baseline

- `11:48:32` 7d baseline (ops 5098): 1374186 invocations / 44882 errors = 3.27% ; last 6h: 3601 / 13 = 0.36% (6h scaled to 7d: 364 errors vs 44882)
- `11:48:32` top error producers (6h): justhodl-market-tape 6/78; justhodl-research-backtest 2/2; justhodl-ecb-deep 1/37; justhodl-engine-signal-map 1/43; cftc-futures-positioning-agent 1/2; justhodl-khalid-metrics 1/2; manufacturing-global-agent 1/1
- `11:48:32` fan-out members (6h): 34/63 invoked, errored: ['justhodl-khalid-metrics']
## verdict

- `11:48:32` ✅ VERDICT: GREEN

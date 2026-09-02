# ops 5111 -- fix wave 4 + before/after re-measure

**Status:** failure  
**Duration:** 1132.1s  
**Finished:** 2026-09-02T03:11:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| docs | duration_ms | engine | err2h | err7d | errors | inv2h | inv7d | newest_age_h | ports | reported | timed_out | with_yoy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 1603.88 | justhodl-import-sentinel |  |  | 0 |  |  |  |  | True | False |  |
|  | 40835.25 | justhodl-fleet-monitor |  |  | 0 |  |  |  |  | True | False |  |
|  | 706.9 | justhodl-feed-registry |  |  | 0 |  |  |  |  | True | False |  |
|  | 31687.85 | justhodl-signal-harvester |  |  | 0 |  |  |  |  | True | False |  |
|  | 35924.84 | justhodl-repo-monitor |  |  | 5 |  |  |  |  | True | False |  |
|  | 7549.76 | manufacturing-global-agent |  |  | 8 |  |  |  |  | True | False |  |
|  |  | portwatch |  |  |  |  |  |  | 52 |  |  | 0 |
| 2 |  | census-us-state |  |  |  |  |  | -0.2 |  |  |  |  |
|  |  | justhodl-provider-catalog | 0.0 | 3.0 |  | 2.0 | 184.0 |  |  |  |  |  |
|  |  | justhodl-cds-proxy | 0 | 6.0 |  | 0 | 20.0 |  |  |  |  |  |
|  |  | justhodl-ici-flows | 0 | 6.0 |  | 0 | 6.0 |  |  |  |  |  |
|  |  | justhodl-equity-research | 0.0 | 7.0 |  | 21.0 | 1311.0 |  |  |  |  |  |
|  |  | justhodl-gdelt-full | 0.0 | 108.0 |  | 25.0 | 737.0 |  |  |  |  |  |
|  |  | justhodl-fi-census | 0.0 | 0 |  | 1.0 | 0 |  |  |  |  |  |
|  |  | justhodl-boj-full | 1100.0 | 37422.0 |  | 1141.0 | 39474.0 |  |  |  |  |  |
|  |  | justhodl-ecb-deep | 0.0 | 6.0 |  | 12.0 | 1014.0 |  |  |  |  |  |
|  |  | justhodl-provider-window-sentinel | 0 | 1.0 |  | 0 | 2.0 |  |  |  |  |  |
|  |  | justhodl-import-sentinel | 36.0 | 1059.0 |  | 37.0 | 1729.0 |  |  |  |  |  |
|  |  | justhodl-stock-screener | 0.0 | 149.0 |  | 1.0 | 149.0 |  |  |  |  |  |
|  |  | justhodl-signal-scorecard | 0 | 42.0 |  | 0 | 42.0 |  |  |  |  |  |
|  |  | justhodl-imf-full | 1.0 | 24.0 |  | 2.0 | 24.0 |  |  |  |  |  |
|  |  | justhodl-calibrator | 0 | 42.0 |  | 0 | 42.0 |  |  |  |  |  |
|  |  | justhodl-risk-gate | 0.0 | 3.0 |  | 2.0 | 177.0 |  |  |  |  |  |
|  |  | justhodl-signal-harvester | 0 | 21.0 |  | 0 | 21.0 |  |  |  |  |  |
|  |  | justhodl-series-extractor | 0.0 | 3073.0 |  | 8.0 | 8440.0 |  |  |  |  |  |
|  |  | justhodl-a2a-bus | 0.0 | 2.0 |  | 71.0 | 6566.0 |  |  |  |  |  |
|  |  | justhodl-plumbing-aggregator | 0.0 | 4.0 |  | 3.0 | 172.0 |  |  |  |  |  |
|  |  | justhodl-cb-injection | 0 | 2.0 |  | 0 | 9.0 |  |  |  |  |  |
|  |  | justhodl-fleet-monitor | 0.0 | 189.0 |  | 1.0 | 189.0 |  |  |  |  |  |
|  |  | justhodl-repo-monitor | 0.0 | 22.0 |  | 4.0 | 357.0 |  |  |  |  |  |
|  |  | justhodl-insider-trades | 1.0 | 667.0 |  | 6.0 | 882.0 |  |  |  |  |  |
|  |  | cftc-futures-positioning-agent | 0.0 | 1.0 |  | 1.0 | 117.0 |  |  |  |  |  |
|  |  | justhodl-real-economy-collector | 0.0 | 1.0 |  | 1.0 | 10.0 |  |  |  |  |  |
|  |  | justhodl-census-us | 18.0 | 1845.0 |  | 335.0 | 7333.0 |  |  |  |  |  |
|  |  | justhodl-outcome-checker | 0 | 3.0 |  | 0 | 17.0 |  |  |  |  |  |
|  |  | justhodl-feed-registry | 0 | 21.0 |  | 0 | 21.0 |  |  |  |  |  |
|  |  | justhodl-research-backtest | 0 | 21.0 |  | 0 | 21.0 |  |  |  |  |  |
|  |  | justhodl-fortress | 0 | 6.0 |  | 0 | 26.0 |  |  |  |  |  |
|  |  | fedliquidityapi | 2.0 | 19.0 |  | 4.0 | 226.0 |  |  |  |  |  |
|  |  | manufacturing-global-agent | 1.0 | 25.0 |  | 1.0 | 51.0 |  |  |  |  |  |
|  |  | justhodl-etf-census | 0.0 | 0 |  | 1.0 | 0 |  |  |  |  |  |
|  |  | justhodl-ecb-derived | 0 | 3.0 |  | 0 | 9.0 |  |  |  |  |  |
|  |  | justhodl-global-liquidity | 0 | 2.0 |  | 0 | 7.0 |  |  |  |  |  |
|  |  | justhodl-market-tape | 0.0 | 77.0 |  | 24.0 | 2089.0 |  |  |  |  |  |

## Log
## 1. deploy-wait + run + verify

- `02:53:27` ✅ justhodl-import-sentinel deployed (2026-09-02T02:53:23.000+0000) after 61s
- `02:55:28` justhodl-import-sentinel: errors=0 [] duration_ms=1603.88 timeout=False
- `02:55:28` import-sentinel feed: MISSING generated_at=None chips=0
- `02:55:28` ✅ justhodl-fleet-monitor deployed (2026-09-02T02:52:59.000+0000) after 0s
- `02:59:29` justhodl-fleet-monitor: errors=0 [] duration_ms=40835.25 timeout=False
- `02:59:29` ✅ justhodl-feed-registry deployed (2026-09-02T02:52:34.000+0000) after 0s
- `03:03:30` justhodl-feed-registry: errors=0 [] duration_ms=706.9 timeout=False
- `03:03:30` ✅ justhodl-signal-harvester deployed (2026-09-02T02:54:32.000+0000) after 0s
- `03:08:31` justhodl-signal-harvester: errors=0 [] duration_ms=31687.85 timeout=False
- `03:08:31` ✅ justhodl-repo-monitor deployed (2026-09-02T02:54:10.000+0000) after 0s
- `03:10:02` justhodl-repo-monitor: errors=5 ["HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 400: Bad Request", "HTTP_ERR[https://api.stlouisfed.org/fred/series/observations?series_i]:HTTP Error 429: Too Man duration_ms=35924.84 timeout=False
- `03:10:02` ✅ manufacturing-global-agent deployed (2026-09-02T02:54:57.000+0000) after 0s
- `03:11:03` manufacturing-global-agent: errors=8 ["Error fetching ISM_COMPOSITE: HTTP Error 400: Bad Request", "Error fetching ISM_NEW_ORDERS: HTTP Error 400: Bad Request", "Error fetching ISM_PRODUCTION: HTTP Error 400: Bad Request", "Error fetching ISM_EMPLOYMENT: HTTP Error 400: Bad Request", "Error fetching ISM_SUPPLIER_DELIVERIES: HTTP Error  duration_ms=7549.76 timeout=False
- `03:11:03` ✅ justhodl-portwatch deployed (2026-09-02T02:53:48.000+0000) after 0s
- `03:11:17` portwatch invoke 200 b'{"ok": true, "chokepoints": 28, "worst": {"name": "Kerch Strait", "z": -1.48, "vs_baseline_pct": -98.9, "status": "DISRUPTED"}, "rows": 10948}'
- `03:11:17` portwatch v1.6.3: ports=52 with_yoy=0 requests={"n": 9, "throttled_429": 0, "budget": 140} history_through={"choke": "2026-08-23", "ports": "2026-08-28"} errors=[]
- `03:11:17`   sample: []
## 2. imf-full 6h schedule off

- `03:11:17` ✅ justhodl-imf-full-6h -> DISABLED (weekly schedule kept)
## 3. ici-flows rule off

- `03:11:18` ✅ justhodl-ici-flows-weekly -> DISABLED: ici.org answers 403 to the fetcher (sitemap + /research), no consumer reads data/ici-flows*.json; rebuild on OFR's MMF monitor if the desk wants MMF flows back
## 4. census-us state freshness

- `03:11:18` census-us _state docs: 2 newest age -0.2h
## 5. re-measure: last 2h vs 7d baseline

- `03:11:18` FIX_ERRORS list -- 7d baseline (inv/err) -> last 2h (inv/err):
- `03:11:18`   justhodl-provider-catalog                184/3 -> 2/0
- `03:11:18`   justhodl-cds-proxy                       20/6 -> 0/0
- `03:11:18`   justhodl-ici-flows                       6/6 -> 0/0
- `03:11:18`   justhodl-equity-research                 1311/7 -> 21/0
- `03:11:18`   justhodl-gdelt-full                      737/108 -> 25/0
- `03:11:18`   justhodl-fi-census                       0/0 -> 1/0
- `03:11:18`   justhodl-boj-full                        39474/37422 -> 1141/1100
- `03:11:18`   justhodl-ecb-deep                        1014/6 -> 12/0
- `03:11:18`   justhodl-provider-window-sentinel        2/1 -> 0/0
- `03:11:18`   justhodl-import-sentinel                 1729/1059 -> 37/36
- `03:11:18`   justhodl-stock-screener                  149/149 -> 1/0
- `03:11:18`   justhodl-signal-scorecard                42/42 -> 0/0
- `03:11:18`   justhodl-imf-full                        24/24 -> 2/1
- `03:11:18`   justhodl-calibrator                      42/42 -> 0/0
- `03:11:18`   justhodl-risk-gate                       177/3 -> 2/0
- `03:11:18`   justhodl-signal-harvester                21/21 -> 0/0
- `03:11:18`   justhodl-series-extractor                8440/3073 -> 8/0
- `03:11:18`   justhodl-a2a-bus                         6566/2 -> 71/0
- `03:11:18`   justhodl-plumbing-aggregator             172/4 -> 3/0
- `03:11:18`   justhodl-cb-injection                    9/2 -> 0/0
- `03:11:18`   justhodl-fleet-monitor                   189/189 -> 1/0
- `03:11:18`   justhodl-repo-monitor                    357/22 -> 4/0
- `03:11:18`   justhodl-insider-trades                  882/667 -> 6/1
- `03:11:18`   cftc-futures-positioning-agent           117/1 -> 1/0
- `03:11:18`   justhodl-real-economy-collector          10/1 -> 1/0
- `03:11:18`   justhodl-census-us                       7333/1845 -> 335/18
- `03:11:18`   justhodl-outcome-checker                 17/3 -> 0/0
- `03:11:18`   justhodl-feed-registry                   21/21 -> 0/0
- `03:11:18`   justhodl-research-backtest               21/21 -> 0/0
- `03:11:18`   justhodl-fortress                        26/6 -> 0/0
- `03:11:18`   fedliquidityapi                          226/19 -> 4/2
- `03:11:18`   manufacturing-global-agent               51/25 -> 1/1
- `03:11:18`   justhodl-etf-census                      0/0 -> 1/0
- `03:11:18`   justhodl-ecb-derived                     9/3 -> 0/0
- `03:11:18`   justhodl-global-liquidity                7/2 -> 0/0
- `03:11:18`   justhodl-market-tape                     2089/77 -> 24/0
- `03:11:18` still erroring in the last 2h: [('justhodl-boj-full', 1141.0, 1100.0), ('justhodl-import-sentinel', 37.0, 36.0), ('justhodl-imf-full', 2.0, 1.0), ('justhodl-insider-trades', 6.0, 1.0), ('justhodl-census-us', 335.0, 18.0), ('fedliquidityapi', 4.0, 2.0), ('manufacturing-global-agent', 1.0, 1.0)]
- `03:11:18` fan-out members: 62/63 invoked in the last 2h; errored: []
## verdict

- `03:11:18` ✗ import-sentinel still failing / feed missing
- `03:11:18` ✗ repo-monitor still logs HTTP_ERR/SRF_ERR
- `03:11:18` ✗ manufacturing-global-agent still logs fetch errors
- `03:11:18` ✗ portwatch: still no ports with yoy

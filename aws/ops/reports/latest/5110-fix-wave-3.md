# ops 5110 -- fix wave 3

**Status:** failure  
**Duration:** 848.6s  
**Finished:** 2026-09-02T02:45:02+00:00  

## Error

```
SystemExit: 1
```

## Data

| acao_before | auth | errors | function_error | http_fail | owner | ports | status_before | step | with_yoy |
|---|---|---|---|---|---|---|---|---|---|
|  |  | 0 | None |  |  |  |  | boj-child |  |
|  |  |  |  |  |  | 51 |  | portwatch | 0 |
|  |  |  |  | 0 |  |  |  | plumbing |  |
|  |  | 0 |  |  |  |  |  | stock-screener |  |
| * | NONE |  |  |  | justhodl-wl-series-api |  | 200 | beacon-url |  |

## Log
## 1. paced FRED / NY Fed re-probe

- `02:30:54`   SOFR25: 200 ('2026-08-31', '3.67')
- `02:30:55`   SOFR75: 200 ('2026-08-31', '3.74')
- `02:30:57`   SOFR1: 200 ('2026-08-31', '3.6')
- `02:31:00`   SOFR99: 200 ('2026-08-31', '3.77')
- `02:31:02`   USD3MTD156N: 400 None
- `02:31:04`   TSFR3M: 400 None
- `02:31:07`   WDTGAL: 200 ('2026-08-26', '959435')
- `02:31:08`   WTREGEN: 200 ('2026-08-26', '950736')
- `02:31:10`   SWPT: 200 ('2026-08-26', '121')
- `02:31:12`   WLCFLPCL: 200 ('2026-08-26', '4890')
- `02:31:14`   SUBLPDRCSM: 200 ('2026-07-01', '-5.7')
- `02:31:16`   DALLASFEDFAB: 400 None
- `02:31:17`   BACTSAMFRBDAL: 200 ('2026-08-01', '11.6')
- `02:31:19`   KCLFEDFAB: 400 None
- `02:31:21`   KCFMCI: 400 None
- `02:31:23`   CHEFMNM156N: 400 None
- `02:31:24`   GAFDIMSA: 400 None
- `02:31:26`   GACDISA066MSFRBNY: 200 ('2026-08-01', '20.6')
- `02:31:28`   GAPHDFBA: 400 None
- `02:31:30`   GACDFSA066MSFRBPHI: 200 ('2026-08-01', '47.40000')
- `02:31:32`   RMTSPL: 400 None
- `02:31:33`   NAPMEI: 400 None
- `02:31:35`   NAPMII: 400 None
- `02:31:38`   EA19PRMNTO01IXOBM: 200 ('2023-10-01', '111.9')
- `02:31:39`   EA20PRMNTO01IXOBM: 400 None
- `02:31:41`   JPNPRMNTO01IXOBM: 200 ('2024-03-01', '99.26672')
- `02:31:43`   GBRPRMNTO01IXOBM: 200 ('2024-03-01', '112.5295')
- `02:31:45`   DEUPRMNTO01IXOBM: 200 ('2024-03-01', '95.46779')
- `02:31:47`   https://markets.newyorkfed.org/api/rp/repo/fixed/results/latest.json: 200 { "repo": { "operations": [ ] } }
- `02:31:47`   https://markets.newyorkfed.org/api/rp/repo/fixed/results/last/1.json: 200 { "repo": { "operations": [ ] } }
- `02:31:48`   https://markets.newyorkfed.org/api/rp/srf/results/last/1.json: HTTP 400
## 2. boj-full / portwatch / plumbing-aggregator

- `02:31:48` ✅ justhodl-boj-full deployed (2026-09-02T02:30:57.000+0000) after 0s
- `02:31:49` boj dbs: 22 ['BP01', 'BS01', 'BS02', 'FF', 'FM01', 'FM02', 'FM03', 'FM08']
- `02:33:34` boj child (BP01): status=200 FunctionError=None payload=b'{"ok": true, "mode": "api", "res": {"BP01": [12240, 17989, 655]}, "elapsed_s": 104.2}'
- `02:33:34`   NameError/Traceback lines: 0 []
- `02:33:34` ✅ justhodl-portwatch deployed (2026-09-02T02:31:36.000+0000) after 0s
- `02:40:30` portwatch invoke 200 b'{"ok": true, "chokepoints": 28, "worst": {"name": "Kerch Strait", "z": -1.48, "vs_baseline_pct": -98.9, "status": "DISRUPTED"}, "rows": 10948}'
- `02:40:31` portwatch v1.6.2: ports=51 with_yoy=0 requests={"n": 9, "throttled_429": 0, "budget": 140} history_through={"choke": "2026-08-23", "ports": "2026-08-28"} errors=[]
- `02:40:31` ✅ justhodl-plumbing-aggregator deployed (2026-09-02T02:31:13.000+0000) after 0s
- `02:41:47` plumbing-aggregator HTTP fail lines after fix: 0 []
## 3. stock-screener redeploy with its env

- `02:41:47` env keys: []
- `02:41:47`   zip: 122659 bytes
## 1. Lambda

- `02:41:47`   Lambda exists — updating
- `02:41:50` ✅   ✓ updated justhodl-stock-screener
- `02:43:55` stock-screener errors after redeploy: 0 []; reports: []
## 4. timeout forensics v2

- `02:43:55`   justhodl-fleet-monitor: REPORT RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 51
- `02:43:55`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:43:55`       START RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7 Version: $LATEST
- `02:43:55`       END RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7
- `02:43:55`       REPORT RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7	Duration: 300000.00 ms	Billed Duration: 300524 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 523.99 ms	Status
- `02:43:55`       START RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7 Version: $LATEST
- `02:43:55`       END RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7
- `02:43:55`       REPORT RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Status: timeout
XRAY TraceId: 1
- `02:43:55`       START RequestId: 436a9395-6742-4adb-8b87-76f6a0ebfca7 Version: $LATEST
- `02:43:56`   justhodl-feed-registry: REPORT RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 25
- `02:43:56`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:43:56`       START RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e Version: $LATEST
- `02:43:56`       END RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e
- `02:43:56`       REPORT RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e	Duration: 120000.00 ms	Billed Duration: 120516 ms	Memory Size: 256 MB	Max Memory Used: 101 MB	Init Duration: 515.85 ms	Status
- `02:43:56`       START RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e Version: $LATEST
- `02:43:56`       END RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e
- `02:43:56`       REPORT RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 256 MB	Max Memory Used: 101 MB	Status: timeout
XRAY TraceId: 1
- `02:43:56`       START RequestId: 376a93db-f83e-4b6e-b74e-e6c74698ef3e Version: $LATEST
- `02:43:57`   justhodl-research-backtest: REPORT RequestId: 58cc8b74-adc3-4d9c-a488-0e2a7dc83b70	Duration: 240000.00 ms	Billed Duration: 240441 ms	Memory Size: 51
- `02:43:57`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:43:57`       START RequestId: 58cc8b74-adc3-4d9c-a488-0e2a7dc83b70 Version: $LATEST
- `02:43:57`       [backtest] starting at 2026-08-30T11:11:21.426673+00:00
- `02:43:57`       [backtest] universe: 565 unique tickers; earliest research: 2026-06-02T01:27:22.884388+00:00
- `02:43:57`       [backtest] fetched current prices for 565/565 tickers
- `02:43:57`       [backtest] SPY current: 769.35
- `02:43:57`       [backtest] SPY history: 62 days from 2026-06-02 to 2026-08-30
- `02:43:57`       [backtest] found 705 current research files
- `02:43:57`   justhodl-global-liquidity: REPORT RequestId: 5ef86671-2853-4d2d-8bf3-1af1b329e055	Duration: 120000.00 ms	Billed Duration: 120508 ms	Memory Size: 25
- `02:43:57`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:43:57`       START RequestId: 5ef86671-2853-4d2d-8bf3-1af1b329e055 Version: $LATEST
- `02:43:57`       [global-liquidity] starting 2026-08-31T14:00:26.032701+00:00
- `02:43:57`       [fred] DEXUSEU: 385 obs
- `02:43:57`       [fred] ECBASSETSW: 400 obs
- `02:43:57`       [fred] WTREGEN: 400 obs
- `02:44:00`   justhodl-provider-window-sentinel: REPORT RequestId: 296a93f1-107c-4a25-b29e-736191abef63	Duration: 120000.00 ms	Billed Duration: 120528 ms	Memory Size: 25
- `02:44:00`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:44:00`       START RequestId: 296a93f1-107c-4a25-b29e-736191abef63 Version: $LATEST
- `02:44:01`   justhodl-signal-harvester: REPORT RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 51
- `02:44:01`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:44:01`       START RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a Version: $LATEST
- `02:44:01`       END RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a
- `02:44:01`       REPORT RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a	Duration: 300000.00 ms	Billed Duration: 300485 ms	Memory Size: 512 MB	Max Memory Used: 106 MB	Init Duration: 484.33 ms	Status
- `02:44:01`       START RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a Version: $LATEST
- `02:44:01`       END RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a
- `02:44:01`       REPORT RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 512 MB	Max Memory Used: 105 MB	Status: timeout
XRAY TraceId: 1
- `02:44:01`       START RequestId: abbf2f48-2a05-47ce-b325-e66e99acf12a Version: $LATEST
- `02:44:02`   justhodl-imf-full: REPORT RequestId: 676a9395-6d71-4eb3-823f-1f9a869baa05	Duration: 850000.00 ms	Billed Duration: 850000 ms	Memory Size: 10
- `02:44:02`       INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffeca7bc
- `02:44:02`       START RequestId: 676a9395-6d71-4eb3-823f-1f9a869baa05 Version: $LATEST
- `02:44:02`       END RequestId: 676a9395-6d71-4eb3-823f-1f9a869baa05
- `02:44:02`       REPORT RequestId: 676a9395-6d71-4eb3-823f-1f9a869baa05	Duration: 850000.00 ms	Billed Duration: 850534 ms	Memory Size: 1024 MB	Max Memory Used: 147 MB	Init Duration: 533.72 ms	Statu
- `02:44:02`       START RequestId: 676a9395-6d71-4eb3-823f-1f9a869baa05 Version: $LATEST
- `02:44:03`   justhodl-calibrator: REPORT RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 51
- `02:44:03`       WARN: ka_aliases unavailable: No module named 'ka_aliases'
- `02:44:03`       START RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea Version: $LATEST
- `02:44:03`       END RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea
- `02:44:03`       REPORT RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea	Duration: 300000.00 ms	Billed Duration: 300564 ms	Memory Size: 512 MB	Max Memory Used: 511 MB	Init Duration: 563.27 ms	Status
- `02:44:03`       WARN: ka_aliases unavailable: No module named 'ka_aliases'
- `02:44:03`       START RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea Version: $LATEST
- `02:44:03`       END RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea
- `02:44:03`       REPORT RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea	Duration: 300000.00 ms	Billed Duration: 300000 ms	Memory Size: 512 MB	Max Memory Used: 511 MB	Status: timeout
XRAY TraceId: 1
- `02:44:03`       WARN: ka_aliases unavailable: No module named 'ka_aliases'
- `02:44:03`       START RequestId: fba7be35-fdc6-452b-83dc-b4b193c72cea Version: $LATEST
- `02:44:03`   justhodl-signal-scorecard: REPORT RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 25
- `02:44:03`       START RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39 Version: $LATEST
- `02:44:03`       [signal-scorecard] starting 2026-08-30T10:30:25.977212+00:00
- `02:44:03`       END RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39
- `02:44:03`       REPORT RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39	Duration: 120000.00 ms	Billed Duration: 120552 ms	Memory Size: 256 MB	Max Memory Used: 256 MB	Init Duration: 551.53 ms	Status
- `02:44:03`       START RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39 Version: $LATEST
- `02:44:03`       [signal-scorecard] starting 2026-08-30T10:33:28.646751+00:00
- `02:44:03`       END RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39
- `02:44:03`       REPORT RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 256 MB	Max Memory Used: 255 MB	Status: timeout
XRAY TraceId: 1
- `02:44:03`       START RequestId: 98d5d505-8020-4c72-9d15-0a7ea46f1e39 Version: $LATEST
- `02:44:03`       [signal-scorecard] starting 2026-08-30T10:37:35.887138+00:00
- `02:44:04`   justhodl-import-sentinel: no timeout REPORT in 3d
## 5. chart-pro / nav-drawer beacon URL

- `02:44:59` URL owner: justhodl-wl-series-api auth=NONE cors={"AllowCredentials": false, "AllowHeaders": ["content-type"], "AllowMethods": ["*"], "AllowOrigins": ["*"], "MaxAge": 86400}
- `02:45:00`   before: HTTP 200 ACAO=* body=b'{"ok": true}'
## 6. OECD walker retry schedules

- `02:45:00` oecd walker ledgers before: failures=479 truncated=264 done=1548
- `02:45:01` ✅ justhodl-sdmx-walker-oecd-retrunc created (rate(10 minutes))
- `02:45:01` ✅ justhodl-sdmx-walker-oecd-refail created (rate(15 minutes))
## 7. import-sentinel

- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02`   [ERROR] NameError: name 'now' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 164, in lambda_handler
    _cut = (now - timedelta(days=14)).isoformat()
- `02:45:02` import-sentinel feed: MISSING An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist
## verdict

- `02:45:02` ✗ portwatch: no ports with yoy

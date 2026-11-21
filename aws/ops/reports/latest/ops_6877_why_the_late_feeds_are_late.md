# ops 6877 - why the really-late feeds are late (read-only)

**Status:** success  
**Duration:** 26.2s  
**Finished:** 2026-11-21T05:18:52+00:00  

## Data

| engines | exceptions | fmp_blocked | kept_previous | no_reason | other_429 | out_of_memory | timed_out |
|---|---|---|---|---|---|---|---|
| 93 | 11 | 6 | 2 | 65 | 6 | 0 | 0 |

## Log
- `05:18:38` monitor report of 2026-11-20T07:59:11.605314+00:00; 137 engines own a feed whose data is past its limit
## Engines that ran in the last 3 days and own a late feed, by what their log says

- `05:18:52` FMP refused (429 / limit): 6 engines, 6 late feeds; 1 of the engines end runs in an error
- `05:18:52`     justhodl-watchlist-debate                runs   3 errors   0 | oldest feed   3082 h (07-15) | [FETCH] https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=SPY&apikey=[...] HTTP Error 429: Too Many Requests
- `05:18:52`     justhodl-russell-recon-frontrun          runs   6 errors   0 | oldest feed    499 h (10-31) | fmp_screener single ex=NYSE err=HTTP Error 429: Too Many Requests
- `05:18:52`     justhodl-cot-tracker                     runs   1 errors   0 | oldest feed    490 h (10-31) | [fmp] commitment-of-traders-report: HTTP 429
- `05:18:52`     justhodl-fmp-ratios                      runs   9 errors   9 | oldest feed    417 h (11-03) | [fmp-ratios] attempted 314 ok 0 errors 314 skipped(time) 0 key_status {"status": "http_429", "path": "ratios"} in 252s
- `05:18:52`     justhodl-vol-target-unwind               runs   6 errors   0 | oldest feed    403 h (11-04) | fmp_history SPY attempt=1 error: HTTP Error 429: Too Many Requests
- `05:18:52`     justhodl-sellside-views                  runs   2 errors   0 | oldest feed     42 h (11-19) | [fmp] grades-latest-news: HTTP Error 429: Too Many Requests
- `05:18:52` provider 429 (other): 6 engines, 9 late feeds; 3 of the engines end runs in an error
- `05:18:52`     justhodl-tv-bars                         runs 908 errors   0 | oldest feed   2324 h (08-16) | REPORT RequestId: [...]	Duration: 428.74 ms	Billed Duration: 429 ms	Memory Size: 1024 MB	Max Memory Used: 107 MB	
XRAY TraceId: 1-6aff5b13-003dc326467d5fe41814ad86	Segmen
- `05:18:52`     justhodl-positioning-analog              runs  18 errors  18 | oldest feed    609 h (10-26) | [ERROR] HTTPError: HTTP Error 429: Too Many Requests
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 145, in lambda_handler
    spx = spx_c
- `05:18:52`     justhodl-earnings-blackout               runs  18 errors  18 | oldest feed    500 h (10-31) | [ERROR] HTTPError: HTTP Error 429: Too Many Requests
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 66, in lambda_handler
    j = _get("ht
- `05:18:52`     justhodl-earnings-iv-crush               runs   3 errors   0 | oldest feed    418 h (11-03) | earnings calendar error: HTTP Error 429: Too Many Requests
- `05:18:52`     justhodl-gf-value                        runs   3 errors   0 | oldest feed    418 h (11-03) | REPORT RequestId: [...]	Duration: 184742.99 ms	Billed Duration: 185173 ms	Memory Size: 1536 MB	Max Memory Used: 986 MB	Init Duration: 429.31 ms	
XRAY TraceId: 1-6aff71e6-
- `05:18:52`     justhodl-stock-screener                  runs  13 errors  12 | oldest feed    142 h (11-15) | ERR esg-ratings: exhausted retries (HTTP Error 429: Too Many Requests)
- `05:18:52` provider 401 / 403: 3 engines, 3 late feeds; 0 of the engines end runs in an error
- `05:18:52`     justhodl-ml-predictions                  runs   3 errors   0 | oldest feed   5055 h (04-24) | ERROR:HTTP Error 403: Forbidden
- `05:18:52`     justhodl-audit-loop                      runs   3 errors   0 | oldest feed   2579 h (08-05) | telegram err: HTTP Error 401: Unauthorized
- `05:18:52`     justhodl-crypto-enricher                 runs   3 errors   0 | oldest feed     15 h (11-20) | FETCH_ERR[https://api.bybit.com/v5/market/funding/history?category=lin]: HTTP Error 403: Forbidden
- `05:18:52` kept the previous publication: 2 engines, 2 late feeds; 2 of the engines end runs in an error
- `05:18:52`     justhodl-hiring-velocity                 runs   9 errors   9 | oldest feed    619 h (10-26) | [ERROR] ValueError: No received employee/income population; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 4
- `05:18:52`     justhodl-volatility-squeeze-hunter       runs   9 errors   9 | oldest feed    500 h (10-31) | [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 555, in lambda_handle
- `05:18:52` exception: 11 engines, 14 late feeds; 9 of the engines end runs in an error
- `05:18:52`     justhodl-insider-aggregate               runs  12 errors   6 | oldest feed   1497 h (09-19) | [ERROR] RuntimeError: Native insider publication failed; inspect reviewed request evidence
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 
- `05:18:52`     justhodl-share-flows                     runs   9 errors   9 | oldest feed   1314 h (09-27) | [ERROR] ValueError: Underlying capital-structure or SEC acquisition is future-dated or older than 48 hours
Traceback (most recent call last):
  File "/var/task/lambda_fun
- `05:18:52`     justhodl-estimate-revisions              runs  18 errors  18 | oldest feed   1292 h (09-28) | [ERROR] ValueError: Original received calendar required
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 440, in lambda_handler
    calendar
- `05:18:52`     justhodl-catalyst-clusters               runs   3 errors   0 | oldest feed   1290 h (09-28) | [clusters] ERROR: No aggressive basket positions
- `05:18:52`     justhodl-research-backtest               runs   9 errors   9 | oldest feed    932 h (10-13) | [ERROR] RuntimeError: [...]
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 578, in lambda_handler
    list(pool.map(read_s3_json,historica
- `05:18:52`     justhodl-dark-pool                       runs  18 errors  18 | oldest feed    826 h (10-17) | [ERROR] ValueError: One or more planned sources failed; current head retained
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 46, in lambda
- `05:18:52`     justhodl-signal-genealogy                runs  18 errors  18 | oldest feed    546 h (10-29) | [ERROR] ValueError: complete_artifact_exceeds_budget
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 33, in lambda_handler
    result=run(c
- `05:18:52`     justhodl-forensic-screen                 runs  18 errors  18 | oldest feed    402 h (11-04) | [ERROR] ValueError: Ready accounting acquisition is future-dated or older than 48 hours
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 81,
- `05:18:52`     justhodl-sector-flow-state               runs   8 errors   4 | oldest feed    227 h (11-11) | [ERROR] RuntimeError: Native sector fusion failed; inspect reviewed request evidence
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 25, in
- `05:18:52`     justhodl-china-liquidity                 runs   3 errors   0 | oldest feed    185 h (11-13) | [china-research] failure context: {"contract": "china-publication-failure.v1", "exception_class": "EvidenceError", "last_attempt_sha256": "[...]", "reason": "Undeclared p
- `05:18:52`     justhodl-port-cargo                      runs   7 errors   6 | oldest feed     43 h (11-19) | [ERROR] CaptureError: Whole provider acquisition failed
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 536, in lambda_handler
    return c
- `05:18:52` no reason in the log: 65 engines, 76 late feeds; 12 of the engines end runs in an error
- `05:18:52`     justhodl-political-stocks                runs   3 errors   0 | oldest feed   3546 h (06-26) | 
- `05:18:52`     justhodl-ai-brief-router                 runs  18 errors   0 | oldest feed   2551 h (08-06) | 
- `05:18:52`     justhodl-a2a-bus                         runs  31 errors   0 | oldest feed   2545 h (08-07) | 
- `05:18:52`     justhodl-divergence-interpreter          runs   3 errors   0 | oldest feed   2535 h (08-07) | 
- `05:18:52`     justhodl-crypto-funding                  runs   6 errors   0 | oldest feed   1731 h (09-10) | 
- `05:18:52`     justhodl-credit-stress                   runs   3 errors   0 | oldest feed   1498 h (09-19) | REPORT RequestId: [...]	Duration: 51678.24 ms	Billed Duration: 52030 ms	Memory Size: 512 MB	Max Memory Used: 126 MB	Init Duration: 351.13 ms	
XRAY TraceId: 1-6aff5550-47b
- `05:18:52`     justhodl-khalid-adaptive                 runs   3 errors   0 | oldest feed   1497 h (09-19) | 
- `05:18:52`     justhodl-backfill-orchestrator           runs   3 errors   0 | oldest feed   1492 h (09-20) | 
- `05:18:52`     justhodl-settlement-fails                runs   3 errors   0 | oldest feed   1485 h (09-20) | 
- `05:18:52`     justhodl-nyfed-pd                        runs   2 errors   0 | oldest feed   1485 h (09-20) | [dealer-research] unavailable: HTTPError
- `05:18:52`     justhodl-capital-flow                    runs   3 errors   0 | oldest feed   1479 h (09-20) | 
- `05:18:52`     justhodl-risk-regime                     runs 291 errors   0 | oldest feed   1475 h (09-20) | REPORT RequestId: [...]	Duration: 8899.58 ms	Billed Duration: 8900 ms	Memory Size: 512 MB	Max Memory Used: 147 MB	
XRAY TraceId: 1-6aff429c-5e956c8c30ad62fe755253fc	Segme
- `05:18:52`     justhodl-vrp                             runs   3 errors   0 | oldest feed   1473 h (09-20) | 
- `05:18:52`     justhodl-market-extremes                 runs   3 errors   0 | oldest feed   1473 h (09-20) | 
- `05:18:52`     justhodl-stress-index                    runs   3 errors   0 | oldest feed   1466 h (09-21) | 
- `05:18:52`     justhodl-capitulation                    runs   3 errors   0 | oldest feed   1456 h (09-21) | REPORT RequestId: [...]	Duration: 42904.45 ms	Billed Duration: 43265 ms	Memory Size: 256 MB	Max Memory Used: 196 MB	Init Duration: 359.82 ms	
XRAY TraceId: 1-6b006cc4-717
- `05:18:52`     justhodl-retail-sentiment                runs   3 errors   0 | oldest feed   1453 h (09-21) | 
- `05:18:52`     justhodl-etf-global-desk                 runs   3 errors   0 | oldest feed   1450 h (09-21) | 
- `05:18:52`     justhodl-flow-lookthrough                runs   3 errors   0 | oldest feed   1449 h (09-21) | 
- `05:18:52`     justhodl-activist-filings-scanner        runs   3 errors   0 | oldest feed   1293 h (09-28) | 
- `05:18:52`     justhodl-ticket-ai-rationale             runs   3 errors   0 | oldest feed   1268 h (09-29) | 
- `05:18:52`     justhodl-trade-ticket-monitor            runs 291 errors   0 | oldest feed   1266 h (09-29) | REPORT RequestId: [...]	Duration: 110.59 ms	Billed Duration: 111 ms	Memory Size: 512 MB	Max Memory Used: 99 MB	
XRAY TraceId: 1-6aff429c-5e956c8c30ad62fe755253fc	SegmentI
- `05:18:52`     justhodl-position-sizer                  runs   3 errors   0 | oldest feed   1232 h (09-30) | 
- `05:18:52`     justhodl-contract-gate                   runs   3 errors   0 | oldest feed   1022 h (10-09) | [contracts] S1 MISSING_KEYS   data/readthrough.json — absent top-level keys: by_beneficiary, caveats, chase_guard, consensus_coverage, data_source, industry_boom_co
- `05:18:52`     justhodl-history-snapshotter             runs   3 errors   0 | oldest feed    928 h (10-13) | 
- `05:18:52`     justhodl-causality-scanner               runs   9 errors   9 | oldest feed    713 h (10-22) | 
- `05:18:52`     justhodl-bagger-engine                   runs   9 errors   9 | oldest feed    620 h (10-26) | [fmp-bankfirst] hits=1321 stale=0 misses=28 unshaped=0 passthrough=0 bypassed=957 errors=0 bank_time=60.5s
- `05:18:52`     justhodl-forward-orders                  runs   5 errors   0 | oldest feed    501 h (10-31) | 
- `05:18:52`     justhodl-momentum-breakout               runs   7 errors   6 | oldest feed    501 h (10-31) | 
- `05:18:52`     justhodl-revenue-acceleration            runs   7 errors   6 | oldest feed    501 h (10-31) | 
- `05:18:52`     justhodl-carry-surface                   runs   6 errors   0 | oldest feed    499 h (10-31) | [carry-research] unavailable: ValueError
- `05:18:52`     justhodl-earnings-quality                runs   6 errors   0 | oldest feed    498 h (10-31) | 
- `05:18:52`     justhodl-velocity-acceleration           runs   3 errors   0 | oldest feed    421 h (11-03) | REPORT RequestId: [...]	Duration: 3097.29 ms	Billed Duration: 3376 ms	Memory Size: 1024 MB	Max Memory Used: 125 MB	Init Duration: 277.96 ms	
XRAY TraceId: 1-6aff4038-7ed0
- `05:18:52`     justhodl-cross-asset-regime              runs   6 errors   0 | oldest feed    419 h (11-03) | 
- `05:18:52`     justhodl-leading-markets                 runs   6 errors   0 | oldest feed    417 h (11-03) | [fmp-bankfirst] hits=0 stale=50 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=6.5s
- `05:18:52`     justhodl-gold-equity-rotation            runs   3 errors   0 | oldest feed    417 h (11-03) | 
- `05:18:52`     justhodl-chart-patterns                  runs   9 errors   9 | oldest feed    416 h (11-03) | [fmp-bankfirst] hits=1 stale=999 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=66.8s
- `05:18:52`     justhodl-predictability                  runs   9 errors   9 | oldest feed    416 h (11-03) | [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=0 bypassed=0 errors=0 bank_time=0.1s
- `05:18:52`     justhodl-theme-rotation-engine           runs   3 errors   0 | oldest feed    404 h (11-04) | 
- `05:18:52`     justhodl-eps-revision-velocity           runs   7 errors   6 | oldest feed    357 h (11-06) | 
- `05:18:52` the day each engine's oldest late feed stopped moving: 08-07 x2, 08-16 x1, 09-10 x1, 09-19 x3, 09-20 x7, 09-21 x5, 09-27 x1, 09-28 x3, 09-29 x2, 09-30 x1, 10-09 x1, 10-13 x2, 10-17 x1, 10-22 x1, 10-26 x3, 10-29 x1, 10-31 x9, 11-03 x9, 11-04 x3, 11-06 x2, 11-10 x1, 11-11 x1, 11-12 x1, 11-13 x1, 11-14 x2, 11-15 x4, 11-16 x2, 11-17 x2, 11-18 x2, 11-19 x6
- `05:18:52` ✅ read-only; nothing changed

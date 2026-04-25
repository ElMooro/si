# Diagnose: where does Section 1 (morning brief archive) data live?

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-04-25T09:47:01+00:00  

## Log
## 1. Keys under learning/

- `09:47:00`   learning/last_log_run.json                               80B  age 0.6h
- `09:47:00`   learning/morning_run_log.json                           351B  age 20.8h
## 2. archive/ keys matching 'morning' or 'brief' or 'intelligence'

- `09:47:01`   Found 605 matching keys
- `09:47:01`     archive/intelligence/2026/04/25/0014.json                        4449B  age 9.6h
- `09:47:01`     archive/intelligence/2026/04/25/0010.json                        4264B  age 9.6h
- `09:47:01`     archive/intelligence/2026/04/25/0004.json                        3366B  age 9.7h
- `09:47:01`     archive/intelligence/2026/04/24/2305.json                        2785B  age 10.7h
- `09:47:01`     archive/intelligence/2026/04/24/2205.json                        2785B  age 11.7h
- `09:47:01`     archive/intelligence/2026/04/24/2105.json                        2785B  age 12.7h
- `09:47:01`     archive/intelligence/2026/04/24/2005.json                        2785B  age 13.7h
- `09:47:01`     archive/intelligence/2026/04/24/1905.json                        2785B  age 14.7h
- `09:47:01`     archive/intelligence/2026/04/24/1805.json                        2785B  age 15.7h
- `09:47:01`     archive/intelligence/2026/04/24/1705.json                        2785B  age 16.7h
- `09:47:01`     archive/intelligence/2026/04/24/1605.json                        2785B  age 17.7h
- `09:47:01`     archive/intelligence/2026/04/24/1505.json                        2785B  age 18.7h
- `09:47:01`     archive/intelligence/2026/04/24/1405.json                        2785B  age 19.7h
- `09:47:01`     archive/intelligence/2026/04/24/1305.json                        2785B  age 20.7h
- `09:47:01`     archive/intelligence/2026/04/24/1210.json                        2785B  age 21.6h
## 3. morning-intelligence Lambda source — find write paths

- `09:47:01`   Source file: aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py (358 LOC)
- `09:47:01`     writes: learning/morning_run_log.json
- `09:47:01` 
  Distinct writes found: 1
## 4. Read learning/morning_run_log.json structure

- `09:47:01`   Last modified: 2026-04-24 13:00:47+00:00
- `09:47:01`   Type: dict
- `09:47:01`   Top keys: ['improved', 'khalid', 'outcomes', 'regime', 'run_at', 'weights', 'wrong']
- `09:47:01`   Sample (first 800 chars):
- `09:47:01` {"run_at": "2026-04-24T13:00:46.150730+00:00", "outcomes": 168, "wrong": 0, "improved": false, "weights": 12, "khalid": {"score": 43, "regime": "BEAR", "signals": [["DXY", -12, "118.1"], ["HY Spread", 5, "2.84%"], ["Unemployment", -8, "4.3%"], ["Net Liq", 3, "$5.70T"], ["SPY Trend", 5, "$708"]], "ts": "2026-04-24T12:54:56.676793"}, "regime": "BEAR"}
## 5. S3 root: any morning-* / intelligence-* / brief-* files?

- `09:47:01`   crypto-intel.json                                            56189B  age 0.1h
- `09:47:01`   intelligence-report.json                                      4449B  age 9.6h
- `09:47:01`   intelligence.html                                            27710B  age 1465.5h
- `09:47:01`   prefix/ _audit/
- `09:47:01`   prefix/ _health/
- `09:47:01`   prefix/ archive/
- `09:47:01`   prefix/ bot/
- `09:47:01`   prefix/ calibration/
- `09:47:01`   prefix/ data/
- `09:47:01`   prefix/ deploy/
- `09:47:01`   prefix/ investor-analysis/
- `09:47:01`   prefix/ khalid/
- `09:47:01`   prefix/ learning/
- `09:47:01`   prefix/ reports/
- `09:47:01`   prefix/ screener/
- `09:47:01`   prefix/ secretary/
- `09:47:01`   prefix/ sentiment/
- `09:47:01`   prefix/ stock-analysis/
- `09:47:01`   prefix/ stock/
- `09:47:01`   prefix/ telegram/
- `09:47:01`   prefix/ valuations-archive/
## 6. Sample one archive/intelligence file to see if it's a morning brief

- `09:47:01`   Sampling: archive/intelligence/2026/04/24/1805.json (2,785B)
- `09:47:01`   Top keys: ['action_required', 'data_sources', 'dxy', 'forecast', 'generated_at', 'headline', 'headline_detail', 'metrics_table', 'ml_intelligence', 'phase', 'phase_color', 'plumbing_flags', 'portfolio', 'regime', 'risks', 'scores', 'signals', 'stock_signals', 'swap_spreads', 'timestamp']
- `09:47:01` Done

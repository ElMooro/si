# ops 6728 - why 18 engines have not written since before the Nov 1-2 freeze (triggers, invocations, errors; read-only)

**Status:** success  
**Duration:** 22.9s  
**Finished:** 2026-11-04T11:25:40+00:00  

## Data

| FAILING | ON_TIME | RAN_NO_WRITE | broken_count |
|---|---|---|---|
| earnings-blackout, ecb-derived, eps-revision-velocity, momentum-breakout, revenue-acceleration, systemic-stress, volatility-squeeze-hunter | base-rates, beaters-grader, liquidity-profile, naaim, sellside-views, spx-beaters | crypto-emergence, earnings-quality, forward-orders, portwatch, russell-recon-frontrun | 12 |

## Log
## 0. every Scheduler schedule in the account, indexed by target function (one listing, reused)

- `11:25:18` 452 schedules listed, 406 distinct target functions
## 1. CloudWatch per-day Invocations / Errors / Throttles, 2026-10-27 .. 2026-11-04

## 2. per engine: function, triggers, metrics, last errors, verdict

- `11:25:21` base-rates -> ON_TIME | state Active updated 2026-08-17T14:48 | reserved None | outputs base-rates.json 93h
- `11:25:21`     triggers (1 enabled, daily-or-faster False): sched justhodl-base-rates-weekly ENABLED cron(30 14 ? * SAT *)
- `11:25:21`     metrics: 10-31 n1 e0 t0
- `11:25:22` beaters-grader -> ON_TIME | state Active updated 2026-08-17T18:35 | reserved None | outputs beaters-learned-weights.json 92h
- `11:25:22`     triggers (1 enabled, daily-or-faster False): sched justhodl-beaters-grader-weekly ENABLED cron(0 15 ? * SAT *)
- `11:25:22`     metrics: 10-31 n1 e0 t0
- `11:25:23` crypto-emergence -> RAN_NO_WRITE | state Active updated 2026-10-26T21:50 | reserved None | outputs crypto-emergence.json 132h
- `11:25:23`     triggers (1 enabled, daily-or-faster True): rule justhodl-crypto-emergence-daily ENABLED cron(0 23 * * ? *)
- `11:25:23`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n2 e0 t0, 10-30 n1 e0 t0, 10-31 n1 e0 t0, 11-02 n1 e0 t0, 11-03 n2 e0 t0
- `11:25:24` earnings-blackout -> FAILING | state Active updated 2026-10-26T21:58 | reserved None | outputs earnings-blackout.json 120h, history/blackout.json 120h
- `11:25:24`     triggers (1 enabled, daily-or-faster True): rule justhodl-blackout-daily ENABLED cron(31 11 * * ? *)
- `11:25:24`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n3 e3 t0, 11-03 n6 e6 t0
- `11:25:24`     log: [ERROR] HTTPError: HTTP Error 429: Too Many Requests
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 66, in lambda_handler
    j = _get("https://financialmodelingprep.com/stable/earnings-ca
- `11:25:24`     log: [ERROR] HTTPError: HTTP Error 429: Too Many Requests
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 66, in lambda_handler
    j = _get("https://financialmodelingprep.com/stable/earnings-ca
- `11:25:24`     log: [ERROR] HTTPError: HTTP Error 429: Too Many Requests
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 66, in lambda_handler
    j = _get("https://financialmodelingprep.com/stable/earnings-ca
- `11:25:25` earnings-quality -> RAN_NO_WRITE | state Active updated 2026-10-26T21:59 | reserved None | outputs earnings-quality.json 117h
- `11:25:25`     triggers (1 enabled, daily-or-faster True): rule jh-earnings-quality-daily ENABLED cron(18 14 * * ? *); sched justhodl-earnings-quality-weekly DISABLED cron(30 13 ? * WED *)
- `11:25:25`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n1 e0 t0, 11-03 n2 e0 t0
- `11:25:27` ecb-derived -> FAILING | state Active updated 2026-10-08T17:31 | reserved None | outputs ecb-derived.json 92h, ecb-hist/esi.json 92h
- `11:25:27`     triggers (1 enabled, daily-or-faster True): rule justhodl-ecb-derived-daily ENABLED cron(30 15 * * ? *)
- `11:25:27`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n1 e0 t0, 11-03 n6 e6 t0
- `11:25:28` eps-revision-velocity -> FAILING | state Active updated 2026-10-26T22:02 | reserved None | outputs eps-revision-velocity.json 120h
- `11:25:28`     triggers (0 enabled, daily-or-faster False): NONE
- `11:25:28`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n3 e3 t0, 11-02 n3 e3 t0, 11-03 n3 e3 t0
- `11:25:28`     log: [ERROR] ValueError: All provider populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 692, in lambda_handler
    raise ValueError('All prov
- `11:25:28`     log: [ERROR] ValueError: All provider populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 692, in lambda_handler
    raise ValueError('All prov
- `11:25:28`     log: [ERROR] ValueError: All provider populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 692, in lambda_handler
    raise ValueError('All prov
- `11:25:29` forward-orders -> RAN_NO_WRITE | state Active updated 2026-11-03T05:51 | reserved None | outputs forward-orders.json 120h
- `11:25:29`     triggers (1 enabled, daily-or-faster True): rule forward-orders-daily ENABLED cron(1 11 * * ? *); sched justhodl-forward-orders-daily DISABLED cron(50 21 ? * MON-FRI *)
- `11:25:29`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 11-02 n1 e0 t0, 11-03 n2 e0 t0
- `11:25:30` liquidity-profile -> ON_TIME | state Active updated 2026-10-26T22:29 | reserved None | outputs liquidity-profile.json 109h
- `11:25:30`     triggers (1 enabled, daily-or-faster False): rule liquidity-profile-fri ENABLED cron(3 22 ? * FRI *)
- `11:25:30`     metrics: 10-30 n1 e0 t0
- `11:25:31` momentum-breakout -> FAILING | state Active updated 2026-10-26T22:36 | reserved None | outputs momentum-breakout.json 120h
- `11:25:31`     triggers (0 enabled, daily-or-faster False): NONE
- `11:25:31`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n3 e3 t0, 11-02 n3 e3 t0, 11-03 n3 e3 t0
- `11:25:31`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 476, in lambda_handler
    raise ValueError('No received market observa
- `11:25:31`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 476, in lambda_handler
    raise ValueError('No received market observa
- `11:25:31`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 476, in lambda_handler
    raise ValueError('No received market observa
- `11:25:32` naaim -> ON_TIME | state Active updated 2026-07-02T00:11 | reserved None | outputs naaim.json 114h
- `11:25:32`     triggers (1 enabled, daily-or-faster False): rule justhodl-naaim-weekly ENABLED cron(30 17 ? * THU,FRI *)
- `11:25:32`     metrics: 10-29 n1 e0 t0, 10-30 n1 e0 t0
- `11:25:33` portwatch -> RAN_NO_WRITE | state Active updated 2026-09-27T12:02 | reserved None | outputs portwatch.json 168h
- `11:25:33`     triggers (1 enabled, daily-or-faster True): sched justhodl-portwatch-daily ENABLED cron(20 11 * * ? *)
- `11:25:33`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 11-02 n1 e0 t0, 11-03 n1 e0 t0
- `11:25:34` revenue-acceleration -> FAILING | state Active updated 2026-10-26T23:00 | reserved None | outputs revenue-acceleration.json 120h
- `11:25:34`     triggers (0 enabled, daily-or-faster False): NONE
- `11:25:34`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n3 e3 t0, 11-02 n3 e3 t0, 11-03 n3 e3 t0
- `11:25:34`     log: [ERROR] ValueError: All statement populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 584, in lambda_handler
    raise ValueError('All sta
- `11:25:34`     log: [ERROR] ValueError: All statement populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 584, in lambda_handler
    raise ValueError('All sta
- `11:25:34`     log: [ERROR] ValueError: All statement populations unavailable; preserve previous publication
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 584, in lambda_handler
    raise ValueError('All sta
- `11:25:35` russell-recon-frontrun -> RAN_NO_WRITE | state Active updated 2026-10-26T23:02 | reserved None | outputs russell-recon-frontrun.json 119h
- `11:25:35`     triggers (1 enabled, daily-or-faster True): rule justhodl-russell-recon-daily ENABLED cron(30 12 * * ? *); rule justhodl-russell-recon-frontrun-sched DISABLED cron(30 12 * * ? *)
- `11:25:35`     metrics: 10-27 n1 e0 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n1 e0 t0, 11-03 n2 e0 t0
- `11:25:36` sellside-views -> ON_TIME | state Active updated 2026-10-26T23:06 | reserved None | outputs sellside-views.json 117h
- `11:25:36`     triggers (1 enabled, daily-or-faster False): rule sellside-views-mwf ENABLED cron(4 14 ? * MON,WED,FRI *)
- `11:25:36`     metrics: 10-28 n1 e0 t0, 10-30 n1 e0 t0
- `11:25:37` spx-beaters -> ON_TIME | state Active updated 2026-09-25T14:08 | reserved None | outputs spx-beaters.json 93h
- `11:25:37`     triggers (1 enabled, daily-or-faster False): sched justhodl-spx-beaters-weekly ENABLED cron(36 14 ? * SAT *)
- `11:25:37`     metrics: 10-31 n1 e0 t0
- `11:25:39` systemic-stress -> FAILING | state Active updated 2026-09-09T03:35 | reserved None | outputs systemic-stress.json 95h
- `11:25:39`     triggers (1 enabled, daily-or-faster True): rule systemic-stress-daily ENABLED cron(8 12 * * ? *)
- `11:25:39`     metrics: 10-27 n1 e0 t0, 10-28 n2 e1 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n1 e0 t0, 11-03 n6 e6 t0
- `11:25:40` volatility-squeeze-hunter -> FAILING | state Active updated 2026-10-26T23:33 | reserved None | outputs volatility-squeeze.json 120h
- `11:25:40`     triggers (0 enabled, daily-or-faster False): NONE
- `11:25:40`     metrics: 10-27 n3 e3 t0, 10-28 n1 e0 t0, 10-29 n1 e0 t0, 10-30 n1 e0 t0, 10-31 n3 e3 t0, 11-03 n3 e3 t0
- `11:25:40`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 555, in lambda_handler
    raise ValueError('No received market observa
- `11:25:40`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 555, in lambda_handler
    raise ValueError('No received market observa
- `11:25:40`     log: [ERROR] ValueError: No received market observations; preserve current
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 555, in lambda_handler
    raise ValueError('No received market observa
## 3. verdicts

- `11:25:40` FAILING (7): earnings-blackout, ecb-derived, eps-revision-velocity, momentum-breakout, revenue-acceleration, systemic-stress, volatility-squeeze-hunter
- `11:25:40` ON_TIME (6): base-rates, beaters-grader, liquidity-profile, naaim, sellside-views, spx-beaters
- `11:25:40` RAN_NO_WRITE (5): crypto-emergence, earnings-quality, forward-orders, portwatch, russell-recon-frontrun
- `11:25:40` ⚠ 12 engines need a fix (FAILING / NEVER_STARTED / THROTTLED / RAN_NO_WRITE / NO_FUNCTION) - nothing was changed here
- `11:25:40` ✅ read-only; nothing invoked, nothing changed

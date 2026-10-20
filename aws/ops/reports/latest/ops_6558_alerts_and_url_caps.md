# ops 6558 - fleet-alerts e-mail channel + reserved-concurrency caps on public heavy Function URLs

**Status:** success  
**Duration:** 268.0s  
**Finished:** 2026-10-20T19:06:34+00:00  

## Data

| capped_now | email | held_by_governor | public | public_heavy | urls |
|---|---|---|---|---|---|
| 102 | {"ra***@gmail.com": "confirmation e-mail sent (was absent)"} | 102 | 153 | 102 | 153 |

## Log
- `19:02:08` fleet-alerts subscriptions: email {"ra***@gmail.com": "confirmation e-mail sent (was absent)"}; other [('lambda', 'confirmed')]
- `19:05:28` function URLs: 153 total, 153 with AuthType NONE (public), 102 public + heavy
- `19:06:23` governor tick: stage FREEZE drift {"new": {"applied": 2}, "reapplied": {"applied": 100}}
- `19:06:34` ✅ 102 held functions re-held at 0 by the governor; restore value 2 recorded for every one
- `19:06:34` public heavy URL functions: 102; cap 2 applied to 102 (102 of them were held by the governor and are held again); skipped 0
- `19:06:34`   public+heavy justhodl-thesis-engine                   mem  3008  avg  632098 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-wl-engines                      mem  3008  avg  569302 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-research-backtest               mem  2048  avg  549520 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-etf-constituents                mem  3072  avg  217529 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-provider-catalog                mem  1024  avg  383643 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-market-internals                mem  3008  avg  120452 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-carry-surface                   mem  1024  avg  294039 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-symbol-dictionary               mem  2048  avg  122426 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-13f-positions                   mem  1024  avg  208119 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-floor-audit                     mem  2048  avg   94730 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-etf-fund-flows                  mem  2048  avg   78663 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-industry-rotation               mem  1024  avg  157290 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-stock-screener                  mem  1280  avg  120519 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-flows-ai-analysis               mem  1024  avg  138464 ms  reserved before 0
- `19:06:34`   public+heavy justhodl-liquidity-inflection            mem  1536  avg   87107 ms  reserved before 0
- `19:06:34` ✅ done

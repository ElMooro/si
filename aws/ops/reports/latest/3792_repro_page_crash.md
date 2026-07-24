# ops 3792 — reproduce the TypeError against the live feed

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-07-24T02:00:13+00:00  

## Data

| eligible_n_ge_2 | industries | industries_all_null | industries_without_best | pct | rows | rows_with_null_capture_gap | version |
|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 3411 |  | 4.2.1 |
| 143 | 144 |  | 0 |  |  |  |  |
|  |  |  |  | 0.0 |  | 0 |  |
|  |  | 0 |  |  |  |  |  |

## Log
## Simulate the page's byInd reducer exactly

- `02:00:13` ✅ REPRO.crash_condition :: 0 eligible industries would dereference undefined .best
## Null-capture_gap analysis (the real trigger)

## Other unguarded dereferences to fix in the same pass

- `02:00:13`   line 224  sort: (b.best.capture_gap||0) — b.best may be undefined
- `02:00:13`   line 228  x.best.capture_gap
- `02:00:13`   line 229  x.best.ticker / x.best.mcap_share_pct
- `02:00:13`   Any ONE of these throws inside the shared try{} and kills every
- `02:00:13`   section below it, which is why the leaderboard vanished too.
## VERDICT

- `02:00:13` ⚠ Not reproduced from null capture_gap — widen the search to other unguarded dereferences (r.tier, m.ticker in members).
- `02:00:13` ✅ PASS_ALL — diagnosis complete

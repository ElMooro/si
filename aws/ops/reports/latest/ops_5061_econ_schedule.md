## P0 which rules have a free target slot

**Status:** success  
**Duration:** 3635.8s  
**Finished:** 2026-08-30T17:38:42+00:00  

## Data

| host | n_done | n_total | objects | rows |
|---|---|---|---|---|
| carry-surface-4h | 10 | 1226 | 11 | 1382781 |

## Log
- `16:38:27`   enabled scheduled rules with room: 513
- `16:38:27`     carry-surface-4h                           rate(4 hours)      1/5 targets
- `16:38:27`     cftc-cot-weekly-update                     rate(6 hours)      1/5 targets
- `16:38:27`     cross-asset-confirm-3h                     rate(3 hours)      1/5 targets
- `16:38:27`     event-flow-monitor-hourly                  rate(6 hours)      1/5 targets
- `16:38:27`     fed-nlp-6h                                 rate(6 hours)      1/5 targets
- `16:38:27`     fmp-movers-hourly                          rate(1 hour)       1/5 targets
- `16:38:27`     gap-metrics-daily                          rate(2 hours)      1/5 targets
- `16:38:27`     jsi-6h                                     rate(6 hours)      1/5 targets
## P1 attach the census targets

- `16:38:27`   host rule: carry-surface-4h (rate(4 hours), 1 targets used)
- `16:38:27`   invoke permission granted for carry-surface-4h
- `16:38:27`   put_targets failed=0
- `16:38:27`   carry-surface-4h now has 3 targets: ['1', 'censusecon', 'censusts']
## P2 drain (Event invokes, polled -- never sync)

- `16:38:27`   before: n_done=1/1226 rows=75,573 failures=0
- `16:53:31`   cycle 1  n_done=4/1226  rows=377,739  queue_left=1223  failures=0
- `17:08:34`   cycle 2  n_done=8/1226  rows=862,441  queue_left=1219  failures=0
- `17:23:38`   cycle 3  n_done=9/1226  rows=1,123,121  queue_left=1218  failures=0
- `17:38:41`   cycle 4  n_done=10/1226  rows=1,382,781  queue_left=1216  failures=0
- `17:38:41`   drained 9 entries in 60 min -> 0.1 entries/min
- `17:38:41`   1216 entries left -> ~135.6 h at this rate
## P3 what landed

- `17:38:41`   11 objects, 14.4 MB
- `17:38:41`     cbp              11 objects      14.4 MB
- `17:38:41`   cbp/cbp/2013/g0-c0.json.gz                     129,720 rows · NAICS2012 has 6 distinct codes
- `17:38:42`   cbp/cbp/2014/g0-c0.json.gz                     129,940 rows · NAICS2012 has 6 distinct codes
- `17:38:42` ops 5061 GREEN -- econ lane scheduled and draining

## G0 state

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-08-23T20:57:40+00:00  

## Data

| banked_this_crank | gb | have | inventory | keys | kicks | missing | named_failures |
|---|---|---|---|---|---|---|---|
| 0 | 6.92 | 50 | 50 | 51 | 0 | 0 | 0 |

## Log
- `20:57:40` G0 inventory=50 have=50 missing=0 failures=10 lease=free
- `20:57:40`   fail 2012_q1   tries=2 err=err:HTTP429
- `20:57:40`   fail 2014_q1   tries=2 err=err:HTTP429
- `20:57:40`   fail 2014_q2   tries=1 err=err:HTTP429
- `20:57:40`   fail 2014_q3   tries=1 err=err:HTTP429
- `20:57:40`   fail 2015_q2   tries=1 err=err:HTTP429
- `20:57:40`   fail 2015_q3   tries=1 err=err:HTTP429
- `20:57:40`   fail 2016_q3   tries=1 err=err:HTTP429
- `20:57:40`   fail 2016_q4   tries=1 err=err:HTTP429
- `20:57:40`   fail 2017_q1   tries=2 err=err:HTTP429
- `20:57:40`   fail 2017_q2   tries=2 err=err:HTTP429
## G1 crank

- `20:57:40` G1 done have=50/50 missing=0 kicks=0 elapsed=0s
## G2 verdict

- `20:57:40` G2 PASS drain FULL: every inventoried quarter banked
## G3 bytes/keys census

- `20:57:40` G3 PASS keys=51 6.92GB (have 50 -> 50 this crank)
- `20:57:40` ops 4955 GREEN -- MIDAS drained to terminal: the pause was the chain ending, not a failure; 6h schedule keeps it topped up from here

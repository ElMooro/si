## P0 GDELT after wiring

**Status:** success  
**Duration:** 507.6s  
**Finished:** 2026-08-31T01:21:41+00:00  

## Data

| boj_pct | dormant | gdelt_gaps | ondemand |
|---|---|---|---|
| 50.4 | 19 | 7381 | 362 |

## Log
- `01:13:13`   before: files=396881 gaps=7381 cursor=20260831003000 as_of=2026-08-31T01:11:32+00:00
- `01:13:13`   invocations last 2h: [('00:13', 2)]
## P1 classify the untriggered

- `01:13:14`   untriggered functions from ops 5069: 381
- `01:13:18`   state documents indexed: 90 (data/_state/ + 61 provider dirs)
- `01:13:18`   DORMANT DATA LANES (own state, no trigger, no runs): 19
- `01:13:18`   on-demand / no state document (correct to have no rule): 362
- `01:13:18`   examples of the latter: ['justhodl-13f-clone-alpha', 'justhodl-52wk-quality-breakout', 'justhodl-a2a-bus', 'justhodl-ab-test', 'justhodl-accum-composite', 'justhodl-activist-13d', 'justhodl-activist-filings-scanner', 'justhodl-activity-nowcast']
## P2 dormant lanes, stalest first

- `01:13:18`   function                                   state doc              age(h)
- `01:13:18`   justhodl-repo                              repo-probe-manifes      387.7
- `01:13:18`   justhodl-fundamental-census                fundamental-census      171.7
- `01:13:18`   justhodl-gdelt-full                        gdelt-full              141.0
- `01:13:18`   justhodl-hist-banker                       hist-banker             116.5
- `01:13:18`   justhodl-bls-full                          bls-full                 22.7
- `01:13:18`   justhodl-boe-full                          boe-full                 22.7
- `01:13:18`   justhodl-dol-full                          dol-full                 22.7
- `01:13:18`   justhodl-finra-full                        finra-full               22.7
- `01:13:18`   justhodl-fiscaldata-full                   fiscaldata-full          22.7
- `01:13:18`   justhodl-frbddp-full                       frbddp-full              22.7
- `01:13:18`   justhodl-polygon-full                      polygon-full             22.7
- `01:13:18`   justhodl-sec-midas                         sec-midas                22.7
- `01:13:18`   justhodl-src-mirror                        src-mirror               22.7
- `01:13:18`   justhodl-tic-full                          tic-full                 22.7
- `01:13:18`   justhodl-imf-full                          imf-full                 22.2
- `01:13:18`   justhodl-asia-trade-full                   asia-trade                9.2
- `01:13:18`   justhodl-velocity-acceleration             velocity-accelerat        0.7
- `01:13:18`   justhodl-worldbank-full                    worldbank-full            0.7
- `01:13:18`   justhodl-ecb-deep                          ecb-deep                  0.0
- `01:13:18`   -> each of these owns a warehouse that has not been
- `01:13:18`      written since the age above. census-us, boj-full
- `01:13:18`      and gdelt-full all looked exactly like this.
## P3 BOJ + GDELT progress

- `01:21:38`   gdelt after: files=396882 (was 396881)  gaps=7381 (was 7381)
- `01:21:38`   state advanced (as_of moved) but gap count flat -- it is fetching forward, not backfilling yet
- `01:21:40`   boj: 60,725/120,394 series (50.4%)  rows 294,539  lease-skips 0
- `01:21:41`   -> data/ops/dormant-lanes.json
- `01:21:41` ops 5070 GREEN -- dormant lanes separated from on-demand

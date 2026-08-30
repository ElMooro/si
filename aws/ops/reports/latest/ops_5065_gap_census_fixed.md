## P0 structural discovery

**Status:** success  
**Duration:** 41.8s  
**Finished:** 2026-08-30T20:41:16+00:00  

## Data

| docs | gaps | top | total_missing |
|---|---|---|---|
| 238 | 8 | data/_state/census-econ.json | 2293 |

## Log
- `20:40:35`   provider directories under data/warm/: 61
- `20:40:35`   ['_audit', 'archived-fred', 'asia-trade', 'backlog', 'banxico', 'bis', 'blackswan', 'bls-full', 'boe-full', 'boe', 'boj-full', 'census-econ', 'census-us', 'cftc'] …
- `20:40:35`   data/_state/ documents: 71
- `20:40:46`   candidate state documents reached: 238  (ops 5064 reached far fewer and called it a clean fleet)
## P1 every done/total pair, ranked by absolute missing

- `20:41:12`   where                                       missing         of   have% kind
- `20:41:12`   data/_state/census-econ.json                  1,211      1,226    1.2% fixable
- `20:41:12`   data/_state/census-econ-s1.json                 203        211    3.8% fixable
- `20:41:12`   data/_state/census-econ-s2.json                 202        213    5.2% fixable
- `20:41:12`   data/_state/census-econ-s4.json                 190        198    4.0% fixable
- `20:41:12`   data/_state/census-econ-s5.json                 182        189    3.7% fixable
- `20:41:12`   data/_state/census-econ-s3.json                 153        201   23.9% fixable
- `20:41:12`   data/_state/census-econ-s0.json                 151        214   29.4% fixable
- `20:41:12`   census-us/_state/state.json                       1         56   98.2% fixable
- `20:41:12`   documents with a measurable gap: 8
- `20:41:12`   TOTAL MISSING across the fleet: 2,293 items
## P2 the named lanes, at whatever path they really use

- `20:41:12`   boj        api_BP01.json                                  keys=['codes', 'db', 'done', 'fail', 'parts', 'rows']
- `20:41:13`   gdelt      state.json                                     keys=['as_of', 'bytes', 'cursor', 'failures', 'files', 'gaps', 'gaps_sample', 'lease_until', 'phase']
- `20:41:13`   fred       fred-categories.json                           keys=['cats', 'errors', 'frontier', 'n_categories', 'status', 'updated_at', 'visited']
- `20:41:14`   imf        IMF:BP6.manifest.json                          keys=['as_of', 'complete', 'engine', 'first_period', 'flow', 'n_parts', 'parts', 'total_raw_bytes']
- `20:41:14`   worldbank  state.json                                     keys=['as_of', 'bytes_total', 'failures', 'have', 'lease_until', 'n_banked', 'n_indicators', 'phase', 'queue']
- `20:41:14`   oecd       sdmx-walk-oecd.json                            keys=['as_of', 'done', 'failures', 'lease_until', 'n_total', 'progress_pct', 'retried_fail', 'retried_ok', 'status']
- `20:41:15`   finra      state.json                                     keys=['as_of', 'drain_src', 'failures', 'have', 'invalid', 'last_discover', 'lease_until', 'n_banked', 'phase']
- `20:41:15`   nyfed      hist-state.json                                keys=['families', 'mark']
- `20:41:15`   frbddp     state.json                                     keys=['as_of', 'failures', 'lease_until', 'rels', 'version']
- `20:41:16`   tic        state.json                                     keys=['as_of', 'failures', 'files', 'lease_until', 'version']
## P3 expedite order

- `20:41:16`   FIXABLE: 8 lanes, 2,293 items missing
- `20:41:16`     data/_state/census-econ.json                1,211 missing  (1% held)
- `20:41:16`     data/_state/census-econ-s1.json               203 missing  (4% held)
- `20:41:16`     data/_state/census-econ-s2.json               202 missing  (5% held)
- `20:41:16`     data/_state/census-econ-s4.json               190 missing  (4% held)
- `20:41:16`     data/_state/census-econ-s5.json               182 missing  (4% held)
- `20:41:16`     data/_state/census-econ-s3.json               153 missing  (24% held)
- `20:41:16`     data/_state/census-econ-s0.json               151 missing  (29% held)
- `20:41:16`     census-us/_state/state.json                     1 missing  (98% held)
- `20:41:16`   BLOCKED-LOOKING: 0 lanes, 0 items
- `20:41:16`   -> data/ops/fleet-gaps.json
- `20:41:16` ops 5065 GREEN -- backlog quantified on a scan that actually reached the fleet

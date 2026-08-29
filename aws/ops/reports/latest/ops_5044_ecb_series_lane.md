## P0 deploy + eurostat unharmed

**Status:** success  
**Duration:** 35.5s  
**Finished:** 2026-08-29T20:36:05+00:00  

## Data

| ecb_flows | ecb_pages | ecb_series | ecb_total |
|---|---|---|---|
| 6 | 48 | 24000 | 207 |

## Log
- `20:35:30`   code fresh 2026-08-29T20:35:29  mem=10240 timeout=900
- `20:35:33`   eurostat: flows=8147 n_pages=1128408 series=564204000 updated_at=2026-08-29T20:35:18+00:00
## P1 wire the ecb target

- `20:35:34`   existing targets: [('t1', '{"provider": "eurostat"}')]
- `20:35:34`   targets now: [('ecb', '{"provider": "ecb"}'), ('t1', '{"provider": "eurostat"}')]
- `20:35:34`   reserved concurrency stays 1, so ecb and eurostat serialise -- separate state docs and separate page prefixes mean they cannot collide
## P2 first ECB run

- `20:35:34`   kick sent
- `20:36:05`   t+  0min flows=6/207 n_pages=48 series=24000 stopped_early=None
## P3 read back a page -- is the history really merged

- `20:36:05`   pages present: ['data/providers/ecb/series/page-0000.json', 'data/providers/ecb/series/page-0001.json', 'data/providers/ecb/series/page-0002.json'] …
- `20:36:05`   page-0000.json: count=500 rows=500
- `20:36:05`     ecb:AGR:AGR.M.I10.N.AGRI.X00000.4F0.N.IX   2000-01..2026-07  n_obs=319 last=166.19 geo=I10
- `20:36:05`     ecb:AGR:AGR.M.I10.N.AGRI.XCEREA.4F0.N.IX   1991-01..2026-07  n_obs=427 last=122.05 geo=I10
- `20:36:05`     ecb:AGR:AGR.M.I10.N.AGRI.XDAIR0.4F0.N.IX   1991-01..2026-07  n_obs=427 last=166.72 geo=I10
- `20:36:05`     ecb:AGR:AGR.M.I10.N.AGRI.XMEAT0.4F0.N.IX   1991-01..2026-07  n_obs=427 last=187.64 geo=I10
- `20:36:05`   records spanning more than one year: 499/500 (cross-slice merge working)
- `20:36:05`   -> data/ops/ecb-series-lane.json
- `20:36:05` ops 5044 GREEN -- ECB series lane live and merging history

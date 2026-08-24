## G-1 marker-in-checkout

**Status:** success  
**Duration:** 327.1s  
**Finished:** 2026-08-24T02:20:05+00:00  

## Data

| ambs_hint | effr_obs | families_ok | raw_mb | repo_hint |
|---|---|---|---|---|
| 10226 | 6568 | 10 | 13.87 | 39447 |

## Log
- `02:14:38`   ok 'nyfed-hist-v1b ops4963'
## G0 settle

- `02:14:38`   settled (0s)
## G1 kick -> hist families converge

- `02:15:03`   t+  25s mark=False ok=9/11
- `02:15:28`   t+  50s mark=False ok=9/11
- `02:15:53`   t+  75s mark=False ok=9/11
- `02:16:18`   t+ 100s mark=False ok=9/11
- `02:16:43`   t+ 125s mark=False ok=9/11
- `02:17:09`   t+ 150s mark=False ok=9/11
- `02:17:34`   t+ 175s mark=False ok=9/11
- `02:17:59`   t+ 200s mark=False ok=9/11
- `02:18:24`   t+ 225s mark=False ok=9/11
- `02:18:49`   t+ 251s mark=False ok=9/11
- `02:19:14`   t+ 276s mark=False ok=9/11
- `02:19:39`   t+ 301s mark=False ok=9/11
- `02:19:39`   re-kick #1
- `02:20:05`   t+ 326s mark=True ok=10/11
- `02:20:05`   ambs_ops         ok=True bytes=1503326 n_hint=10226 /ambs/all/results/summary/search.json?startDate=2009-01-01
- `02:20:05`   fxs_all          ok=True bytes=503898 n_hint=3036 /fxs/all/search.json?startDate=2000-01-01
- `02:20:05`   rates_bgcr       ok=True bytes=496924 n_hint=2515 /rates/secured/bgcr/search.json?startDate=2014-01-01&endDate
- `02:20:05`   rates_effr       ok=True bytes=1514800 n_hint=8195 /rates/unsecured/effr/search.json?startDate=2000-01-01&endDa
- `02:20:05`   rates_obfr       ok=True bytes=623819 n_hint=3158 /rates/unsecured/obfr/search.json?startDate=2000-01-01&endDa
- `02:20:05`   rates_sofr       ok=True bytes=497983 n_hint=2515 /rates/secured/sofr/search.json?startDate=2014-01-01&endDate
- `02:20:05`   rates_tgcr       ok=True bytes=496908 n_hint=2515 /rates/secured/tgcr/search.json?startDate=2014-01-01&endDate
- `02:20:05`   repo_ops         ok=True bytes=7315625 n_hint=39447 /rp/results/search.json?startDate=2000-01-01
- `02:20:05`   seclending_ops   ok=False bytes=None n_hint=None /seclending/all/results/summary/search.json?startDate=2000-0
- `02:20:05`   soma_asofdates   ok=True bytes=15716 n_hint=120 /soma/asofdates/list.json
- `02:20:05`   tsy_ops          ok=True bytes=899621 n_hint=5688 /tsy/all/results/summary/search.json?startDate=2000-01-01
- `02:20:05` G1 PASS ok_fams=10 effr=8195 repo=39447 ambs=10226
## G2 substance: EFFR span 2000 -> current

- `02:20:05` G2 PASS n=6568 span=2000-07-03..2026-08-20
## G3 hist keys landed

- `02:20:05`   ambs_ops.json.gz              63.72KB
- `02:20:05`   fxs_all.json.gz               25.46KB
- `02:20:05`   rates_bgcr.json.gz            20.86KB
- `02:20:05`   rates_effr.json.gz            48.94KB
- `02:20:05`   rates_obfr.json.gz            20.62KB
- `02:20:05`   rates_sofr.json.gz            22.71KB
- `02:20:05`   rates_tgcr.json.gz            20.46KB
- `02:20:05`   repo_ops.json.gz             355.69KB
- `02:20:05`   soma_asofdates.json.gz         3.22KB
- `02:20:05`   tsy_ops.json.gz               43.55KB
- `02:20:05` G3 PASS files=10 gz=0.63MB raw=13.87MB (gz-aware floor)
- `02:20:05` ops 4963 GREEN -- nyfed lane #1 full-depth: latest-only families now carry complete source history, self-refreshing on the existing hourly schedule

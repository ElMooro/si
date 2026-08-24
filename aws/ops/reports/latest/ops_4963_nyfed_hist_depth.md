## G-1 marker-in-checkout

**Status:** failure  
**Duration:** 25.5s  
**Finished:** 2026-08-24T02:10:22+00:00  

## Error

```
SystemExit: 1
```

## Log
- `02:09:56`   ok 'nyfed-hist-v1 ops4963'
## G0 settle

- `02:09:56`   settled (0s)
## G1 kick -> hist families converge

- `02:10:21`   t+  25s mark=True ok=9/11
- `02:10:21`   ambs_ops         ok=True bytes=1503326 n_hint=10226 /ambs/all/results/summary/search.json?startDate=2009-01-01
- `02:10:21`   fxs_all          ok=True bytes=503898 n_hint=3036 /fxs/all/search.json?startDate=2000-01-01
- `02:10:21`   rates_bgcr       ok=True bytes=496924 n_hint=2515 /rates/secured/bgcr/search.json?startDate=2014-01-01&endDate
- `02:10:21`   rates_effr       ok=True bytes=1514800 n_hint=8195 /rates/unsecured/effr/search.json?startDate=2000-01-01&endDa
- `02:10:21`   rates_obfr       ok=True bytes=623819 n_hint=3158 /rates/unsecured/obfr/search.json?startDate=2000-01-01&endDa
- `02:10:21`   rates_sofr       ok=True bytes=497983 n_hint=2515 /rates/secured/sofr/search.json?startDate=2014-01-01&endDate
- `02:10:21`   rates_tgcr       ok=True bytes=496908 n_hint=2515 /rates/secured/tgcr/search.json?startDate=2014-01-01&endDate
- `02:10:21`   repo_ops         ok=True bytes=7315625 n_hint=39447 /rp/results/search.json?startDate=2000-01-01
- `02:10:21`   seclending_ops   ok=False bytes=None n_hint=None /seclending/all/all/results/search.json?startDate=2000-01-01
- `02:10:21`   soma_asofdates   ok=True bytes=15716 n_hint=120 /soma/asofdates/list.json
- `02:10:21`   tsy_ops          ok=False bytes=None n_hint=None /tsy/all/results/search.json?startDate=2000-01-01 -> HTTPErr
- `02:10:21` G1 PASS ok_fams=9 effr=8195 repo=39447 ambs=10226
## G2 substance: EFFR span 2000 -> current

- `02:10:21` G2 PASS n=6568 span=2000-07-03..2026-08-20
## G3 hist keys landed

- `02:10:22`   ambs_ops.json.gz              63.72KB
- `02:10:22`   fxs_all.json.gz               25.46KB
- `02:10:22`   rates_bgcr.json.gz            20.86KB
- `02:10:22`   rates_effr.json.gz            48.94KB
- `02:10:22`   rates_obfr.json.gz            20.62KB
- `02:10:22`   rates_sofr.json.gz            22.71KB
- `02:10:22`   rates_tgcr.json.gz            20.46KB
- `02:10:22`   repo_ops.json.gz             355.69KB
- `02:10:22`   soma_asofdates.json.gz         3.22KB
- `02:10:22` G3 FAIL files=9 0.58MB
- `02:10:22` ops 4963 RED: G3

## G-1 markers-in-checkout

**Status:** failure  
**Duration:** 1266.1s  
**Finished:** 2026-08-24T23:48:45+00:00  

## Error

```
SystemExit: 1
```

## Log
- `23:27:39`   ok justhodl-imf-full        'v1.0.2 ops4967'
- `23:27:39`   ok justhodl-provider-catalog 'imf-note-v2'
- `23:27:39`   ok justhodl-gov-sources     'imf-api-v2 ops4967'
## G0 settle x3

- `23:28:05`   justhodl-imf-full settled (25s)
- `23:28:05`   justhodl-provider-catalog settled (0s)
- `23:28:05`   justhodl-gov-sources settled (0s)
- `23:28:05` G0 PASS
## G0b schedules

- `23:28:05`   exists justhodl-imf-full-6h (ok)
- `23:28:05`   exists justhodl-imf-full-weekly (ok)
## G1 chain-drive (15min)

- `23:28:06`   t+   0s DISCOVER banked=0 q=0 cat=0 fail=1
- `23:28:31`   t+  25s DRAIN banked=5 q=217 cat=222 fail=1
- `23:28:56`   t+  50s DRAIN banked=10 q=212 cat=222 fail=1
- `23:32:42`   t+ 276s DRAIN banked=11 q=211 cat=222 fail=1
- `23:36:28`   t+ 502s DRAIN banked=12 q=210 cat=222 fail=1
- `23:39:48`   t+ 702s DRAIN banked=13 q=209 cat=222 fail=1
- `23:43:09` G1 FAIL phase=DRAIN banked=13 catalog=222 failures=1
## G2 substance: BOP SDMX payload

- `23:43:14`   BOP raw=1866.07MB obs_tags=12219733
- `23:43:14` G2 PASS
## G3 card (post-mark)

- `23:48:45` G3 PASS note=FULL SDMX-2.1 warehouse (imf-full v1) on api.imf.org: 13/222 dataflows · 8 vintages retained · 5.65GB · 0 lastN-partial · phase DRAIN · daily rediscovery
## DAY-TWO board (info)

- `23:48:45`   worldbank: phase=DRAIN banked=9200 q=20289
- `23:48:45`   gdelt: phase=DRAIN files=334653 gb=35.12 cursor=2024110307 gaps=5699 v1=0/None
- `23:48:45`   bls: phase=COMPLETE files=1659 gb=40.06
- `23:48:45`   dol: files=70 mb=160.6 fresh=70 unchanged=0
- `23:48:45` ops 4967 RED: G1

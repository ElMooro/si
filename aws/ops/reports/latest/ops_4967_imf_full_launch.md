## G-1 markers-in-checkout

**Status:** failure  
**Duration:** 1197.5s  
**Finished:** 2026-08-24T11:43:41+00:00  

## Error

```
SystemExit: 1
```

## Log
- `11:23:44`   ok justhodl-imf-full        'v1.0.0 ops4967'
- `11:23:44`   ok justhodl-provider-catalog 'imf-note-v2'
- `11:23:44`   ok justhodl-gov-sources     'imf-api-v2 ops4967'
## G0 settle x3

- `11:23:44`   justhodl-imf-full t+0s NOT FOUND
- `11:24:09`   justhodl-imf-full settled (25s)
- `11:24:35`   justhodl-provider-catalog settled (25s)
- `11:24:35`   justhodl-gov-sources settled (0s)
- `11:24:35` G0 PASS
## G0b schedules

- `11:24:35`   created justhodl-imf-full-6h rate(6 hours)
- `11:24:35`   created justhodl-imf-full-weekly rate(7 days)
## G1 chain-drive (15min)

- `11:24:36`   t+   0s None banked=0 q=0 cat=0 fail=0
- `11:25:01`   t+  25s DISCOVER banked=0 q=0 cat=0 fail=1
- `11:28:47`   chain restart kick #1
- `11:32:33`   chain restart kick #2
- `11:36:19`   chain restart kick #3
- `11:39:40` G1 FAIL phase=DISCOVER banked=0 catalog=0 failures=1
## G2 substance: BOP SDMX payload

- `11:39:40`   substance err: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist
- `11:39:40` G2 FAIL
## G3 card (post-mark)

- `11:43:41` G3 PASS note=FULL SDMX-2.1 warehouse (imf-full v1) on api.imf.org: 0/0 dataflows · 0 vintages retained · 0.00GB · 0 lastN-partial · phase DISCOVER · daily rediscovery
## DAY-TWO board (info)

- `11:43:41`   worldbank: phase=DRAIN banked=9200 q=20289
- `11:43:41`   gdelt: phase=DRAIN files=147513 gb=19.24 cursor=2019052819 gaps=2233 v1=0/None
- `11:43:41`   bls: phase=COMPLETE files=1659 gb=40.06
- `11:43:41`   dol: files=70 mb=160.6 fresh=0 unchanged=70
- `11:43:41` ops 4967 RED: G1; G2

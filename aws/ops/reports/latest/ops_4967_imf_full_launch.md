## G-1 markers-in-checkout

**Status:** success  
**Duration:** 1281.6s  
**Finished:** 2026-08-25T03:50:32+00:00  

## Data

| banked | catalog | failures | phase | vintages |
|---|---|---|---|---|
| 103 | 222 | 1 | DRAIN | 45 |

## Log
- `03:29:10`   ok justhodl-imf-full        'v1.0.2 ops4967'
- `03:29:10`   ok justhodl-provider-catalog 'imf-note-v2'
- `03:29:10`   ok justhodl-gov-sources     'imf-api-v2 ops4967'
## G0 settle x3

- `03:29:11`   justhodl-imf-full settled (0s)
- `03:29:11`   justhodl-provider-catalog settled (0s)
- `03:29:12`   justhodl-gov-sources settled (0s)
- `03:29:12` G0 PASS
## G0b schedules

- `03:29:12`   exists justhodl-imf-full-6h (ok)
- `03:29:12`   exists justhodl-imf-full-weekly (ok)
## G1 chain-drive (15min)

- `03:29:13`   t+   0s DRAIN banked=103 q=119 cat=222 fail=1
- `03:29:38`   t+  25s DRAIN banked=103 q=118 cat=222 fail=1
- `03:43:33`   chain restart kick #1
- `03:44:23` G1 PASS phase=DRAIN banked=103 catalog=222 failures=1
## G2 substance: BOP SDMX payload

- `03:44:28`   BOP raw=1866.07MB obs_tags=12219733
- `03:44:28` G2 PASS
## G3 card (post-mark)

- `03:50:31` G3 PASS note=FULL SDMX-2.1 warehouse (imf-full v1) on api.imf.org: 76/222 dataflows · 33 vintages retained · 24.96GB · 0 lastN-partial · phase DRAIN · daily rediscovery
## DAY-TWO board (info)

- `03:50:31`   worldbank: phase=DRAIN banked=9200 q=20289
- `03:50:32`   gdelt: phase=LIVE files=396316 gb=39.72 cursor=2026082503 gaps=7381 v1=0/0
- `03:50:32`   bls: phase=COMPLETE files=1659 gb=40.06
- `03:50:32`   dol: files=70 mb=160.6 fresh=0 unchanged=70
- `03:50:32` ops 4967 GREEN -- IMF full warehouse draining; daily rediscovery + weekly redrain own it; original drain queue resumes next (boe -> coinmetrics -> ...)

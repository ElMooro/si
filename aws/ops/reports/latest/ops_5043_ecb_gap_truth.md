## P0 live registry, parsed with ElementTree

**Status:** success  
**Duration:** 271.9s  
**Finished:** 2026-08-29T20:29:12+00:00  

## Data

| added | history_gaps | live | no_file | ours |
|---|---|---|---|---|
| 0 | 0 | 214 | 7 | 214 |

## Log
- `20:24:41`   /service/dataflow -> HTTP 200, 147052 bytes
- `20:24:41`   live dataflows: 214
- `20:24:41`   sample: ['AGR', 'AME', 'BKN', 'BLS', 'BNT', 'BOP', 'BSI', 'BSP', 'CAR', 'CBD', 'CBD2', 'CCP']
- `20:24:41`   our catalog: 214 flows, as_of 2026-08-18T19:34:38+00:00 (11 days old)
- `20:24:41`   ON THE PORTAL, NOT IN OUR CATALOG: 0  []
- `20:24:41`   in our catalog, no longer published: 0  []
## P1 flows grouped on the real delimiter

- `20:24:42`   574 objects, 2.56 GB, 265 distinct FLOWS
- `20:24:42`   CSEC             slices=37     886.2 MB  covers 1980..2025
- `20:24:42`   IVF              slices=8      240.3 MB  covers 2000..2035
- `20:24:42`   YC               slices=8      190.7 MB  covers 2000..2035
- `20:24:42`   BSI              slices=11     139.0 MB  covers 1980..2035
- `20:24:42`   HICP             slices=12     138.6 MB  covers 1900..2035
- `20:24:42`   SAFE             slices=7      130.8 MB  covers 2005..2035
- `20:24:42`   ICP              slices=11      78.4 MB  covers 1980..2035
- `20:24:42`   SEC              slices=9       70.4 MB  covers 1980..2022
- `20:24:42`   CBD2             slices=7       62.9 MB  covers 2005..2035
- `20:24:42`   SPF              slices=9       56.1 MB  covers 1995..2035
- `20:24:42`   CATALOGUED FLOWS WITH NO FILE: 7  ['DD', 'ECB.DISS:JVC_PUB', 'ECB.DISS:LCI_PUB', 'ECB.DISS:MOBILE_KEY_4', 'ECB.DISS:MOBILE_KEY_5', 'ECB.DISS:RESR_PUB', 'ECB.DISS:SUR_PUB']
- `20:24:42`   files with no catalog entry: 58 ['BLS.manifest.json', 'BOP.manifest.json', 'BP6.manifest.json', 'BPS.manifest.json', 'BSI.manifest.json', 'BSP.manifest.json', 'CBD.manifest.json', 'CBD2.manifest.json', 'CSEC.manifest.json', 'DWA.manifest.json', 'E09.manifest.json', 'E11.manifest.json']
- `20:24:42`   flows whose earliest slice starts after 1999: 25
- `20:24:42`   latest-starting: [('SHSS', 2020), ('PMC', 2020), ('SUP', 2015), ('SHS', 2010), ('SESFOD', 2010), ('PPC', 2010), ('STP', 2005), ('SAFE', 2005), ('ICB', 2005), ('FVC', 2005), ('DWA', 2005), ('CBD2', 2005)]
## P2 decisive test -- does the portal hold OLDER data?

- `20:26:22`   SHSS           probe err The read operation timed out
- `20:26:28`   PMC            ours start 2020 | pre-2020 query -> HTTP 200, 0 rows (none - we have it all)
- `20:26:33`   SUP            ours start 2015 | pre-2015 query -> HTTP 200, 0 rows (none - we have it all)
- `20:26:37`   SHS            ours start 2010 | pre-2010 query -> HTTP 200, 0 rows (none - we have it all)
- `20:26:38`   SESFOD         ours start 2010 | pre-2010 query -> HTTP 200, 0 rows (none - we have it all)
- `20:26:45`   PPC            ours start 2010 | pre-2010 query -> HTTP 200, 0 rows (none - we have it all)
- `20:26:46`   STP            ours start 2005 | pre-2005 query -> HTTP 200, 0 rows (none - we have it all)
- `20:28:27`   SAFE           probe err The read operation timed out
- `20:28:30`   ICB            ours start 2005 | pre-2005 query -> HTTP 200, 0 rows (none - we have it all)
- `20:28:32`   FVC            ours start 2005 | pre-2005 query -> HTTP 200, 0 rows (none - we have it all)
- `20:28:32`   flows with provable pre-coverage history: 0
## P3 distinct series, deduped across slices

- `20:28:43`   CSEC           22447 distinct series across its slices (3610418 rows read)
- `20:28:54`   IVF            89400 distinct series across its slices (3416076 rows read)
- `20:29:05`   YC             2164 distinct series across its slices (3646251 rows read)
- `20:29:12`   BSI            30095 distinct series across its slices (2246394 rows read)
- `20:29:12`   -> data/ops/ecb-gap-truth.json
- `20:29:12` ops 5043 GREEN -- ECB gap measured on correct parsers

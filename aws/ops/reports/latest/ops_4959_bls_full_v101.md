## P0 runner probe: BASE listing evidence

**Status:** success  
**Duration:** 1482.5s  
**Finished:** 2026-08-24T01:19:12+00:00  

## Data

| failures | files | gb | manifest_gb | phase | queue_left | surveys |
|---|---|---|---|---|---|---|
| 3 | 1095 | 35.33 | 35.33 | DRAIN | 549 | 65 |

## Log
- `00:54:30`   HTTP 200 uppercase-HREF=48 lowercase-href=0
- `00:54:30`   head: '<html><head><title>download.bls.gov - /pub/time.series/</title></head><body><H1>download.bls.gov - /pub/time.series/</H1><hr>\r \r <pre><A HREF="/pub/">[To Parent Directory]</A><br><br> 8/12/2026  8:30 AM        &lt;dir&gt; <A HREF="/pub/time.series/ap/">ap</A><'
## G-1 markers-in-checkout

- `00:54:30`   ok justhodl-bls-full            'v1.0.1 ops4959'
- `00:54:30`   ok justhodl-provider-catalog    'bls-note-v2'
## G0 settle (new-function fallback armed)

- `00:54:30`   justhodl-bls-full settled (0s)
- `00:54:30`   justhodl-provider-catalog settled (0s)
- `00:54:30` G0 PASS
## G0b schedule rate(12 hours)

- `00:54:30` G0b exists (ok)
## G1 chain-drive (22min budget; chains finish the rest)

- `00:54:31`   t+   0s DRAIN files=0 gb=0.00 q=0 fail=0
- `00:54:56`   t+  25s DRAIN files=0 gb=0.00 q=1656 fail=0
- `00:55:21`   t+  50s DRAIN files=0 gb=0.00 q=1620 fail=0
- `00:55:46`   t+  75s DRAIN files=0 gb=0.00 q=1605 fail=0
- `00:57:26`   t+ 175s DRAIN files=58 gb=3.37 q=1604 fail=0
- `00:57:51`   t+ 200s DRAIN files=59 gb=6.25 q=1595 fail=0
- `00:58:16`   t+ 225s DRAIN files=59 gb=6.25 q=1579 fail=1
- `00:59:32`   t+ 301s DRAIN files=59 gb=6.25 q=1578 fail=1
- `00:59:57`   t+ 326s DRAIN files=84 gb=10.72 q=1566 fail=1
- `01:00:22`   t+ 351s DRAIN files=84 gb=10.72 q=1564 fail=1
- `01:00:47`   t+ 376s DRAIN files=84 gb=10.72 q=1563 fail=1
- `01:01:12`   t+ 401s DRAIN files=98 gb=14.60 q=1526 fail=1
- `01:01:37`   t+ 426s DRAIN files=98 gb=14.60 q=1498 fail=1
- `01:02:02`   t+ 452s DRAIN files=98 gb=14.60 q=1467 fail=1
- `01:02:28`   t+ 477s DRAIN files=98 gb=14.60 q=1445 fail=1
- `01:03:18`   t+ 527s DRAIN files=217 gb=18.78 q=1444 fail=1
- `01:03:43`   t+ 552s DRAIN files=218 gb=22.15 q=1441 fail=1
- `01:04:08`   t+ 577s DRAIN files=218 gb=22.15 q=1431 fail=1
- `01:04:33`   t+ 602s DRAIN files=218 gb=22.15 q=1395 fail=1
- `01:04:58`   t+ 627s DRAIN files=218 gb=22.15 q=1376 fail=3
- `01:05:23`   t+ 652s DRAIN files=284 gb=26.00 q=1354 fail=3
- `01:05:48`   t+ 678s DRAIN files=284 gb=26.00 q=1319 fail=3
- `01:06:14`   t+ 703s DRAIN files=284 gb=26.00 q=1296 fail=3
- `01:06:39`   t+ 728s DRAIN files=284 gb=26.00 q=1252 fail=3
- `01:07:04`   t+ 753s DRAIN files=284 gb=26.00 q=1211 fail=3
- `01:07:29`   t+ 778s DRAIN files=284 gb=26.00 q=1172 fail=3
- `01:07:54`   t+ 803s DRAIN files=284 gb=26.00 q=1136 fail=3
- `01:08:19`   t+ 828s DRAIN files=284 gb=26.00 q=1104 fail=3
- `01:08:44`   t+ 853s DRAIN files=284 gb=26.00 q=1080 fail=3
- `01:09:09`   t+ 878s DRAIN files=580 gb=28.62 q=1049 fail=3
- `01:09:34`   t+ 904s DRAIN files=580 gb=28.62 q=1008 fail=3
- `01:10:00`   t+ 929s DRAIN files=580 gb=28.62 q=973 fail=3
- `01:10:25`   t+ 954s DRAIN files=580 gb=28.62 q=937 fail=3
- `01:10:50`   t+ 979s DRAIN files=580 gb=28.62 q=911 fail=3
- `01:11:15`   t+1004s DRAIN files=580 gb=28.62 q=889 fail=3
- `01:11:40`   t+1029s DRAIN files=780 gb=30.94 q=864 fail=3
- `01:12:05`   t+1054s DRAIN files=780 gb=30.94 q=846 fail=3
- `01:12:30`   t+1079s DRAIN files=780 gb=30.94 q=804 fail=3
- `01:12:55`   t+1104s DRAIN files=780 gb=30.94 q=770 fail=3
- `01:13:20`   t+1129s DRAIN files=780 gb=30.94 q=737 fail=3
- `01:13:46`   t+1155s DRAIN files=780 gb=30.94 q=697 fail=3
- `01:14:11`   t+1180s DRAIN files=780 gb=30.94 q=661 fail=3
- `01:14:36`   t+1205s DRAIN files=780 gb=30.94 q=626 fail=3
- `01:15:01`   t+1230s DRAIN files=780 gb=30.94 q=589 fail=3
- `01:15:26`   t+1255s DRAIN files=780 gb=30.94 q=565 fail=3
- `01:15:51`   t+1280s DRAIN files=1095 gb=35.33 q=559 fail=3
- `01:16:16`   t+1305s DRAIN files=1095 gb=35.33 q=549 fail=3
- `01:16:41` G1 PASS phase=DRAIN files=1095 gb=35.33 q=549 kicks=0
## G2 substance (deep AllData streaming proven)

- `01:16:41`   ap/ap.series head: 'series_id        \tarea_code\titem_code\tseries_title\tfootnote_codes\tbegin_year\tbegin_period\t'
- `01:16:41`   BIG cb/cb.series                       3878.4MB
- `01:16:41`   BIG ch/ch.data.0.Current               3369.7MB
- `01:16:41`   BIG ch/ch.data.1.AllData               3369.7MB
- `01:16:41`   BIG ca/ca.data.0.Current               2885.9MB
- `01:16:41`   BIG ca/ca.data.1.AllData               2885.9MB
- `01:16:41` G2 PASS big_files=36 series_maps=True
## G3 catalog card (post-mark)

- `01:19:12` G3 PASS keys=1156 note=FULL time.series warehouse (bls-full v1): 65 surveys · 1095 files · 35.33GB · phase DRAIN · complete history since 1913, conditional Last-Modified refresh
- `01:19:12` ops 4959 GREEN -- the BLS warehouse is draining since 1913; chains + rate(12h) finish and keep it fresh; day-two: phase COMPLETE + final GB + unchanged-proof

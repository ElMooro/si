## G-1 markers-in-checkout

**Status:** failure  
**Duration:** 1358.4s  
**Finished:** 2026-08-24T01:42:16+00:00  

## Error

```
SystemExit: 1
```

## Log
- `01:19:38`   ok justhodl-worldbank-full        'v1.0.0 ops4960'
- `01:19:38`   ok justhodl-provider-catalog      'wb-note-v2'
## G0 settle

- `01:19:38`   justhodl-worldbank-full settled (0s)
- `01:19:38`   justhodl-provider-catalog settled (0s)
- `01:19:38` G0 PASS
## G0b weekly redrain schedule

- `01:19:39` G0b created
## G1 chain-drive (20min; chains finish the rest)

- `01:19:39`   t+   0s None banked=0 mb=0.0 q=0 fail=0
- `01:20:04`   t+  25s DRAIN banked=0 mb=0.0 q=29474 fail=0
- `01:20:29`   t+  50s DRAIN banked=0 mb=0.0 q=29447 fail=0
- `01:20:54`   t+  75s DRAIN banked=0 mb=0.0 q=29421 fail=0
- `01:21:20`   t+ 100s DRAIN banked=0 mb=0.0 q=29396 fail=0
- `01:21:45`   t+ 125s DRAIN banked=0 mb=0.0 q=29370 fail=0
- `01:22:10`   t+ 150s DRAIN banked=0 mb=0.0 q=29343 fail=0
- `01:22:35`   t+ 176s DRAIN banked=0 mb=0.0 q=29319 fail=0
- `01:23:00`   t+ 201s DRAIN banked=0 mb=0.0 q=29292 fail=0
- `01:23:25`   t+ 226s DRAIN banked=0 mb=0.0 q=29266 fail=0
- `01:23:50`   t+ 251s DRAIN banked=0 mb=0.0 q=29240 fail=0
- `01:24:15`   t+ 276s DRAIN banked=0 mb=0.0 q=29216 fail=0
- `01:24:41`   t+ 301s DRAIN banked=0 mb=0.0 q=29189 fail=0
- `01:25:06`   t+ 326s DRAIN banked=0 mb=0.0 q=29163 fail=0
- `01:25:31`   t+ 352s DRAIN banked=0 mb=0.0 q=29137 fail=0
- `01:25:56`   t+ 377s DRAIN banked=0 mb=0.0 q=29111 fail=0
- `01:26:21`   t+ 402s DRAIN banked=0 mb=0.0 q=29085 fail=0
- `01:26:46`   t+ 427s DRAIN banked=0 mb=0.0 q=29059 fail=0
- `01:27:12`   t+ 452s DRAIN banked=0 mb=0.0 q=29033 fail=0
- `01:27:37`   t+ 477s DRAIN banked=0 mb=0.0 q=29009 fail=0
- `01:28:02`   t+ 502s DRAIN banked=0 mb=0.0 q=28988 fail=0
- `01:28:27`   t+ 528s DRAIN banked=0 mb=0.0 q=28963 fail=0
- `01:28:52`   t+ 553s DRAIN banked=0 mb=0.0 q=28944 fail=0
- `01:29:17`   t+ 578s DRAIN banked=0 mb=0.0 q=28924 fail=0
- `01:29:42`   t+ 603s DRAIN banked=0 mb=0.0 q=28903 fail=0
- `01:30:07`   t+ 628s DRAIN banked=0 mb=0.0 q=28884 fail=0
- `01:30:33`   t+ 653s DRAIN banked=0 mb=0.0 q=28865 fail=0
- `01:30:58`   t+ 678s DRAIN banked=628 mb=3.6 q=28847 fail=0
- `01:31:23`   t+ 703s DRAIN banked=628 mb=3.6 q=28827 fail=0
- `01:31:48`   t+ 729s DRAIN banked=628 mb=3.6 q=28807 fail=0
- `01:32:13`   t+ 754s DRAIN banked=628 mb=3.6 q=28787 fail=0
- `01:32:38`   t+ 779s DRAIN banked=628 mb=3.6 q=28767 fail=0
- `01:33:03`   t+ 804s DRAIN banked=628 mb=3.6 q=28747 fail=0
- `01:33:29`   t+ 829s DRAIN banked=628 mb=3.6 q=28727 fail=0
- `01:33:54`   t+ 854s DRAIN banked=628 mb=3.6 q=28707 fail=0
- `01:34:19`   t+ 880s DRAIN banked=628 mb=3.6 q=28686 fail=0
- `01:34:44`   t+ 905s DRAIN banked=628 mb=3.6 q=28666 fail=0
- `01:35:09`   t+ 930s DRAIN banked=628 mb=3.6 q=28645 fail=0
- `01:35:34`   t+ 955s DRAIN banked=628 mb=3.6 q=28625 fail=0
- `01:35:59`   t+ 980s DRAIN banked=628 mb=3.6 q=28605 fail=0
- `01:36:25`   t+1005s DRAIN banked=628 mb=3.6 q=28585 fail=0
- `01:36:50`   t+1030s DRAIN banked=628 mb=3.6 q=28565 fail=0
- `01:37:15`   t+1056s DRAIN banked=628 mb=3.6 q=28544 fail=0
- `01:37:40`   t+1081s DRAIN banked=628 mb=3.6 q=28524 fail=0
- `01:38:05`   t+1106s DRAIN banked=628 mb=3.6 q=28499 fail=0
- `01:38:30`   t+1131s DRAIN banked=628 mb=3.6 q=28474 fail=0
- `01:38:55`   t+1156s DRAIN banked=628 mb=3.6 q=28455 fail=0
- `01:39:21`   t+1181s DRAIN banked=628 mb=3.6 q=28433 fail=0
- `01:39:46` G1 FAIL phase=DRAIN banked=628 mb=3.6 q=28433 kicks=0
## G2 substance

- `01:39:46`   1.1_ACCESS.ELECTRICITY.TOT -> 3 files, head ok=False
- `01:39:46` G2 FAIL
## G3 catalog card (post-mark)

- `01:42:16` G3 PASS note=FULL indicator warehouse (worldbank-full v1): 1184/29490 indicators banked · 0 no-data · 0.01GB · phase DRAIN · official CSV-zips verbatim, weekly re-drain
- `01:42:16` ops 4960 RED: G1; G2

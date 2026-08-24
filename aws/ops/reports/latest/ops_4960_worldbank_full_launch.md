## G-1 markers-in-checkout

**Status:** success  
**Duration:** 704.8s  
**Finished:** 2026-08-24T01:59:02+00:00  

## Data

| banked | failures | mb | no_data | phase | queue_left |
|---|---|---|---|---|---|
| 1990 | 0 | 23.8 | 0 | DRAIN | 27500 |

## Log
- `01:47:17`   ok justhodl-worldbank-full        'v1.0.0 ops4960'
- `01:47:17`   ok justhodl-provider-catalog      'wb-note-v2'
## G0 settle

- `01:47:18`   justhodl-worldbank-full settled (0s)
- `01:47:18`   justhodl-provider-catalog settled (0s)
- `01:47:18` G0 PASS
## G0b weekly redrain schedule

- `01:47:18` G0b exists (ok)
## G1 chain-drive (20min; chains finish the rest)

- `01:47:18`   t+   0s DRAIN banked=1523 mb=14.5 q=27967 fail=0
- `01:47:43`   t+  25s DRAIN banked=1549 mb=14.9 q=27941 fail=0
- `01:48:09`   t+  50s DRAIN banked=1569 mb=15.2 q=27921 fail=0
- `01:48:34`   t+  75s DRAIN banked=1589 mb=15.7 q=27901 fail=0
- `01:48:59`   t+ 100s DRAIN banked=1611 mb=16.1 q=27879 fail=0
- `01:49:24`   t+ 125s DRAIN banked=1634 mb=16.4 q=27856 fail=0
- `01:49:49`   t+ 150s DRAIN banked=1657 mb=16.8 q=27833 fail=0
- `01:50:14`   t+ 175s DRAIN banked=1679 mb=17.1 q=27811 fail=0
- `01:50:39`   t+ 201s DRAIN banked=1700 mb=17.3 q=27790 fail=0
- `01:51:04`   t+ 226s DRAIN banked=1724 mb=17.7 q=27766 fail=0
- `01:51:30`   t+ 251s DRAIN banked=1747 mb=18.1 q=27743 fail=0
- `01:51:55`   t+ 276s DRAIN banked=1768 mb=18.4 q=27722 fail=0
- `01:52:20`   t+ 301s DRAIN banked=1790 mb=18.7 q=27700 fail=0
- `01:52:45`   t+ 326s DRAIN banked=1810 mb=18.9 q=27680 fail=0
- `01:53:10`   t+ 352s DRAIN banked=1830 mb=19.2 q=27660 fail=0
- `01:53:35`   t+ 377s DRAIN banked=1851 mb=19.5 q=27639 fail=0
- `01:54:01`   t+ 402s DRAIN banked=1873 mb=19.9 q=27617 fail=0
- `01:54:26`   t+ 427s DRAIN banked=1894 mb=20.2 q=27596 fail=0
- `01:54:51`   t+ 452s DRAIN banked=1920 mb=21.6 q=27570 fail=0
- `01:55:16`   t+ 477s DRAIN banked=1945 mb=22.6 q=27545 fail=0
- `01:55:41`   t+ 502s DRAIN banked=1970 mb=23.4 q=27520 fail=0
- `01:56:06`   t+ 527s DRAIN banked=1990 mb=23.8 q=27500 fail=0
- `01:56:31` G1 PASS phase=DRAIN banked=1990 mb=23.8 q=27500 kicks=0
## G2 substance (zip validity + any-member content)

- `01:56:31`   AG.LND.ARBL.HA.PC -> 3 members, content member=Metadata_Indicator_API_AG.LND.ARBL.HA.PC_DS2_en_csv_v2_34191.csv
- `01:56:31` G2 PASS
## G3 catalog card (post-mark)

- `01:59:02` G3 PASS note=FULL indicator warehouse (worldbank-full v1): 1796/29490 indicators banked · 0 no-data · 0.02GB · phase DRAIN · official CSV-zips verbatim, weekly re-drain
- `01:59:02` ops 4960 GREEN -- World Bank warehouse draining; chains + weekly redrain own it; day-two: COMPLETE + final GB

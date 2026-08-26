## P0 shape evidence (runner)

**Status:** failure  
**Duration:** 4597.2s  
**Finished:** 2026-08-26T20:55:26+00:00  

## Error

```
SystemExit: 1
```

## Log
- `19:38:49`   getMetadata MD11: 399164B head=b'{\n "STATUS":200,\n "MESSAGEID":"M181000I",\n "MESSAGE":"Successfully completed",\n "DATE":"2026-08-22T5:01:59.017+09:00",\n "DB":"MD11",\n "RESULTSET":[\n{\n "SERIES_CODE":"",\n "NAME_OF_TIME_SERIES":"Deposits, Vault Cash, and L'
- `19:38:49`   getDataCode err HTTP Error 400: Bad Request
## G0 settle

- `19:38:50`   settled (0s)
## G0 runner-deploys (event-blip fallback)

- `19:38:50`   justhodl-boj-full already carries v1.1.2 ops4987
- `19:38:50`   justhodl-provider-catalog already carries boj-note-v3
## P0b window ladder (single code)

- `19:38:50`   full -> HTTP Error 400: Bad Request
- `19:38:51`   20y (197001-198912) -> 3584B dates=True
- `19:38:52`   10y (198001-198912) -> 2144B dates=True
- `19:38:53`   5y (198501-198912) -> 1424B dates=True
- `19:38:54`   2y (198801-198912) -> 992B dates=True
- `19:38:54`   widest working window: 20y (engine CHUNK_Y=10)
## P1 sharded sync drive (6 lanes, 60min)

- `19:38:54`   dbs=22 ['BP01', 'BS01', 'BS02', 'FF', 'FM01', 'FM02', 'FM03', 'FM08', 'IR01', 'IR02', 'IR03', 'IR04', 'LA01', 'MD01', 'MD02', 'MD11', 'MD12', 'MD13', 'PR01', 'PR02', 'PR03', 'PS01']
- `19:39:40`   t+  45s series 43006/85102 parts=6815 live=2
- `19:40:26`   t+  91s series 43006/85102 parts=6815 live=2
- `19:41:11`   t+ 136s series 43126/85102 parts=6815 live=2
- `19:41:57`   t+ 182s series 43126/85102 parts=6815 live=2
- `19:42:42`   t+ 228s series 43126/85102 parts=6815 live=2
- `19:43:28`   t+ 273s series 43246/85102 parts=6815 live=2
- `19:44:14`   t+ 319s series 43246/85102 parts=6815 live=2
- `19:44:59`   t+ 365s series 43366/85102 parts=6815 live=2
- `19:45:45`   t+ 410s series 43366/85102 parts=6815 live=2
- `19:46:31`   t+ 456s series 43366/85102 parts=6815 live=2
- `19:47:17`   t+ 502s series 43486/85102 parts=6815 live=2
- `19:48:02`   t+ 548s series 43486/85102 parts=6815 live=2
- `19:48:48`   t+ 593s series 43606/85102 parts=6815 live=2
- `19:49:34`   t+ 639s series 43606/85102 parts=6815 live=2
- `19:50:19`   t+ 684s series 43666/85102 parts=6815 live=2
- `19:51:05`   t+ 730s series 43726/85102 parts=6815 live=2
- `19:51:51`   t+ 776s series 43726/85102 parts=6815 live=2
- `19:52:37`   t+ 822s series 43846/85102 parts=6815 live=2
- `19:53:22`   t+ 867s series 43846/85102 parts=6815 live=2
- `19:54:08`   t+ 913s series 43906/85102 parts=6815 live=2
- `19:54:53`   t+ 959s series 43966/85102 parts=6815 live=2
- `19:55:40`   t+1005s series 43966/85102 parts=6815 live=2
- `19:56:25`   t+1050s series 44086/85102 parts=6815 live=2
- `19:57:11`   t+1096s series 44086/85102 parts=6815 live=2
- `19:57:56`   t+1142s series 44206/85102 parts=6815 live=2
- `19:58:42`   t+1187s series 44206/85102 parts=6815 live=2
- `19:59:28`   t+1233s series 44206/85102 parts=6815 live=2
- `20:00:13`   t+1278s series 44326/85102 parts=6815 live=2
- `20:00:59`   t+1324s series 44326/85102 parts=6815 live=2
- `20:01:44`   t+1370s series 44326/85102 parts=6815 live=2
- `20:02:30`   t+1415s series 44446/85102 parts=6815 live=2
- `20:03:16`   t+1461s series 44446/85102 parts=6815 live=2
- `20:04:02`   t+1507s series 44566/85102 parts=6815 live=2
- `20:04:47`   t+1552s series 44566/85102 parts=6815 live=2
- `20:05:33`   t+1598s series 44566/85102 parts=6815 live=2
- `20:06:18`   t+1644s series 44686/85102 parts=6815 live=2
- `20:07:04`   t+1689s series 44686/85102 parts=6815 live=2
- `20:07:50`   t+1735s series 44806/85102 parts=6815 live=2
- `20:08:36`   t+1781s series 44806/85102 parts=6815 live=2
- `20:09:21`   t+1826s series 44806/85102 parts=6815 live=2
- `20:10:07`   t+1872s series 44926/85102 parts=6815 live=2
- `20:10:53`   t+1918s series 44926/85102 parts=6815 live=2
- `20:11:38`   t+1963s series 45046/85102 parts=6815 live=2
- `20:12:24`   t+2009s series 45046/85102 parts=6815 live=2
- `20:13:09`   t+2055s series 45046/85102 parts=6815 live=2
- `20:13:55`   t+2100s series 45166/85102 parts=6815 live=2
- `20:14:41`   t+2146s series 45166/85102 parts=6815 live=2
- `20:15:26`   t+2192s series 45226/85102 parts=6815 live=2
- `20:16:12`   t+2237s series 45406/85102 parts=6945 live=2
- `20:16:58`   t+2283s series 45586/85102 parts=6969 live=2
- `20:17:43`   t+2328s series 45646/85102 parts=6969 live=2
- `20:18:29`   t+2374s series 45706/85102 parts=7006 live=2
- `20:19:14`   t+2420s series 45706/85102 parts=7006 live=2
- `20:20:00`   t+2465s series 45826/85102 parts=7006 live=2
- `20:20:46`   t+2511s series 45826/85102 parts=7006 live=2
- `20:21:31`   t+2557s series 45886/85102 parts=7006 live=2
- `20:22:17`   t+2602s series 45946/85102 parts=7006 live=2
- `20:23:03`   t+2648s series 45946/85102 parts=7006 live=2
- `20:23:48`   t+2694s series 46066/85102 parts=7006 live=2
- `20:24:34`   t+2739s series 46066/85102 parts=7006 live=2
- `20:25:20`   t+2785s series 46126/85102 parts=7006 live=2
- `20:26:05`   t+2831s series 46186/85102 parts=7006 live=2
- `20:26:51`   t+2877s series 46186/85102 parts=7006 live=2
- `20:27:37`   t+2922s series 46306/85102 parts=7006 live=2
- `20:28:23`   t+2968s series 46306/85102 parts=7006 live=2
- `20:29:08`   t+3014s series 46426/85102 parts=7006 live=2
- `20:29:54`   t+3059s series 46426/85102 parts=7006 live=2
- `20:30:40`   t+3105s series 46486/85102 parts=7006 live=2
- `20:31:25`   t+3151s series 46546/85102 parts=7006 live=2
- `20:32:11`   t+3196s series 46546/85102 parts=7006 live=2
- `20:32:57`   t+3242s series 46666/85102 parts=7006 live=2
- `20:33:42`   t+3287s series 46666/85102 parts=7006 live=2
- `20:34:28`   t+3333s series 46726/85102 parts=7006 live=2
- `20:35:14`   t+3379s series 46786/85102 parts=7006 live=2
- `20:35:59`   t+3424s series 46786/85102 parts=7006 live=2
- `20:36:45`   t+3470s series 46906/85102 parts=7006 live=2
- `20:37:30`   t+3516s series 46906/85102 parts=7006 live=2
- `20:38:16`   t+3561s series 46966/85102 parts=7006 live=2
- `20:39:02`   t+3607s series 47026/85102 parts=7006 live=2
- `20:39:47`   t+3653s series 47026/85102 parts=7006 live=2
- `20:40:33`   t+3698s series 47146/85102 parts=7006 live=2
- `20:41:19`   t+3744s series 47146/85102 parts=7006 live=2
- `20:42:04`   t+3789s series 47206/85102 parts=7006 live=2
- `20:42:50`   t+3835s series 47266/85102 parts=7006 live=2
- `20:43:36`   t+3881s series 47266/85102 parts=7006 live=2
- `20:44:21`   t+3926s series 47386/85102 parts=7006 live=2
- `20:45:07`   t+3972s series 47386/85102 parts=7006 live=2
- `20:45:53`   t+4018s series 47446/85102 parts=7006 live=2
- `20:46:38`   t+4064s series 47506/85102 parts=7006 live=2
- `20:47:24`   t+4109s series 47506/85102 parts=7006 live=2
- `20:47:24`     BP01 fail: HTTP 500 @2360 195501
- `20:47:25`     PR02 fail: HTTP 500 @520 196501
- `20:47:25`     PR03 fail: HTTP 500 @9240 196501
- `20:47:25`   remainder: ['BP01 7740/17989', 'FF 6540/33887']
- `20:47:25` P1 PASS dbs=22 series=47506/85102 parts=7006
## G2 substance

- `20:47:25`   substance err Parameter validation failed:
Invalid type for parameter Key, value: None, type: <class 'NoneType'>, valid type
- `20:47:25` G2 FAIL
## G3 card

- `20:55:26` G3 note=FULL flat-file warehouse (boj-full v1): 16/16 database zips · 19MB · the entire time-series portal · API universe: 22 dbs · 47626/120394 series · 7006 parts
- `20:55:26` ops 4987 RED: REMAINDER(2 dbs); G2

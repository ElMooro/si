## expected values from live census state

**Status:** success  
**Duration:** 153.2s  
**Finished:** 2026-08-23T17:35:45+00:00  

## Data

| datasets | failures | keys | note | rows |
|---|---|---|---|---|
| 51/56 | 5 | 325 | full timeseries universe since inception: 51/56 datasets · 18 families · 3,909,472 rows · phase COMPLETE · excluded by design: idb 2, intltrade 36 (intltrade -> import-canary) · 5  | 3909472 |

## Log
- `17:33:12`   state: 51/56 datasets · 3,909,472 rows · 5 failures · phase COMPLETE
## G-1 marker-in-source self-check

- `17:33:12` G-1 PASS marker present in checkout
## G0 zip-settle census-note-v2

- `17:33:13` G0 PASS after 0s
## G1 invoke catalog + fresh stamp

- `17:33:44`   t+  30s stamp unchanged (2026-08-23T16:48:53)
- `17:34:14`   t+  60s stamp unchanged (2026-08-23T16:48:53)
- `17:34:44`   t+  90s stamp unchanged (2026-08-23T16:48:53)
- `17:35:14`   t+ 121s stamp unchanged (2026-08-23T16:48:53)
- `17:35:45` G1 PASS stamp 2026-08-23T16:48:53 -> 2026-08-23T17:33:14 after 151s
## G2 census card contract

- `17:35:45`   keys=325 note=full timeseries universe since inception: 51/56 datasets · 18 families · 3,909,472 rows · phase COMPLETE · excluded by design: idb 2, intltrade 36 (intltrade -> import-canary) · 5 structurally-named source failures
- `17:35:45` G2 PASS missing=[] fossil=False keys_ok=True
## G3 origin

- `17:35:45` G3 PASS
- `17:35:45` ops 4952 GREEN -- census card now composed from state truth; the fossil template is gone and the numbers match the live walker

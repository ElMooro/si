## G-1 markers-in-checkout

**Status:** success  
**Duration:** 390.4s  
**Finished:** 2026-08-23T17:49:47+00:00  

## Data

| fresh | freshest_h | haircuts | mirrored | new_harvested | sources | unchanged |
|---|---|---|---|---|---|---|
| 429 | 0.0 | {"tri-party-repo_data_current.xlsx": {"status": "fresh", "bytes": 217243}, "tri-party-repo_preNov25_history.xlsx": {"status": "fresh", "bytes": 439002}} | 429 | 0 | 431 | 0 |

## Log
- `17:43:17`   ok justhodl-src-mirror          'nyfed-research lane ops 4953'
- `17:43:17`   ok justhodl-provider-catalog    'nyfed-note-v2'
## G0 zip-settle both engines

- `17:43:43`   justhodl-src-mirror settled after 26s
- `17:43:43`   justhodl-provider-catalog settled after 0s
- `17:43:43` G0 PASS
## G1 src-mirror run (Event + state poll)

- `17:44:14`   t+  30s waiting (as_of=2026-08-23T05:05:49)
- `17:44:44`   t+  60s waiting (as_of=2026-08-23T05:05:49)
- `17:45:14`   t+  90s waiting (as_of=2026-08-23T05:05:49)
- `17:45:44`   t+ 120s waiting (as_of=2026-08-23T05:05:49)
- `17:46:15`   t+ 151s waiting (as_of=2026-08-23T05:05:49)
- `17:46:45`   t+ 181s waiting (as_of=2026-08-23T05:05:49)
- `17:47:15` G1 PASS lane={"haircuts": {"tri-party-repo_data_current.xlsx": {"status": "fresh", "bytes": 217243}, "tri-party-repo_preNov25_history.xlsx": {"status": "fresh", "bytes": 439002}}, "mirrored": 429, "fresh": 429, "unchanged": 0, "errors": 0, "new_harvested": 0} lastcheck={sources:431 at:2026-08-23T17:47:05}
## G2 provider-catalog nyfed card

- `17:47:46`   t+  30s stamp unchanged
- `17:48:16`   t+  60s stamp unchanged
- `17:48:46`   t+  90s stamp unchanged
- `17:49:17`   t+ 120s stamp unchanged
- `17:49:47` G2 PASS freshest_h=0.0 note=src-mirror daily since ops 4953: 431 sources · 429 fresh / 0 unchanged · haircut workbooks fresh/fresh · parsed haircuts-series re-transform = phase 2 (seed ops 4793-94)
## G3 origin

- `17:49:47` G3 PASS
- `17:49:47` ops 4953 GREEN -- the last audit orphan has a real import loop; nyfed-research refreshes daily beside the OFR lanes, phase-2 re-transforms stated in refresh-orphans

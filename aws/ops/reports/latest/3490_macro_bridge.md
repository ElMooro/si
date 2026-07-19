# ops 3490 — macro overlays via wl-series bridge

**Status:** success  
**Duration:** 145.7s  
**Finished:** 2026-07-19T01:44:28+00:00  

## Log
- `01:42:05` resolved id map: {"US10Y": {"id": "TVC:US10Y", "n": 1906, "last": ["2026-28", 4.54]}, "US02Y": {"id": "TVC:US02Y", "n": 1906, "last": ["2026-28", 4.16]}, "HYOAS": {"id": "FRED:BAMLH0A0HYM2", "n": 157, "last": ["2026-28", 2.7]}, "FEDFUNDS": {"id": "FRED:FEDFUNDS", "n": 438, "last": ["2026-23", 3.63]}, "UNRATE": {"id": "FRED:UNRATE", "n": 437, "last": ["2026-23", 4.2]}, "DXY": {"id": "TVC:DXY", "n": 1906, "last": ["2026-28", 101.11499786376953]}}
- `01:42:05` PASS  X1_bridge_probe — {'resolved': {'US10Y': 'TVC:US10Y', 'US02Y': 'TVC:US02Y', 'HYOAS': 'FRED:BAMLH0A0HYM2', 'FEDFUNDS': 'FRED:FEDFUNDS', 'UNRATE': 'FRED:UNRATE', 'DXY': 'TVC:DXY'}, 'missed': ['T10Y2Y', 'CPIYOY'], 'sample_last': {'US10Y': ['2026-28', 4.54], 'US02Y': ['2026-28', 4.16], 'HYOAS': ['2026-28', 2.7]}}
- `01:44:28` PASS  X2_served_js — {'node_ok': [True, True, True, True]}
- `01:44:28` PASS  X3_flagship_v23 — {'ops3490': True, 'mxbtn': True, 'registry': True, 'chip_x': True, 'evt_intact': True, 'rt_intact': True, 'whales_intact': True, 'flags_intact': True}
# RESULT: ALL PASS


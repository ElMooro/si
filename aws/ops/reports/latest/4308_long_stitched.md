# ops 4308 -- rehypo barometer + 1996 long history

**Status:** success  
**Duration:** 10.0s  
**Finished:** 2026-08-03T01:34:25+00:00  

## Data

| latest | leg | n | z |
|---|---|---|---|
| 175175.0 | fails | 104 | -0.4 |
| 4.64 | velocity | 104 | 0.68 |
| 6.0 | specialness | 104 | -0.68 |
| 2816966347720. | dvp_volume | 104 | -0.67 |
| 0.0 | sofr_iorb | 104 | 0.66 |
| -0.02 | rrp_drain_4w | 104 | 0.02 |

## Log
- `01:34:15` function: fresh+keyed
- `01:34:25` run: {"ok": true, "composite": 50.6, "band": "WATCH", "legs": ["fails", "velocity", "specialness", "dvp_volume", "sofr_iorb", "rrp_drain_4w"], "missing": []}
- `01:34:25` COMPOSITE 50.6 (WATCH)
- `01:34:25` missing: None
- `01:34:25` note: nyfed catalog: 1539 keyids
- `01:34:25` note: picked: {"fails": 6, "sec_in": 8, "sec_out": 8, "net_pos": 8}
- `01:34:25` long_history: {}
- `01:34:25` ✅ LONG: 2020-03-09 -> 2026-07-31 · 1600 weekly pts · legs_from={'fails': '2024-07-03', 'sofr_iorb': '2021-07-29', 'rrp_drain_4w': '2008-09-24'}
- `01:34:25` era_coverage: {'fails': {'SBN2024': 108}, 'financing_in': {'SBN2024': 108}, 'financing_out': {'SBN2024': 108}, 'positions': {}}
- `01:34:25` sample: [('2020-03-09', 46.8, 1), ('2021-06-17', 3.9, 1), ('2022-09-27', 34.7, 2), ('2024-01-10', 47.2, 2), ('2025-04-21', 51.1, 2)]
## RESULT

- `01:34:25` ✗   actual_start 2020-03-09 later than 2014 -- even proven-keyid segments unreachable

# ops 3489 — congress + insider events on the tape

**Status:** success  
**Duration:** 155.8s  
**Finished:** 2026-07-19T00:38:11+00:00  

## Log
- `00:35:36` PASS  V1_parser_units — {'congress': [['2026-03-15', 'Jane Doe', 'B', '$15,001 - $50,000'], ['2026-04-02', 'John Roe', 'S', '$1,001 - $15,000']], 'insiders': [['2026-05-01', 'Tim C (CEO)', 'B', 2500000.0], ['2026-06-01', 'SELL CLUSTER 4 insiders (30d)', 'S', 9000000.0]]}
- `00:35:36`   zip: 98475 bytes
## 1. Lambda

- `00:35:36`   Lambda exists — updating
- `00:35:40` ✅   ✓ updated justhodl-fundamental-graphs
- `00:35:47` PASS  V2_realdata_selfselect — {'symbol': 'WAB', 'congress_n': 1, 'insiders_n': 0, 'sample': [['2026-06-09', 'Thomas H Tuberville', 'S', '$1,001 - $15,000']], 'picked_filer': 'Thomas H Tuberville'}
- `00:38:11` PASS  V3_served_js — {'node_ok': [True, True, True, True]}
- `00:38:11` PASS  V4_surfaces — {'flag_ops3489': True, 'evtbtn': True, 'rt_intact': True, 'whales_intact': True, 'flags_intact': True, 'marks_intact': True, 'why_evt': True, 'tdz_intact': True}
# RESULT: ALL PASS


# ops 3513 — equity-FTD family (SEC CNS)

**Status:** success  
**Duration:** 267.8s  
**Finished:** 2026-07-19T16:11:59+00:00  

## Log
- `16:07:31` PASS  L1_ci_battery — {'parse': True, 'spike': True, 'floors_etf': True, 'few_priors': True}
- `16:07:32`   zip: 83423 bytes
## 1. Lambda

- `16:07:32`   Lambda missing — creating
- `16:07:37` ✅   ✓ created justhodl-equity-ftd
- `16:07:56` FAIL  L2_live_run — {'files': ['202606b', '202606a', '202604b', '202604a', '202603b', '202603a'], 'universe_n': 13707, 'n_candidates': 252, 'top_dollars_3': [('GOOG', 83557.25), ('GOOGL', 67455.76), ('AMD', 59448.28)], 'top_spikes_3': [('EME', 33910.86, 5.09), ('IVZ', 17528.29, 6.71), ('SPCX', 13372.21, 0.03)], 'qualifiers': [{'t': 'EME', 'q': 2758083, 'usd': 2201.23, 'peak_day_q': 2757895, 'spike': 33910.86, 'avg20': 542270.0, 'px': 744.16, 'dtc_peak': 5.09, 'desc': 'EMCOR GROUP INC'}, {'t': 'IVZ', 'q': 37366809, 
- `16:07:57` PASS  L3_schedule — {'name': 'equity-ftd-sched', 'expr': 'cron(10 15 ? * MON,THU *)'}
- `16:11:59` PASS  L4_families_page — {'card': True, 'feed': True, 'node': True}
# RESULT: FAILS: ['L2_live_run']


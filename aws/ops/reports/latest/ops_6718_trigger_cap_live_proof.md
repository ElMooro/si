# ops 6718 - the trigger cap on the real account: deployed code, the ledger, the shutdown and the manual release

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-11-03T06:33:35+00:00  

## Log
## A. deployed code

- `06:33:29` ✅ justhodl-event-coordinator: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T05:41:16
- `06:33:29` ✅ justhodl-spend-governor: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T06:30:48
- `06:33:30` ✅ justhodl-a2a-bus: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T05:43:18
- `06:33:30` ✅ justhodl-tv-bars: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T06:07:47
- `06:33:31` ✅ justhodl-calibrator: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T05:46:49
- `06:33:32` ✅ justhodl-symdir: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T06:06:16
- `06:33:32` ✅ justhodl-equity-prewarm: chain_guard v2 in zip True | source uses it True | modified 2026-11-03T05:49:18
- `06:33:33` ✅ governor version 1.6.1 stage NORMAL mode enforce
## B. the ledger, for real

- `06:33:33` hop 1 True | hop 2 True path ['justhodl-ops6718-root', 'justhodl-ops6718-a', 'justhodl-ops6718-b']
- `06:33:33` ✅ 2 hops admitted, the revisit refused as a loop (not reported): loop
- `06:33:33` ✅ DynamoDB: chain counter n=2, engines ['justhodl-ops6718-a', 'justhodl-ops6718-b'], expires in 72 h
## C. the shutdown on the canary, end to end

- `06:33:33` canary reserved concurrency before: None
- `06:33:33` admit at the depth cap: allow False reason max_hops hold ['justhodl-spend-governor-canary'] breach_id breach#20261103#justhodl-spend-governor-canary#justhodl-ops6718-target#max_hops (0.0 s incl. the governor call)
- `06:33:33` record status pending - asking the governor directly (the engine-side call may have found it busy)
- `06:33:34` governor: {"breach_id": "breach#20261103#justhodl-spend-governor-canary#justhodl-ops6718-target#max_hops", "status": "applied", "held": {"justhodl-spend-governor-canary": "held ({\"applied\": 1})"}}
- `06:33:34` ✅ SHUT DOWN: canary reserved 0 | record applied | ledger stage CAP operator_hold True trigger_breach breach#20261103#justhodl-spend-governor-canary#justhodl-ops6718-target#max_hops | capped now ['justhodl-event-coordinator', 'justhodl-miss-calibrator', 'justhodl-spend-governor-canary', 'ofrapi']
- `06:33:34` ✅ trigger-breach event records: ['spend-governor/events/20261103T063333.566889Z-trigger-breach.json']
- `06:33:34` governor-alerts dispatch not possible from here (CalledProcessError) - the hourly run opens the issue
- `06:33:35` ✅ RELEASED by release_cap: {"function": "justhodl-spend-governor-canary", "status": "restored", "mtd_at_release": 0.0} | canary reserved now None (was None)
- `06:33:35` ✅ a second breach the same day for the same rule is not re-reported (record breach#20261103#justhodl-spend-governor-canary#justhodl-ops6718-target#max_hops)
- `06:33:35` ✅ ops 6718 complete

# ops 6521 - gated un-quarantine of the event loop pair

**Status:** success  
**Duration:** 606.9s  
**Finished:** 2026-10-16T20:14:41+00:00  

## Log
- `20:04:36` ✅ gate coordinator.is_self_loop
- `20:04:36` ✅ gate coordinator.audit_cap
- `20:04:36` ✅ gate calibrator.debounce
- `20:04:36` ✅ gate calibrator.no_reemission
- `20:04:37` ✅ justhodl-miss-calibrator restored: reserved=None eic=None
- `20:04:37` ✅ justhodl-event-coordinator restored: reserved=None eic=None
- `20:14:41` justhodl-miss-calibrator invocations/min after restore: []
- `20:14:41` justhodl-event-coordinator invocations/min after restore: []
- `20:14:41` ✅ quarantine lifted; 10-minute watch clean: {"justhodl-miss-calibrator": null, "justhodl-event-coordinator": null}

# ops 3442 — regime bridge + stealth + fade v1.1

**Status:** success  
**Duration:** 366.5s  
**Finished:** 2026-07-18T03:19:56+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:16:16` PASS  G1_settled — 4 markers incl label-stamp in bundled shared
- `03:19:53` FAIL  G2_bridge_e2e — on_row=TESTREGIME3442 table=None
- `03:19:54` PASS  G3_stealth_live — sched=created recent=0 logged=0 rows=[]
- `03:19:56` PASS  G4_fade_selfupdating — families_from_triage=['eng:ai-infra-stack', 'eng:finnhub-signals']
- `03:19:56` VERDICT: GAPS: G2_bridge_e2e

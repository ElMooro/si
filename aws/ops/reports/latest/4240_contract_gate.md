# ops 4240 — output contracts

**Status:** success  
**Duration:** 119.0s  
**Finished:** 2026-08-01T15:15:22+00:00  

## Data

| artifact | cls | contracts | detail | section | sev1 | violations |
|---|---|---|---|---|---|---|
|  |  | 866 |  | contracts | 2 | 2 |
| data/dependency-graph.json | ROW_COLLAPSE |  | page_reads.risk-regime.html has None rows, contract floor is 36 (learned 52) — the engine ran but produced a f | violation |  |  |
| data/fleet-audit.json | ROW_COLLAPSE |  | engine_shared_outs.data/auction-crisis.json has None rows, contract floor is 15 (learned 22) — the engine ran  | violation |  |  |

## Log
## 1. Deploy

- `15:13:29` ✅ updated
- `15:13:30` ✅ GATE 1 zip marker verified
## 2. GATE 2 — learn contracts from current state

- `15:14:29` learn -> {"ok": true, "mode": "learn", "n_contracts": 866, "elapsed_s": 54.0}
- `15:14:29` ✅ 866 contracts learned
## 3. GATE 3/4 — check, and show the sev-1s

- `15:15:06` ✅ check -> {"ok": true, "mode": "check", "n_contracts": 866, "n_violations": 2, "sev1": 2, "by_class": {"ROW_COLLAPSE": 2}, "elapsed_s": 36.8}
- `15:15:06` artifacts live=867 contracted=866 uncontracted=1
- `15:15:06` SEV-1 VIOLATIONS (2):
- `15:15:06` ✗    ROW_COLLAPSE   data/dependency-graph.json                 page_reads.risk-regime.html has None rows, contract floor is 36 (learned 52) — the engine 
- `15:15:06` ✗    ROW_COLLAPSE   data/fleet-audit.json                      engine_shared_outs.data/auction-crisis.json has None rows, contract floor is 15 (learned 2
## 4. GATE 5/6 — schedule + alarm

- `15:15:07` ✅ schedule cron(0 13 * * ? *) -> 1 target
- `15:15:08` ✅ alarm justhodl-contract-sev1 armed
## 5. GATE 7 — declare it in the manifest

- `15:15:08` ✅ manifest now declares justhodl-contract-gate-daily (447 rules total)
- `15:15:22` ✅ reconciler drift after declaring: 0
## RESULT

- `15:15:22` ✅ OPS 4240 PASS — 866 artifacts now have an asserted shape, checked daily.

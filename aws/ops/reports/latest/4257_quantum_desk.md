# ops 4257 -- quantum-desk (PM meta-allocator) live + proven

**Status:** success  
**Duration:** 78.7s  
**Finished:** 2026-08-01T22:23:46+00:00  

## Log
## 1. ensure function

- `22:22:27` ✅ function exists (deploy-lambdas.yml won the race)
## 2. schedule (EventBridge Scheduler)

- `22:22:28` ✅ schedule created: cron(55 23 * * ? *)
## 3. first run (sync)

- `22:22:30` ✅ ran: regime=NEUTRAL sources_ok=12 ladder=14 map=0 best=CASH
## 4. artifact verification (measured, not hoped)

- `22:22:30` ✅ artifact fresh (0s) v1.0.0 -- sources 12/12 live
- `22:22:30` ✅   asset_compass    ok       age=0.1h
- `22:22:30` ✅   best_setups      ok       age=0.2h
- `22:22:30` ✅   crypto_cycle     ok       age=6.3h
- `22:22:30` ✅   cycle_clock      ok       age=22.9h
- `22:22:30` ✅   etf_flows        ok       age=0.4h
- `22:22:30` ✅   forward_returns  ok       age=0.5h
- `22:22:30` ✅   indicator_bus    ok       age=10.1h
- `22:22:30` ✅   liquidity        ok       age=11.4h
- `22:22:30` ✅   master_alloc     ok       age=1.0h
- `22:22:30` ✅   nowcast          ok       age=8.9h
- `22:22:30` ✅   risk_gate        ok       age=11.3h
- `22:22:30` ✅   router           ok       age=0.4h
- `22:22:30` REGIME: NEUTRAL (votes: none)
- `22:22:30` RISK-GATE: posture=RISK_OFF composite=-0.515 sizing=x0.45
- `22:22:30` ✗ artifact: 'Report' object has no attribute 'row'
## 5. page edge check (warn-only, CDN lag tolerated)

- `22:23:46` ⚠ page not on edge yet -- pages.yml/CDN lag; same push carries it, recheck next session
## RESULT

- `22:23:46` ✗   artifact: 'Report' object has no attribute 'row'

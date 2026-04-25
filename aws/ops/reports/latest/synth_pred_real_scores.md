# Step 75 — synth_pred derives ml_risk + carry_risk from real signals

**Status:** success  
**Duration:** 3.8s  
**Finished:** 2026-04-25T00:13:59+00:00  

## Data

| carry_risk_source | ml_risk_source | rationale |
|---|---|---|
| repo-data.stress.score | edge-data.composite_score | defensible mappings — both real signals already computed elsewhere |

## Log
- `00:13:55` ✅   Updated exec_summary to include risk_score from edge composite
- `00:13:55` ✅   Updated synth_pred return — carry_trade.risk_score from plumbing stress
- `00:13:55` ✅   Source valid (40909 bytes), saved
- `00:13:59` ✅   Deployed justhodl-intelligence (11,713 bytes)
## Trigger fresh intelligence + signal-logger runs

- `00:13:59` ✅   intelligence triggered (status 202)
- `00:13:59` Done

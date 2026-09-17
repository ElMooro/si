# ops 5637 -- the owned read's release blockers

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-09-17T19:12:19+00:00  

## Data

| calls | coercions | decision_status | fleet_inputs_status | fleet_summary | read_id | release_blockers | repaired | status | summary | voice |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | null | ADVISORY_ONLY |  |  | 20260917T190906Z |  | False |  |  | owned |
|  |  |  |  |  |  |  |  | BLOCKED | {"declared": 81, "unique_feeds": 80, "eligible": 54, "private_excluded": 8, "schema_versioned": 0, "by_status": {"FRESH": 57, "DEAD": 7, "UNTIMESTAMPED": 10, "STALE": 6}} |  |
|  |  |  | BLOCKED | {"declared": 81, "unique_feeds": 80, "eligible": 54, "private_excluded": 8, "schema_versioned": 0, "by_status": {"FRESH": 57, "DEAD": 7, "UNTIMESTAMPED": 10, "STALE": 6}} |  | null |  |  |  |  |

## Log
## 1. Read state

## 2. Release blockers (why calls are muted)

- `19:12:18` ⚠ fleet registry is not READY
- `19:12:18` ⚠ registry version must equal 2
- `19:12:18` ⚠ registry has 80 unique feeds; minimum is 150
- `19:12:18` ⚠ eligible fleet coverage 67.5% is below 80%
## 3. Board sources

- `19:12:18` bonds                  FRESH    age_h=1.5     data/bond-warroom.json
- `19:12:18` bottom                 FRESH    age_h=15.3    data/bottom.json
- `19:12:18` brain                  FRESH    age_h=139.4   data/brain.json
- `19:12:18` btc_cycle              FRESH    age_h=3.1     data/crypto-cycle-risk.json
- `19:12:18` crisis                 FRESH    age_h=0.9     data/crisis-composite.json
- `19:12:18` crypto                 FRESH    age_h=0.0     crypto-intel.json
- `19:12:18` fortress               FRESH    age_h=15.6    data/fortress.json
- `19:12:18` fusion                 FRESH    age_h=0.2     data/jh-fusion.json
- `19:12:18` gbc                    FRESH    age_h=7.1     data/global-business-cycle.json
- `19:12:18` katlin                 FRESH    age_h=0.0     data/katlin.json
- `19:12:18` khalid_risk            FRESH    age_h=0.8     data/khalid-risk.json
- `19:12:18` metals                 FRESH    age_h=5.0     screener/metals-miners.json
- `19:12:18` regime_composite       FRESH    age_h=1.3     data/regime-composite.json
- `19:12:18` risk_gate              FRESH    age_h=0.5     data/risk-gate.json
- `19:12:18` scorecard              FRESH    age_h=8.6     data/signal-scorecard.json
## 4. Fleet registry summary (fleet_inputs)

- `19:12:19` ✅ probe complete

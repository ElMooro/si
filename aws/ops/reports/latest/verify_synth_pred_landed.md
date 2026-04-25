# Verify ml_risk + carry_risk now have real values

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-04-25T00:15:25+00:00  

## Data

| carry_risk_score | ml_risk_score | scores_real |
|---|---|---|
| 25 | 60 | 5/5 |

## Log
## Source data sanity check

- `00:15:25`   edge-data.json composite_score: 60 (2.2h old)
- `00:15:25`   repo-data.json stress: score=25 status=ELEVATED
## intelligence-report.json scores

- `00:15:25`   Age: 1.4 min, size: 4,449 bytes
- `00:15:25`   scores: {
    "khalid_index": 43,
    "crisis_distance": 60,
    "plumbing_stress": 25,
    "ml_risk_score": 60,
    "carry_risk_score": 25,
    "vix": 19.31,
    "move": null
}
- `00:15:25`   ✓ khalid_index = 43
- `00:15:25`   ✓ plumbing_stress = 25
- `00:15:25`   ✓ ml_risk_score = 60
- `00:15:25`   ✓ carry_risk_score = 25
- `00:15:25`   ✓ vix = 19.31
- `00:15:25` ✅   ✅ 5/5 critical scores are real values
- `00:15:25` Done

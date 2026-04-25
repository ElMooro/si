# Verify intelligence-report.json now has real values

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-25T00:06:09+00:00  

## Data

| carry_risk_score | file_age_min | khalid_index | ml_fields_populated | ml_risk_score |
|---|---|---|---|---|
| 0 | 1.7 | 43 | 0 | 0 |

## Log
## A. File freshness

- `00:06:09`   intelligence-report.json age: 1.7 min (3,366 bytes)
- `00:06:09`   LastModified: 2026-04-25T00:04:27+00:00
## B. data.scores dict (critical for signal-logger)

- `00:06:09`   scores: {
  "khalid_index": 43,
  "crisis_distance": 60,
  "plumbing_stress": 0,
  "ml_risk_score": 0,
  "carry_risk_score": 0,
  "vix": null,
  "move": null
}
- `00:06:09` 
  Real values:  2
- `00:06:09`   Zero values:  3
- `00:06:09`   Null values:  2
- `00:06:09` ✅   ✓ khalid_index = 43 (real value, was 0)
## C. Synthesized ML fields from new pred dict

- `00:06:09`   - executive_summary         EMPTY (acceptable for synth — only fills what's available)
- `00:06:09`   - carry_trade               EMPTY (acceptable for synth — only fills what's available)
- `00:06:09`   - sector_rotation           EMPTY (acceptable for synth — only fills what's available)
- `00:06:09`   - trade_recommendations     EMPTY (acceptable for synth — only fills what's available)
- `00:06:09`   - market_snapshot           EMPTY (acceptable for synth — only fills what's available)
## D. Top-level regime/phase fields

- `00:06:09`   regime: {'khalid': 'BEAR', 'ml': 'N/A', 'ml_description': '', 'sector': 'N/A', 'credit': 'N/A', 'liquidity': 'contracting', 'curve': 'NORMAL'}
- `00:06:09`   phase:  PRE-CRISIS
- `00:06:09` ✅   ✓ regime is set ({'khalid': 'BEAR', 'ml': 'N/A', 'ml_description': '', 'sector': 'N/A', 'credit': 'N/A', 'liquidity': 'contracting', 'curve': 'NORMAL'})
- `00:06:09` ✅   ✓ phase is set (PRE-CRISIS)
## E. Recent justhodl-intelligence log output

- `00:06:09`   Latest stream: 2026/04/25/[$LATEST]9c59dc80aa124506af807343fc65a912 (1.7 min old)
- `00:06:09`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `00:06:09`     === MARKET INTELLIGENCE ENGINE v3.0 ===
- `00:06:09`     Loading data/report.json (current)...
- `00:06:09`     data/report.json: OK
- `00:06:09`     Loading repo-data.json...
- `00:06:09`     FETCH_ERR[https://justhodl-dashboard-live.s3.amazonaws.com/repo-data.j]:HTTP Error 403: Forbidden
- `00:06:09`     repo-data.json: FAILED
- `00:06:09`     Loading edge-data.json (for pred synthesis)...
- `00:06:09`     FETCH_ERR[https://justhodl-dashboard-live.s3.amazonaws.com/edge-data.j]:HTTP Error 403: Forbidden
- `00:06:09`     Loading flow-data.json (for pred synthesis)...
- `00:06:09`     Generating cross-system intelligence...
- `00:06:09`     Publishing to justhodl-dashboard-live/intelligence-report.json
- `00:06:09`     === DONE === Phase:PRE-CRISIS Khalid:43 Crisis:60 Metrics:9
- `00:06:09` Done

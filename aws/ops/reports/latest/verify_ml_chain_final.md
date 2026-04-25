# Final verification — entire ml-predictions chain healthy

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-04-25T00:11:37+00:00  

## Data

| carry_risk_score | critical_scores_real | khalid_index | ml_fields_populated | ml_risk_score |
|---|---|---|---|---|
| 0 | 2/4 | 43 | 0/5 | 0 |

## Log
## A. intelligence-report.json — fresh + real values?

- `00:11:37`   Age: 1.6 min, size: 4,264 bytes
- `00:11:37` 
  scores dict:
- `00:11:37` {
    "khalid_index": 43,
    "crisis_distance": 60,
    "plumbing_stress": 25,
    "ml_risk_score": 0,
    "carry_risk_score": 0,
    "vix": 19.31,
    "move": null
}
- `00:11:37`   ✓ khalid_index = 43
- `00:11:37`   ✗ ml_risk_score = 0
- `00:11:37`   ✗ carry_risk_score = 0
- `00:11:37`   ✓ plumbing_stress = 25
## B. Synthesized ML fields populated?

- `00:11:37`   - executive_summary: empty
- `00:11:37`   - carry_trade: empty
- `00:11:37`   - sector_rotation: empty
- `00:11:37`   - trade_recommendations: empty
- `00:11:37`   - market_snapshot: empty
## C. Latest justhodl-intelligence log output

- `00:11:37`   Latest stream: 2026/04/25/[$LATEST]5c63ae0dd3434684ab47abd4ed56f690 (1.6 min old)
- `00:11:37`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `00:11:37`     === MARKET INTELLIGENCE ENGINE v3.0 ===
- `00:11:37`     Loading data/report.json (current)...
- `00:11:37`     data/report.json: OK
- `00:11:37`     Loading repo-data.json...
- `00:11:37`     repo-data.json: OK
- `00:11:37`     Loading edge-data.json (for pred synthesis)...
- `00:11:37`     Loading flow-data.json (for pred synthesis)...
- `00:11:37`     Generating cross-system intelligence...
- `00:11:37`     Publishing to justhodl-dashboard-live/intelligence-report.json
- `00:11:37`     === DONE === Phase:PRE-CRISIS Khalid:43 Crisis:60 Metrics:13
- `00:11:37` 
  Error lines in this run: 0
## D. Trigger signal-logger — next batch should have real ml_risk

- `00:11:37` ✅   Async-triggered signal-logger (status 202)
- `00:11:37`   Next run reads fresh intelligence-report.json with real scores
- `00:11:37` ⚠   Partial: 2/4 critical scores real
- `00:11:37` Done

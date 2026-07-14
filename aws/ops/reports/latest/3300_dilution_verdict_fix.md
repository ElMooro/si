- `15:46:36`   zip: 123884 bytes
## 1. Lambda
**Status:** failure  
**Duration:** 498.6s  
**Finished:** 2026-07-14T15:54:54+00:00  

## Error

```
SystemExit: FAILS: BMNR cache never freshened; AAPL cache never freshened; BMNR verdict None — expected HEAVY/DEATH; BMNR risk_flag not set; AAPL regression: verdict None
```

## Data

| aapl_key | aapl_risk | aapl_verdict | bmnr_key | bmnr_key_source | bmnr_live_vs_fy | bmnr_qtr_yoy | bmnr_risk | bmnr_verdict | fails | live_page_marker | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | None | None | None | None | None | None |  |  |  |
| None | None | None |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | True |  |
|  |  |  |  |  |  |  |  |  | ['BMNR cache never freshened', 'AAPL cache never freshened', 'BMNR verdict None — expected HEAVY/DEATH', 'BMNR risk_flag not set', 'AAPL regression: verdict None'] |  | [] |

## Log

- `15:46:36`   Lambda exists — updating
- `15:46:42` ✅   ✓ updated justhodl-equity-research
## 2. Force-refresh BMNR + AAPL through the fixed grader

## 3. Page guard markers


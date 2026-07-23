# ops 3762 — canary #13: narrow-line PPI acceleration

**Status:** failure  
**Duration:** 405.6s  
**Finished:** 2026-07-23T02:13:39+00:00  

## Error

```
SystemExit: 1
```

## Data

| accelerating | decelerating | failed | lines | verdict |
|---|---|---|---|---|
| 0 | 0 | G1_settle,G3_data_truth,G4_breadth | 0 | FAIL |

## Log
## G0 — key contract (producer + page)

- `02:06:53` PASS G0_key_contract — producer_missing=[] page_missing=[]
## G1 — settle v1.2.0

- `02:13:38` FAIL G1_settle — marker absent
## G2 — async invoke + freshness

## G3 — data truth (accel IS the 2nd derivative)

- `02:13:38` FAIL G3_data_truth — no doc
## G4 — sweep covers the discovered universe

- `02:13:38`   universe=198 swept=0 coverage=0.0%
- `02:13:38`   drop reasons: {}
- `02:13:38`   dropped ids (sample): []
- `02:13:38` FAIL G4_breadth — coverage 0.0% (0 of 198) · accounted=0/198 · reasons={}
## G5 — page served + field coverage + nav

- `02:13:39`   served page CURRENT len=10871 after 0s
- `02:13:39` PASS G5_page_live — len=10871
## VERDICT

- `02:13:39` ✗ gates failed: ['G1_settle', 'G3_data_truth', 'G4_breadth']

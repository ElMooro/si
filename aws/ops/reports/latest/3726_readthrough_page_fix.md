# ops 3726 — readthrough.html: surface the hidden engine fields

**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-07-22T20:00:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| engine actually publishes: {'top_picks': True, 'consensus_coverage': True, 'quadrant_counts': True, 'materiality': True, 'materiality_note': True, 'quadrant_note': True,  | G0_key_contract | True |
| already patched (idempotent re-run) | G1_patched | True |
| board header now has 12 columns | G2_columns_wired | True |
| rendered 12/13; still missing=['capture_share'] | G3_coverage_closed | False |

## Log
- `20:00:14` G0_key_contract True
- `20:00:14` G1_patched True
- `20:00:14` G2_columns_wired True
- `20:00:14` G3_coverage_closed False
- `20:00:14` VERDICT: GAPS: G3_coverage_closed

# ops 3725 — readthrough.html: surface the hidden engine fields

**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-07-22T19:56:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| engine actually publishes: {'top_picks': True, 'consensus_coverage': True, 'quadrant_counts': True, 'materiality_pct': False, 'edge_source': True, 'capture_share': True,  | G0_key_contract | False |
| page 13976 -> 19508 bytes; +picks +consensus +evidence columns +chips | G1_patched | True |
| board header now has 12 columns | G2_columns_wired | True |
| rendered 10/12; still missing=['quadrant_note', 'capture_share'] | G3_coverage_closed | False |

## Log
- `19:56:28` G0_key_contract False
- `19:56:28` G1_patched True
- `19:56:28` G2_columns_wired True
- `19:56:28` G3_coverage_closed False
- `19:56:28` VERDICT: GAPS: G0_key_contract,G3_coverage_closed

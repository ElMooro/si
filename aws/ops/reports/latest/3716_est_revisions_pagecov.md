# ops 3716 — estimate-revisions diagnosis + readthrough page coverage

**Status:** failure  
**Duration:** 0.3s  
**Finished:** 2026-07-22T18:43:20+00:00  

## Error

```
SystemExit: 1
```

## Data

| detail | gate | ok |
|---|---|---|
| data/estimate-revisions.json age=1.0h | A0_artifact_exists | True |
| upward=0 downward=0 n_tracked=436 n_fmp_enriched=256 n_with_history=436 status=LIVE | A1_revision_arrays_nonempty | False |
| emulated join -> 0 tickers (sample=[]) | A2_reader_join_nonempty | False |
| age=1.0h (>48h = schedule dead, cf. ops 3642 pattern) | A3_fresh | True |
| references_benzinga=True uses_shared_fmp_analyst=False (Benzinga 403-dead since 2026-07-15; ops 3311-3323 re-sourced peers onto aws/shared/fmp_analyst.py) | A4_not_on_dead_benzinga | False |
| readthrough.html 13976 bytes | B0_page_exists | True |
| rendered=3/13 MISSING=['consensus_coverage', 'quadrant_note', 'consensus_observed', 'consensus_moved', 'consensus_dissenting', 'analyst_actions', 'rpo_representative', 'b | B1_all_engine_fields_rendered | False |
| row+fundamental keys with NO render path (42): ['anchor_close', 'bom_weight', 'capture_share', 'catalyst_type', 'dollar_vol', 'edge_confidence', 'edge_source', 'move_sinc | B2_no_unrendered_row_fields | False |

## Log
## A — estimate-revisions sidecar

- `18:43:20` A0_artifact_exists True
- `18:43:20` A1_revision_arrays_nonempty False
- `18:43:20` A2_reader_join_nonempty False
- `18:43:20` A3_fresh True
- `18:43:20` A4_not_on_dead_benzinga False
## B — readthrough.html field coverage

- `18:43:20` B0_page_exists True
- `18:43:20` B1_all_engine_fields_rendered False
- `18:43:20` B2_no_unrendered_row_fields False
- `18:43:20` VERDICT: GAPS: A1_revision_arrays_nonempty,A2_reader_join_nonempty,A4_not_on_dead_benzinga,B1_all_engine_fields_rendered,B2_no_unrendered_row_fields

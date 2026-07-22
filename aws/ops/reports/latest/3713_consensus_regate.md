# ops 3713 — consensus observability re-gate (3712 key typo)

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-07-22T17:42:41+00:00  

## Data

| detail | gate | ok |
|---|---|---|
| engine writes consensus_coverage=True consensus_honesty=False (3712 read the latter) | G0_key_contract | True |
| v1.2.1=True observed_field=True | G1_shipped | True |
| version=1.2.1 rows=74 generated=2026-07-22T16:59:39 degraded=['fundamental sidecar missing: forward-orders'] | G2_artifact_fresh | True |
| consensus snapshot ledger holds 73 names | G3_snapshot_ledger | True |
| rows_consensus_observed=74 rows=74 sellside_covered=137 deltas=62 | G4_consensus_observable | True |
| coverage_block=74 recount_from_rows=74 (mismatch = rollup drifted from row truth) | G5_counter_matches_rows | True |
| non-empty={'CONSENSUS_NOT_DUE_YET': 53, 'CONSENSUS_DISSENTING': 2, 'PRICE_LEADING': 18, 'FULLY_PRICED': 1} top_share=0.716 (3710 collapsed to PRICE_ONLY 78/79) | G6_quadrants_discriminate | True |

## Log
- `17:42:41` G0_key_contract True
- `17:42:41` G1_shipped True
- `17:42:41` G2_artifact_fresh True
- `17:42:41` G3_snapshot_ledger True
- `17:42:41` G4_consensus_observable True
- `17:42:41` G5_counter_matches_rows True
- `17:42:41` G6_quadrants_discriminate True
- `17:42:41` VERDICT: PASS_ALL

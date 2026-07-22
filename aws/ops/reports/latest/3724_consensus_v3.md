# ops 3724 — estimate-revisions v3.0.0 (moving consensus)

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-22T19:51:30+00:00  

## Data

| detail | gate | ok |
|---|---|---|
| fmp_estimate_profile() returns fwd_eps_cur (the moving epsAvg) | G0_key_contract | True |
| fmp populated at 746 before row loop at 5278 | G0b_fmp_before_loop | True |
| cur_eps <- fmp fwd_eps_cur; sched_eps kept for context; ledger re-seeds pre-v3 keys; version -> 3.0.0 | G1_patched | True |
| engine source compiles | G2_compiles | True |
| n_keys=448 pre_v3_keys_to_reseed=448 updated=2026-07-22T19:21:54.161781+00:00 | G3_ledger_snapshot | True |

## Log
- `19:51:30` G0_key_contract True
- `19:51:30` G0b_fmp_before_loop True
- `19:51:30` G1_patched True
- `19:51:30` G2_compiles True
- `19:51:30` G3_ledger_snapshot True
- `19:51:30` VERDICT: PASS_ALL

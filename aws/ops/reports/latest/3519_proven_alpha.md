# ops 3519 — Proven Alpha Report + census 7/10

**Status:** success  
**Duration:** 243.2s  
**Finished:** 2026-07-19T17:42:22+00:00  

## Log
- `17:38:19` PASS  T1_ci — {'famA': ('PROVEN', 70.0), 'ftd-squeeze': ('PENDING', None)}
- `17:38:19`   zip: 82094 bytes
## 1. Lambda

- `17:38:19`   Lambda missing — creating
- `17:38:22` ✅   ✓ created justhodl-proven-alpha
- `17:38:54` PASS  T2_live — {'summary': {'n_families': 413, 'n_proven': 42, 'n_evaluating': 271, 'n_pending': 92, 'n_suppressed': 8, 'n_graded_signals': 36651, 'n_pending_signals': 9406}, 'ftd': {'verdict': 'PENDING', 'pending': 13, 'first_grades_eta': '2026-08-09'}, 'congress_buy': {'verdict': 'PENDING', 'graded': 0, 'hit_primary': None, 'avg_excess_bps': None, 'pending': 14}, 'top5': [('squeeze_risk', 'PROVEN', 316, 69.8), ('crisis_hy_oas_vs_hyg', 'PROVEN', 224, 81.7), ('crisis_dfii10_vs_gld', 'PROVEN', 212, 72.2), ('attention_crowded', 'PR
- `17:38:55` PASS  T3_schedule — cron(40 22 * * ? *)
- `17:38:55`   zip: 81097 bytes
## 1. Lambda

- `17:38:55`   Lambda exists — updating
- `17:38:58` FAIL  T4_sla_overrides — Parameter validation failed:
Invalid type for parameter Environment.Variables, value: None, type: <class 'NoneType'>, valid types: <class 'dict'>
Invalid type for parameter Timeout, value: None, type: <class 'NoneType'>, valid types: <class 'int'>
Invalid type for parameter MemorySize, value: None, 
- `17:42:22` PASS  T5_pages — {'shelled': 12, 'missing': [], 'page': True, 'node': True, 'pinned': True}

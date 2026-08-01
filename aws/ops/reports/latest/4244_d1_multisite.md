# ops 4244 — D1 v1.1.1 multi-site classification

**Status:** success  
**Duration:** 193.2s  
**Finished:** 2026-08-01T15:45:08+00:00  

## Data

| bounds | case | cls | complete | defects | expect | failed | fixed | flags | function | got | new | passed | scanned | section | sev1 | sites | total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | unguarded |  |  |  | UNGUARDED |  |  |  |  | UNGUARDED |  | True |  | selftest |  |  |  |
|  | counter |  |  |  | BOUNDED |  |  |  |  | BOUNDED |  | True |  | selftest |  |  |  |
|  | kickoff |  |  |  | BOUNDED |  |  |  |  | BOUNDED |  | True |  | selftest |  |  |  |
|  | clean |  |  |  | NO_SELF_INVOKE |  |  |  |  | NO_SELF_INVOKE |  | True |  | selftest |  |  |  |
|  | multi_site |  |  |  | BOUNDED |  |  |  |  | BOUNDED |  | True |  | selftest |  |  |  |
|  |  |  | True |  |  | 23 |  |  |  |  |  |  | 745 | scan |  |  | 768 |
| [10] |  | BOUNDED |  |  |  |  |  | None | justhodl-13f-clone-alpha |  |  | True |  | real_engines |  | 1 |  |
| None |  | BOUNDED |  |  |  |  |  | ['_internal'] | justhodl-equity-research |  |  | True |  | real_engines |  | 1 |  |
| [12] |  | BOUNDED |  |  |  |  |  | ['phase'] | justhodl-fundamental-census |  |  | True |  | real_engines |  | 2 |  |
|  |  |  |  | 54 |  |  | 4 |  |  |  | 0 |  |  | audit | 7 |  |  |

## Log
## 1. Deploy

- `15:42:02` ✅ marker verified
## 2. GATE A — self-test, now 5 cases

- `15:42:03` ✅    unguarded   expect=UNGUARDED       got=UNGUARDED       {"sites": [{"guard": "UNGUARDED"}], "n_sites": 1, "unguarded_sites": 1}
- `15:42:03` ✅    counter     expect=BOUNDED         got=BOUNDED         {"sites": [{"guard": "BOUNDED_COUNTER", "bound": 10}], "n_sites": 1, "min_bound": 10, "bou
- `15:42:03` ✅    kickoff     expect=BOUNDED         got=BOUNDED         {"sites": [{"guard": "BOUNDED_FLAG", "flag": "_internal"}], "n_sites": 1, "flags": ["_inte
- `15:42:03` ✅    clean       expect=NO_SELF_INVOKE  got=NO_SELF_INVOKE  {}
- `15:42:03` ✅    multi_site  expect=BOUNDED         got=BOUNDED         {"sites": [{"guard": "BOUNDED_FLAG", "flag": "phase"}, {"guard": "BOUNDED_COUNTER", "bound
- `15:42:03` ✅ 5/5
## 3. Rescan (cache is keyed by sha; code changed, so rescan)

- `15:42:03` cache cleared — classifier semantics changed, stale classifications would be worse than none
- `15:44:33` pass 1 -> {"ok": true, "mode": "d1scan", "scanned": 745, "from_cache": 0, "failed": 23, "cursor": 0, "total": 768, "complete": true, "cache_entries": 745}
- `15:44:33` ⚠ 23 function(s) had no lambda_function.py to parse (non-Python or nested handler) — reported, not assumed clean
## 4. GATE B — the three real engines

- `15:44:33` ✅    justhodl-13f-clone-alpha           cls=BOUNDED        sites=1 bounds=[10] flags=None
- `15:44:33` ✅    justhodl-equity-research           cls=BOUNDED        sites=1 bounds=None flags=['_internal']
- `15:44:33` ✅    justhodl-fundamental-census        cls=BOUNDED        sites=2 bounds=[12] flags=['phase']
- `15:44:33` fleet: {'NO_SELF_INVOKE': 742, 'BOUNDED': 3}
- `15:44:33` GENUINELY UNGUARDED: 0
- `15:44:33`    bounded justhodl-13f-clone-alpha               sites=1 bounds=[10] flags=None
- `15:44:33`    bounded justhodl-equity-research               sites=1 bounds=None flags=['_internal']
- `15:44:33`    bounded justhodl-fundamental-census            sites=2 bounds=[12] flags=['phase']
## 5. Full audit with the corrected D1

- `15:45:08` ✅ audit -> {"ok": true, "n_defects": 54, "n_new": 0, "n_fixed": 4, "sev1": 7}
## RESULT

- `15:45:08` ✅ OPS 4244 PASS

# ops 4187 — config-driven demand wave

**Status:** failure  
**Duration:** 30.9s  
**Finished:** 2026-07-31T19:41:52+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4187_config_wave.py", line 154, in main
    assert mark in src
           ^^^^^^^^^^^
AssertionError

```

## Data

| defs |
|---|
| ["CAG", "BCOI", "IPRI"] |

## Log
- `19:41:21`   CAG wb: 200
- `19:41:26`   MEI BCOI: BSCICP03.IXNSA ✓
- `19:41:28`   MEI CCI: none answered
- `19:41:30`   MEI IPRI: PRINTO01.IXOBSA ✓
- `19:41:33`   MEI LEI: none answered
- `19:41:36`   MEI CU: none answered
- `19:41:42` ✅   family-defs.json written (3)
- `19:41:52` ✅   justhodl-families-feed settled at loop 1

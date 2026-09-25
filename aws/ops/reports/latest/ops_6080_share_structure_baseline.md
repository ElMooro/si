
**Status:** failure  
**Duration:** 12.3s  
**Finished:** 2026-09-25T11:49:08+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6080_share_structure_baseline.py", line 78, in main
    result=inventory.inventory(packets,progress['generated_at'][:10],(ROOT/native).read_text(encoding='utf-8'))
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/share_structure_inventory.py", line 67, in inventory
    if not isinstance(values,dict):raise ValueError('Explicit valuation universe mapping required')
                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Explicit valuation universe mapping required

```

## Log



**Status:** failure  
**Duration:** 4.7s  
**Finished:** 2026-09-19T04:11:03+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5829_quantum_permission_verify.py", line 31, in main
    result=json.loads(response['Payload'].read());assert not response.get('FunctionError') and result.get('statusCode')==200,result
                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: {'ok': True, 'regime': 'RECESSION_BUST', 'sources_ok': 29, 'ladder': 13, 'money_map': 4, 'best_class': 'BONDS_LONG'}

```

## Log


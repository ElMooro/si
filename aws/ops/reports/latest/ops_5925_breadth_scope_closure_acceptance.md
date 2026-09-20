
**Status:** failure  
**Duration:** 8.6s  
**Finished:** 2026-09-20T13:38:01+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5925_breadth_scope_closure_acceptance.py", line 69, in main
    assert count<=1 and hashlib.sha256(clean).hexdigest()==build['files_sha256'][name]==digest
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Data

| engine_invocations | notifications_sent | portfolio_writes | private_account_reads | runtimes |
|---|---|---|---|---|
| 0 | 0 | 0 | 0 | {'justhodl-playbook-engine': {'commit': '345f294e5ca1b83f4ce8373e525b7269c71621cd', 'code_sha256': 'uQdgZh1X6TU+VxxYOOTC4+NIDJ+9dzRJlR8ElmPRkFw=', 'packaged_files_checked': 6, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-symbol-dictionary': {'commit': '345f294e5ca1b83f4ce8373e525b7269c71621cd', 'code_sha256': '3+S0kTDEP2IJQl8VTjXVmR4/o35CBEysjWwUdZG9Sfw=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-thesis-engine': {'commit': '345f294e5ca1b83f4ce8373e525b7269c71621cd', 'code_sha256': '8ob+4Pv8vaJXDENuxaTn9KB0iZ0Z6sZHvGEtSucEyaY=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-wl-engines': {'commit': '345f294e5ca1b83f4ce8373e525b7269c71621cd', 'code_sha256': '27216KmDrtw3+SnrTFOlFhNBYeEuElDQE+vCuWmAopo=', 'packaged_files_checked': 4, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-market-internals': {'commit': '245e5e386bca56324da8db2a5468296fc27972ba', 'code_sha256': 'EGjaH8FgZvoUDhmj1iar0G9vrvPV39Hj+OL9SIjtztI=', 'packaged_files_checked': 5, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-signal-board': {'commit': '3a3408be40f4c60c41beab29bdbab6952894534e', 'code_sha256': 'iM6jAMsZVFXBqyalKRBxtLs7mZ7oPn4yZPT2Ouj05MY=', 'packaged_files_checked': 8, 'all_packaged_sources_match': True, 'alias': None}} |

## Log


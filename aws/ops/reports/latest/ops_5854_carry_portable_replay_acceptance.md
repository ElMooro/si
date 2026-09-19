
**Status:** failure  
**Duration:** 323.0s  
**Finished:** 2026-09-19T15:20:14+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5854_carry_portable_replay_acceptance.py", line 68, in main
    assert not response.get('FunctionError') and result.get('statusCode')==200,'carry invocation failed'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: carry invocation failed

```

## Data

| active_carry_diagnostics | invoke_status | portable_original_run | portable_output_sha256 | runtimes | throttle_rejections | windows_linux_exact_match |
|---|---|---|---|---|---|---|
|  |  |  |  | {'justhodl-carry-surface': {'commit': 'd594ff9c70bb3a2d4878a5d5b387a1945825a04c', 'code_sha256': 'hAW430UFaTsN11I+1Xm2e1pEATEtgyoTOVI+z1VABhg='}, 'justhodl-stress-index': {'commit': '8b8c0f1e3ed8a51d9eacdefb366006a5b1d16650', 'code_sha256': 'EmLA4SuI8u2EuE550hT193JcwJK1Pc14gJY3qUzPctI='}} |  |  |
|  |  | data/carry-research/runs/6a083d9c711fc9e620b129a6b4f2d051cfaa21f82d6e170b85e933341dc41c4c.json | d3828f530d45678bdb58a38f97d956b0d225da317b5d074c5344b93f26928303 |  |  | True |
|  | 503 |  |  |  | 0 |  |
| ['[carry-research] {"error_count": 38, "first_error_keys": ["BAMLU0A0CMEY", "BAMLU0A0CMEY:definition", "DGS6MO", "DGS6MO:definition", "DGS7", "DGS7:definition", "IR3TIB01AUM156N", "IR3TIB01AUM156N:definition", "IR3TIB01CAM156N", "IR3TIB01CAM156N:definition", "IR3TIB01CHM156N", "IR3TIB01CHM156N:definition"], "verified_equities": 110, "verified_rates": 18}', '[carry-research] unavailable: ValueError'] |  |  |  |  |  |  |

## Log


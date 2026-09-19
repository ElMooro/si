
**Status:** failure  
**Duration:** 459.3s  
**Finished:** 2026-09-19T07:33:05+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5839_global_liquidity_acceptance.py", line 81, in main
    assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],'runtime moved during acceptance'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: runtime moved during acceptance

```

## Data

| active_aliases | runtimes |
|---|---|
| {'justhodl-katlin': {'function': 'justhodl-katlin', 'alias': 'live', 'version': '5', 'code_sha256': 'QV1qfFYSof5LbwB+WffGh7BZyyJ+nzgP01H7YXneOH0=', 'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a'}} | {'justhodl-wl-fusion': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': '2a09VYR9VrSIWblHRO13BHnkDv1mNtyXCftjFnGJpC8='}, 'justhodl-allocator': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'Uj9JAYrQhLBW2nnoBq5WDbLXUsu+fwt5hh6aiXTOfPw='}, 'justhodl-crisis-composite': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'Js5B46d8CFTLyjwjJxDg49dn8U8Uw/3ggYswbTObgvM='}, 'justhodl-cycle-clock': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'ra5eElvrOT86If8Fnx9u0u306bFbpi0YMb8nTRwHVsw='}, 'justhodl-global-liquidity': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'K6H/wo5suMJJsjED3J2egnPPvTM4ap6FS9Uya8QlP2w='}, 'justhodl-katlin': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'QV1qfFYSof5LbwB+WffGh7BZyyJ+nzgP01H7YXneOH0='}, 'justhodl-liquidity-agent': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'FerEM/Z2bNYWx+EgUTC7uVSbS085wsWAoVbynr18CQs='}, 'justhodl-quantum-desk': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'vDzJYyqMGliUM9JtBFTzHJVgz39hr/nKpZ0QuzKkr2s='}, 'justhodl-risk-regime': {'commit': 'f40aadcf966a845ff84c198321bcad227725bf6a', 'code_sha256': 'JsL5Shvqg/b0TQBVNYGOvRtHmnxJn2kSm5loKhc1fQM='}} |

## Log


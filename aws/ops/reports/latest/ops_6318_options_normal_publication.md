
**Status:** failure  
**Duration:** 2.5s  
**Finished:** 2026-09-28T22:22:35+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6318_options_normal_publication.py", line 42, in main
    before={fn:acceptance.runtime(*args,fn) for fn in EXPECTED};check_packages(before,baselines)
                                                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6318_options_normal_publication.py", line 29, in check_packages
    acceptance.check_runtime(actual[fn],baselines[fn],EXPECTED[fn],count)
  File "/home/runner/work/si/si/aws/ops/staged/ops_6232_sec_search_research_acceptance.py", line 28, in check_runtime
    raise ValueError('Exact complete SEC release required')
ValueError: Exact complete SEC release required

```

## Log



**Status:** failure  
**Duration:** 40.9s  
**Finished:** 2026-09-21T05:12:04+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5971_sector_fusion_native_acceptance.py", line 150, in main
    restored=store.compile_output(inputs,audit_read);assert restored['quality']['issuer_price_five_window_available']==11
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/sector_fusion_store.py", line 94, in compile_output
    issuance=issuer.restore(source(refs[ROOTS[2]],ROOTS[2],read),read)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/sector_fusion_issuer.py", line 49, in restore
    dates=sorted({row['date'] for row in ivv['rows']});calendar=checked(packet['reference_calendar'],'histories',read)
                                                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/sector_fusion_issuer.py", line 25, in checked
    raw=read(ref['key'])
        ^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_5971_sector_fusion_native_acceptance.py", line 147, in audit_read
    assert ref,'Unretained audit source path';return protected(ref,read)
           ^^^
AssertionError: Unretained audit source path

```

## Data

| runtimes |
|---|
| {'justhodl-sector-flow-state': {'commit': '830ef1a195e591fd56658dbd03c87581f6ca0955', 'code_sha256': 'J8BgES3EbDh0L19U3rFvrWyJDwBKUWoFts1DPNv9S8I=', 'packaged_files_checked': 22, 'all_packaged_sources_match': True, 'alias': None}, 'justhodl-sector-capital-fusion': {'commit': '830ef1a195e591fd56658dbd03c87581f6ca0955', 'code_sha256': 'KYEVMlN8AZEnGrFAZwwWIkBHabBtmMIW3aeFTKLNu5M=', 'packaged_files_checked': 23, 'all_packaged_sources_match': True, 'alias': None}} |

## Log


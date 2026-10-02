
**Status:** failure  
**Duration:** 4.2s  
**Finished:** 2026-10-02T00:12:49+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6437_crypto_funding_archive_acceptance.py", line 68, in main
    before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6437_crypto_funding_archive_acceptance.py", line 53, in inspect
    before=snapshot()
           ^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6437_crypto_funding_archive_acceptance.py", line 51, in snapshot
    raise ValueError('Named release acceptance failed: '+fn+' '+type(exc).__name__) from None
ValueError: Named release acceptance failed: justhodl-options-confluence ValueError

```

## Log


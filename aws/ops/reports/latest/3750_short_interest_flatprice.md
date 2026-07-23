# ops 3750 — canary #19: short-interest v1.1 (SI collapse on flat price)

**Status:** failure  
**Duration:** 0.1s  
**Finished:** 2026-07-23T00:23:14+00:00  

## Error

```
SystemExit: 1
```

## Log
## G0 — key contract

- `00:23:14` ✗ UNCAUGHT: Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/pending/ops_3750_short_interest_flatprice.py", line 74, in <module>
    page = (ROOT / "squeeze.html").read_text()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1027, in read_text
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1013, in open
    return io.open(self, mode, buffering, encoding, errors, newline)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/si/si/aws/squeeze.html'


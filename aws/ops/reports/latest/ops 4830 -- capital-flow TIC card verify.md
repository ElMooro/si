# G0. live JSON contract for every card binding

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-08-17T16:57:42+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4830_capflow_tic_card_verify.py", line 117, in main
    html = PAGE.read_text(encoding="utf-8")
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1027, in read_text
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1013, in open
    return io.open(self, mode, buffering, encoding, errors, newline)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/si/si/aws/capital-flow.html'

```

## Log
- `16:57:42` ✅   flows_bn.total latest=+262.8 12m=+1771.9 z=1.61
- `16:57:42` ✅   flows_bn.treas latest=+17.0 12m=+334.8 z=-0.16
- `16:57:42` ✅   flows_bn.equity latest=+134.3 12m=+901.7 z=1.27
- `16:57:42` ✅   flows_bn.corp latest=+52.5 12m=+448.8 z=1.17
- `16:57:42` ✅   flows_bn.agency latest=+19.1 12m=+128.9 z=0.67
- `16:57:42` ✅   flows_bn.tbills latest=-43.5 12m=+52.6 z=-1.63
- `16:57:42` ✅   signals.risk_appetite latest=+206.0B z=1.6
- `16:57:42` ✅   signals.safe_haven latest=-117.3B z=-1.2
- `16:57:42` ✅   signals.total_demand latest=+223.0B z=1.22
- `16:57:42` ✅   signals.official_private latest=+230.7B z=1.84
- `16:57:42` ✅   holder_splits.lt_total status=OK gap=0.0
- `16:57:42` ✅   split OK: private +246.8B vs official +16.1B
- `16:57:42` ✅   country_lt_treasury: 5 OK rows (e.g. china +608.5B, other-gap 6.1)
# 1. committed-HTML asserts


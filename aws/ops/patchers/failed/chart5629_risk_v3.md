# patcher failed: chart5629_risk_v3.py

Quarantined by apply-lane run 35262404863 at 2026-09-17T19:01:58Z. Push a corrected patcher under a NEW name; this one will not run again.

```
  File "/home/runner/work/si/si/aws/ops/patchers/chart5629_risk_v3.py", line 17
    t2, n = re.subn(r'<script src="/jh-chart-risk\.js[^"']*"></script>', NEW, t, count=1)
                                                         ^
SyntaxError: closing parenthesis ']' does not match opening parenthesis '('
```

# Fix Khalid timeline — use full timestamps, not date-grouped

**Status:** failure  
**Duration:** 18.1s  
**Finished:** 2026-04-25T02:32:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/109_fix_khalid_timeline_v2.py", line 195, in <module>
    html = html_path.read_text()
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1027, in read_text
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/pathlib.py", line 1013, in open
    return io.open(self, mode, buffering, encoding, errors, newline)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/si/si/reports.html'

```

## Log
## 1. Inspect khalid_index signal shape

## 2. Patch Lambda

- `02:31:42` ✅   Syntax OK
## 3. Re-deploy Lambda

- `02:31:49` ✅   Re-deployed (9909B)
## 4. Invoke + verify

- `02:32:00` ✅   Invoked: timeline_points=150 scorecard_rows=15
- `02:32:00`   Timeline points: 150
- `02:32:00`   First point: {'ts': '2026-04-24T23:42:13.103532+00:00', 'date': '2026-04-24', 'score': 43.0, 'regime': 'BEAR'}
- `02:32:00`   Last point: {'ts': '2026-04-25T00:24:42.477539+00:00', 'date': '2026-04-25', 'score': 43.0, 'regime': 'BEAR'}
- `02:32:00`   Unique dates: 2
- `02:32:00`   Score range: 43.0 → 43.0
## 5. Update reports.html to display intra-day timestamps


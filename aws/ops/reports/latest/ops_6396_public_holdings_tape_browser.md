
**Status:** failure  
**Duration:** 13.7s  
**Finished:** 2026-09-30T23:45:38+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6396_public_holdings_tape_browser.py", line 152, in main
    except Exception as exc:raise RuntimeError("browser_probe_stopped_"+type(exc).__name__) from None
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: browser_probe_stopped_RuntimeError

```

## Data

| browser_version | certificate_validation | consumer_proof | denied_statuses | error_categories | failure_category | new_schedules | observed_at | page | pages | reason | sandbox | scope | status | stored_credentials | viewports | width |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | index.html -> jh-nav-drawer.js -> jh-market-tape.js -> #jhc-tape |  |  |  | 0 |  |  | ['etf-holdings.html', 'flow-lookthrough.html', 'index.html'] |  |  | Public browser read-only; no AWS clients/provider acquisition/app writes |  |  | [1440, 390] |  |
| 154.0.8037.57 | normal |  |  |  |  |  |  |  |  |  | True |  |  | False |  |  |
|  |  |  | [] | {'other_console_error': 1, 'page_exception': 1} | browser_or_assertion_failure |  | 2026-09-30T23:45:35.287104+00:00 | etf-holdings.html |  | Error |  |  | failed |  |  | 1440 |

## Log


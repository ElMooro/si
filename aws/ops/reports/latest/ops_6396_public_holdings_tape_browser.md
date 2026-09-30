
**Status:** failure  
**Duration:** 8.5s  
**Finished:** 2026-09-30T23:41:35+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6396_public_holdings_tape_browser.py", line 148, in main
    except Exception as exc:raise RuntimeError("browser_probe_stopped_"+type(exc).__name__) from None
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: browser_probe_stopped_AttributeError

```

## Data

| browser_version | certificate_validation | consumer_proof | new_schedules | pages | sandbox | scope | stored_credentials | viewports |
|---|---|---|---|---|---|---|---|---|
|  |  | index.html -> jh-nav-drawer.js -> jh-market-tape.js -> #jhc-tape | 0 | ['etf-holdings.html', 'flow-lookthrough.html', 'index.html'] |  | Public browser read-only; no AWS clients/provider acquisition/app writes |  | [1440, 390] |
| 154.0.8037.57 | normal |  |  |  | True |  | False |  |

## Log


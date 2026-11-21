# ops 6875 - the deploy gate of every engine that bundles the FMP-bank reader, run now (read-only)

**Status:** success  
**Duration:** 113.6s  
**Finished:** 2026-11-21T05:17:51+00:00  

## Data

| does_not_compile | engines | green | not_reached | red | timed_out | wall_s | with_suite |
|---|---|---|---|---|---|---|---|
| 0 | 225 | 100 | 0 | 1 | 0 | 114 | 101 |

## Log
- `05:16:16` aws/shared/fmp_bank.py is bundled into 225 engines
## 1. The static checks for the whole set

- `05:16:24` ✅ scripts/validate_lambda_sources.py for 225 engines: passes
- `05:16:24` ✅ scripts/validate_lambda_configs.py for 225 engines: passes
- `05:16:30` ✅ py_compile: every engine's sources compile
## 2. Each engine's own suite

- `05:16:30` 101 engines ship a suite; 124 ship none (their gate is the static checks)
- `05:16:33` justhodl-bond-desk                           RED             0 s | tests/test_cohort_native.py:67 test_corrupt_outputs_partial_input_wrong_code_and_permissions_fail_closed | subprocess.CalledProcessError: Command '['/opt/hostedtoolcache/Python/3.12.16/x64/bin/python3', '/home/runner/work/si/si/aws/lambdas/justhodl-bond-desk/tests/test_cohort_native.py']' returned non-zero
- `05:17:51` green 100, red 1, over 420 s 0, not reached 0; the slowest green suites: justhodl-fmp-walker 73 s, justhodl-macro-leads 13 s, justhodl-bottleneck-boom 3 s, justhodl-etf-true-flows 3 s, justhodl-wave-signal-logger 2 s
- `05:17:51` all suites one after another would take about 3 minutes on this runner
- `05:17:51` ⚠ a roll-out of the shared reader would stop at the first of these 1 suites; each also blocks that engine's own next deploy

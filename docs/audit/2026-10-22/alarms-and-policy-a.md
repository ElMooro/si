# 2026-10-22 - the four alarms after the thaw: what was real, what was done

Khalid's 2026-10-21 08:17 CT screenshots showed four CloudWatch alarms red. After a full post-thaw day (ops 6609,
13:29 UTC) and his decisions ("continue", then policy A):

| alarm | finding | action | state after |
|---|---|---|---|
| `justhodl-schedule-drift` | cleared by itself once ops 6594/6605 declared every live trigger | none | OK since 07:31 UTC |
| `justhodl-llm-providers-down` | both providers answered 200 to the daily ping; the alarm expected hourly data and treated the gaps as breaching | ops 6615: period 86400 s, 1 datapoint, missing = notBreaching (backup `backups/alarms/20261022T173528Z/`) | clears on the next evaluation; fires only when `ProvidersUp` < 1 |
| `justhodl-integrity-new-defects` | 20 "D4 orphan rules" were alias-qualified targets (`...:function:<name>:live`) misread as a function named `live` - every function and alias exists and runs | fleet-integrity v1.2.1 (`_fn_of`), commit 4b55630f; nothing disabled | still red on D8 errors 174, D7 concurrency 57, D2 timeout-clipped 7, D14 unscanned 6, D6 never-ran 4 |
| `justhodl-contract-sev1` | 2 of the sev1 rows were `llm_status: "unavailable"` on ka-/khalid-analysis - the standing no-paid-AI policy (2026-09-18), declared, not a defect | contract gate accepts the declared state (commit f48075f0); gate re-run 17:37 UTC | 44 sev1 remain: ROW_COLLAPSE 33, MISSING_KEYS 11 (pre-freeze set) |

Also closed the same day: the error storm behind issue #122 - `justhodl-polygon-futures-curves` paced to Massive's
~5 futures requests a minute, 429 / empty body = provider failure, 900 s run, out of the 5-minute lane; first paced
run complete and published (`data/futures-research.json` fresh for the first time since Oct 10). Massive plan facts:
the key is on Futures Basic (5 calls/min, $0); Starter ($29/month) would only buy intraday futures curves.

The no-paid-AI policy stands (Khalid, 13:22 ET): `aws/shared/llm_router.py` resolves no credentials, GLM commentary is
off, the deploy gate's NoPaidAdapters tests block any paid call, SageMaker is stopped and sealed; ai.html says
"offline (deterministic desk read, no calls)"; brain.json regime_read/directive stay null. Option B (one voice under a
daily dollar ceiling in `llm_cost`) is the documented way back if he ever wants the AI reads.

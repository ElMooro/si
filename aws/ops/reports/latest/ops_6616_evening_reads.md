# ops 6616 - evening reads: futures-curves after the overlap, research-backtest / estimate-revisions errors, the LLM alarm, sev1 (read-only)

**Status:** success  
**Duration:** 6.2s  
**Finished:** 2026-10-23T00:05:17+00:00  

## Data

| estimate_revisions_errors | futures_head_generated | futures_records_since_1630 | futures_statuses | llm_alarm | research_backtest_errors | sev1 |
|---|---|---|---|---|---|---|
|  | 2026-10-22T16:48:39 | 1 | {"complete": 1} |  |  |  |
| 6 |  |  |  | OK | 3 | 44 |

## Log
## a. futures-curves

- `00:05:12` {"written": "16:45:03", "request_id": "21545d3f-9142-4e76-8527-07cd", "status": "complete", "phase": "complete", "published": true, "error": null, "failure_class": null, "started": "16:35:30", "completed": "16:45:02", "requests": 46, "quality": {"complete_datasets": 42, "products": 7, "selected_contracts": 21, "status": "descriptive"}}
- `00:05:12` public head: contract futures-original-research.v1 generated_at 2026-10-22T16:48:39 quality {"complete_datasets": 25, "products": 7, "selected_contracts": 15, "status": "descriptive"} products 7
## b. research-backtest and estimate-revisions errors

- `00:05:15` research-backtest errors (Complete): [["11:48:14", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,historical_keys+list_keys_under(CRITIQUE_PREFIX))) \u00a0\u00a0File \"/var"], ["11:31:44", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,historical_keys+list_keys_under(CRITIQUE_PREFIX))) \u00a0\u00a0File \"/var"], ["11:16:26", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,historical_keys+list_keys_under(CRITIQUE_PREFIX))) \u00a0\u00a0File \"/var"]]
- `00:05:17` estimate-revisions errors (Complete): [["13:33:51", "[ERROR] ValueError: Original received calendar required Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 368, in lambda_handler \u00a0\u00a0\u00a0\u00a0calendar_acquisition=_estimate_calendar_fetch();calendar_e"], ["13:31:55", "[ERROR] ValueError: Original received calendar required Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 368, in lambda_handler \u00a0\u00a0\u00a0\u00a0calendar_acquisition=_estimate_calendar_fetch();calendar_e"], ["13:30:57", "[ERROR] ValueError: Original received calendar required Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 368, in lambda_handler \u00a0\u00a0\u00a0\u00a0calendar_acquisition=_estimate_calendar_fetch();calendar_e"]]
## c. the LLM alarm after policy A

- `00:05:17` justhodl-llm-providers-down: OK since 2026-10-22 17:36:18 - Threshold Crossed: 1 out of the last 1 datapoints [2.0 (21/10/26 17:36:00)] was not less than the threshold (1.0) (minimum 1 datapoint for ALARM -> OK transitio | period 86400 eval 1 datapoints 1 missing notBreaching
## d. sev1 now

- `00:05:17` contract-violations.json generated 2026-10-22T17:37:34.611465+00:00: sev1 44 by class [["ROW_COLLAPSE", 33], ["MISSING_KEYS", 11]]; sev2 STALE 296
- `00:05:17` ✅ read-only; nothing changed

# ops 6621 - morning reads: dark-pool after 23:31, research-backtest, the four alarms (read-only)

**Status:** success  
**Duration:** 6.9s  
**Finished:** 2026-10-23T13:30:15+00:00  

## Data

| alarms | d4 | dark_pool_error_lines | integrity_new | research_backtest_errors | sev1 |
|---|---|---|---|---|---|
| {"justhodl-contract-sev1": "ALARM", "justhodl-integrity-new-defects": "ALARM", "justhodl-llm-providers-down": "OK", "justhodl-schedule-drift": "OK"} | 0 | 2 | 245 | 3 | 24 |

## Log
## a. dark-pool

- `13:30:10` dark-pool error lines (Complete): [["10-22 23:34:55", "[ERROR] ValueError: Request already attempted; inspect retained status instead of recollecting Traceback (most recent call last):   File \"/var/task/lambda_function.py\", line 46, in lambda_handler     result=producer.run(client,BUCKET,event.get('request_id') or event.get('id') or execution,execution,remaining_seconds=remaining)   File \"/var/task/offexchange_producer.py\", line 155, in run     if old_status.get('status'"], ["10-22 23:31:39", "[ERROR] ValueError: One or more planned sources failed; current head retained Traceback (most recent call last):   File \"/var/task/lambda_function.py\", line 46, in lambda_handler     result=producer.run(client,BUCKET,event.get('request_id') or event.get('id') or execution,execution,remaining_seconds=remaining)   File \"/var/task/offexchange_producer.py\", line 183, in run     if errors or daily is None:raise ValueError"]]
- `13:30:12` dark-pool runs per hour (Complete): [["10-22 23:00", 3]]
## b. research-backtest

- `13:30:14` research-backtest error lines in 14 h (Complete): 3; newest: [["10-23 11:48:08", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,his"], ["10-23 11:31:47", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,his"], ["10-23 11:16:27", "[ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION Traceback (most recent call last): \u00a0\u00a0File \"/var/task/lambda_function.py\", line 578, in lambda_handler \u00a0\u00a0\u00a0\u00a0list(pool.map(read_s3_json,his"]]
## c. alarms

- `13:30:15` justhodl-contract-sev1: ALARM since 2026-10-21 13:03:47 - Threshold Crossed: 1 datapoint [46.0 (20/10/26 13:03:00)] was greater than the threshold (0.0).
- `13:30:15` justhodl-integrity-new-defects: ALARM since 2026-10-21 05:02:21 - Threshold Crossed: 1 datapoint [263.0 (20/10/26 05:02:00)] was greater than the threshold (0.0).
- `13:30:15` justhodl-llm-providers-down: OK since 2026-10-22 17:36:18 - Threshold Crossed: 1 out of the last 1 datapoints [2.0 (21/10/26 17:36:00)] was not less than the threshold (1.0) (minimum 1 datapoint for ALARM -> OK
- `13:30:15` justhodl-schedule-drift: OK since 2026-10-22 07:31:38 - Threshold Crossed: 1 datapoint [0.0 (21/10/26 07:31:00)] was not greater than the threshold (0.0).
- `13:30:15` contract-violations.json generated 2026-10-23T13:02:42.204051+00:00: sev1 24 by class [["ROW_COLLAPSE", 20], ["MISSING_KEYS", 4]]; llm_status rows 0; sev2 STALE 280
- `13:30:15` fleet-integrity.json generated 2026-10-23T05:01:53.184998+00:00 v1.2.1: new 245 by class [["D8_errors", 171], ["D7_concurrency_drop", 57], ["D2_timeout_clipped", 7], ["D14_unscanned", 6], ["D6_scheduled_never_ran", 4]]; D4 rows 0
- `13:30:15` ✅ read-only; nothing changed

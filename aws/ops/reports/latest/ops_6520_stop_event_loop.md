# ops 6520 - break the miss-calibrator <-> event-coordinator loop

**Status:** success  
**Duration:** 246.5s  
**Finished:** 2026-10-16T19:48:53+00:00  

## Log
- `19:44:47` ✅ ledger written data/ops/event-loop-quarantine-6520.json: {"justhodl-miss-calibrator": {"reserved": null, "event_invoke_config": null}, "justhodl-event-coordinator": {"reserved": null, "event_invoke_config": null}}
- `19:44:48` ✅ justhodl-miss-calibrator reserved=0 eic={'MaximumRetryAttempts': 0, 'MaximumEventAgeInSeconds': 60, 'DestinationConfig': {'OnSuccess': {}, 'OnFailure': {}}}
- `19:44:48` ✅ justhodl-event-coordinator reserved=0 eic={'MaximumRetryAttempts': 0, 'MaximumEventAgeInSeconds': 60, 'DestinationConfig': {'OnSuccess': {}, 'OnFailure': {}}}
- `19:44:48` ✅ lifecycle ops6520-purge-dead-versions-system-events present (rules now 10)
## Measurements

- `19:44:49` justhodl-miss-calibrator invocations/h last 6: [('10-16 13:44', 101991.0), ('10-16 14:44', 100569.0), ('10-16 15:44', 99774.0), ('10-16 16:44', 98676.0), ('10-16 17:44', 97367.0), ('10-16 18:44', 97150.0)]
- `19:44:50` justhodl-event-coordinator invocations/h last 6: [('10-16 13:44', 177648.0), ('10-16 14:44', 173553.0), ('10-16 15:44', 171059.0), ('10-16 16:44', 169416.0), ('10-16 17:44', 169852.0), ('10-16 18:44', 168262.0)]
- `19:44:50` coordinator AsyncEventAge max (ms) last hours: [('10-16 15:44', 21600000.0), ('10-16 16:44', 21600001.0), ('10-16 17:44', 21600283.0), ('10-16 18:44', 21599999.0)]
- `19:44:51` BucketSizeBytes GB by day: [('09-30', 712.49), ('10-01', 729.09), ('10-02', 749.15), ('10-03', 767.33), ('10-04', 823.91), ('10-05', 965.9), ('10-06', 823.93), ('10-07', 843.95), ('10-08', 857.67), ('10-09', 879.98), ('10-10', 891.84), ('10-11', 921.73), ('10-12', 24653.67), ('10-13', 61148.73), ('10-14', 105871.84)]
- `19:44:51` NumberOfObjects (millions) by day: [('09-30', 2.96), ('10-01', 3.02), ('10-02', 3.1), ('10-03', 3.15), ('10-04', 3.25), ('10-05', 3.44), ('10-06', 3.41), ('10-07', 3.49), ('10-08', 3.56), ('10-09', 3.65), ('10-10', 3.69), ('10-11', 3.75), ('10-12', 9.11), ('10-13', 12.11), ('10-14', 16.35)]
- `19:44:52` system-events/audit/2026-10-16.jsonl -> {'current_mb': 25.96, 'versions_sampled': 1000, 'truncated': True, 'sample_gb': 25.96}
- `19:44:52` system-events/audit/2026-10-15.jsonl -> {'current_mb': 30.85, 'versions_sampled': 1000, 'truncated': True, 'sample_gb': 30.84}
- `19:44:52` system-events/audit/2026-10-14.jsonl -> {'current_mb': 30.7, 'versions_sampled': 1000, 'truncated': True, 'sample_gb': 30.69}
- `19:44:52` system-events/audit/2026-10-13.jsonl -> {'current_mb': 33.2, 'versions_sampled': 1000, 'truncated': True, 'sample_gb': 33.2}
- `19:44:52` system-events/audit/2026-10-12.jsonl -> {'current_mb': 22.04, 'versions_sampled': 1000, 'truncated': True, 'sample_gb': 22.04}
- `19:44:52` system-events/audit/2026-10-11.jsonl -> {'current_mb': 0.03, 'versions_sampled': 44, 'truncated': False, 'sample_gb': 0.0}
- `19:48:52` justhodl-miss-calibrator 1-min invocations around quarantine: [('10-16 19:39', 1580.0), ('10-16 19:40', 1638.0), ('10-16 19:41', 1623.0), ('10-16 19:42', 1570.0), ('10-16 19:43', 1614.0), ('10-16 19:44', 1278.0)]
- `19:48:52` justhodl-miss-calibrator 1-min throttles: [('10-16 19:39', 43.0), ('10-16 19:40', 36.0), ('10-16 19:41', 41.0), ('10-16 19:42', 25.0), ('10-16 19:43', 40.0), ('10-16 19:44', 366.0), ('10-16 19:45', 141.0)]
- `19:48:53` justhodl-event-coordinator 1-min invocations around quarantine: [('10-16 19:39', 2856.0), ('10-16 19:40', 2838.0), ('10-16 19:41', 2831.0), ('10-16 19:42', 2865.0), ('10-16 19:43', 2808.0), ('10-16 19:44', 2247.0)]
- `19:48:53` justhodl-event-coordinator 1-min throttles: [('10-16 19:39', 52.0), ('10-16 19:40', 58.0), ('10-16 19:41', 52.0), ('10-16 19:42', 73.0), ('10-16 19:43', 63.0), ('10-16 19:44', 409.0), ('10-16 19:45', 124.0)]
- `19:48:53` ✅ wrote aws/ops/reports/6520.json

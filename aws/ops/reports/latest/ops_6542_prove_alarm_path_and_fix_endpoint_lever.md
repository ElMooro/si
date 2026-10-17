# ops 6542 - alarm -> governor path proven; owned endpoint lever fixed

**Status:** success  
**Duration:** 207.5s  
**Finished:** 2026-10-17T02:01:44+00:00  

## Log
## Governor v1.1.1

- `02:00:50` ✅ governor v1.1.1 live (stage FREEZE)
## 1. Real alarm -> governor

- `02:00:50`   01:54:23 ConfigurationUpdate Alarm "justhodl-s3-live-bucket-size" created
- `02:00:50`   01:55:02 StateUpdate Alarm updated from INSUFFICIENT_DATA to ALARM
- `02:00:50`   01:55:02 Action Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor
- `02:00:50`   01:55:02 Action Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts
- `02:00:50` governor decisions for justhodl-s3-live-bucket-size: [('spend-governor/events/20261017T015502Z-fast-fuse-check.json', [], 0.02)]
- `02:00:50` ✅ REAL path proven: CloudWatch alarm -> SNS -> governor fast fuse (spend-governor/events/20261017T015502Z-fast-fuse-check.json)
## 2. Synthetic alarm through SNS -> governor

- `02:01:03` ✅ SYNTHETIC path proven: SNS -> governor -> decision spend-governor/events/20261017T020050Z-fast-fuse-check.json (hot=[], fleet $0.08/day)
## 3. Owned SageMaker endpoint lever

- `02:01:03` ✅ governor role: + cloudwatch:PutMetricAlarm/DeleteAlarms/DescribeAlarms (Application Auto Scaling checks the caller for the scaling policy's alarms)
- `02:01:44` tick 1: drift={"applied": 1} smasync={"status": "reapplied", "error": null, "error_msg": null, "scale_down_note": null}
- `02:01:44` scalable target now: min=0 max=1 suspended={'DynamicScalingInSuspended': False, 'DynamicScalingOutSuspended': True, 'ScheduledScalingSuspended': True}
- `02:01:44` ✅ owned endpoint: scale-out suspended (scale-in live) - a Sunday exam request cannot wake an instance
## 4. Guardrail self-check

- `02:01:44` governor guardrail check: {"checked_at": "2026-10-17T02:01:23.672156+00:00", "problems": []}
- `02:01:44` ✅ done

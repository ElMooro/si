# ops 6533 - governor v1.0.3, owned endpoint suspension, Oct 16 cost cuts live

**Status:** success  
**Duration:** 4.1s  
**Finished:** 2026-10-17T01:27:57+00:00  

## Log
## Governor

- `01:27:55` governor status: {"version": "1.0.3", "mode": "enforce", "stage": "FREEZE", "stage_since": "2026-10-16T22:53:03.699511+00:00", "cap": 500.0, "cap_source": "budget", "held": {"FREEZE/lambda": 914, "FREEZE/smasync": 1}}
- `01:27:55` state updated 2026-10-17T01:07:37.734218+00:00; smasync ledger entry: {"status": "failed", "error": "ValidationException", "error_msg": "User is missing the following permissions: cloudwatch:PutMetricAlarm, cloudwatch:DeleteAlarms", "failed_at": "2026-10-17T01:07:37.734218+00:00", "applied_at": null, "scale_down_note": null, "scaled_down_from": null, "before": {"desired": 0, "endpoint": "jh-owned-coder-async", "max": 1, "min": 0, "suspended": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}, "variant": "owned"}}
- `01:27:55` ⚠ ledger entries failing: [{"key": "smasync:endpoint/jh-owned-coder-async/variant/owned", "status": "failed", "error": "ValidationException", "error_msg": "User is missing the following permissions: cloudwatch:PutMetricAlarm, cloudwatch:DeleteAlarms"}]
## Owned SageMaker endpoint

- `01:27:56` endpoint: {"status": "InService", "config": "jh-owned-coder-cfg-16k-20260918200057", "variants": [{"VariantName": "owned", "CurrentInstanceCount": 0, "DesiredInstanceCount": 0}], "last_modified": "2026-10-15 08:06:16.424000+00:00", "instance_types": ["ml.g5.xlarge"]}
- `01:27:56` scalable target: [{"MinCapacity": 0, "MaxCapacity": 1, "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}, "RoleARN": "arn:aws:iam::857687956942:role/aws-service-role/sagemaker.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_SageMakerEndpoint"}]
- `01:27:56` scaling policies: [{"name": "jh-owned-coder-wake", "type": "StepScaling", "alarms": ["jh-owned-coder-has-backlog"]}, {"name": "jh-owned-coder-backlog", "type": "TargetTrackingScaling", "alarms": ["TargetTracking-endpoint/jh-owned-coder-async/variant/owned-AlarmHigh-03c32acc-5efd-4f35-a692-46f1ca5a6df9", "TargetTracking-endpoint/jh-owned-coder-async/variant/owned-AlarmLow-27b44ff3-d259-4d64-b799-164fdeced99d"]}]
- `01:27:56` ⚠ endpoint: ClientError: An error occurred (AccessDeniedException) when calling the DescribeScheduledActions operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: application-autoscaling:DescribeScheduledActions on resource: arn:aws:application-autoscaling:us-east-1:8576879569
## Cost-cut deploys

- `01:27:57` fleet-error-monitor: {"lookback": "60", "rule": "rate(1 hour)", "rule_state": "ENABLED", "last_modified": "2026-10-17T00:31:56.000+0000"}
- `01:27:57` justhodl-ai LastModified 2026-10-16T23:10:59.000+0000 (bc19de21 deploy ran 2026-10-16T23:06Z)
- `01:27:57` ✅ all checks passed

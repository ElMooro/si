# ops 5825 -- owned endpoint scale-to-zero after the 16k update

**Status:** success  
**Duration:** 33.3s  
**Finished:** 2026-09-18T23:44:54+00:00  

## Data

| alarm | config | current_instances | desired | policies | scalable_target | status |
|---|---|---|---|---|---|---|
|  | jh-owned-coder-cfg-16k-20260918200057 | 1 | 1 |  |  | InService |
| [{"StateValue": "ALARM", "StateReason": "Threshold Crossed: 1 datapoint [1.0 (18/09/26 23:29:00)] was greater than or equal to the threshold (1.0).", "AlarmActions": ["arn:aws:autoscaling:us-east-1:857687956942:scalingPolicy:f3c10f6b-ad93-4252-bb88-910ec1f288ee:resource/sagemaker/endpoint/jh-owned-coder-async/variant/owned:policyName/jh-owned-coder-wake"]}] |  |  |  | ['jh-owned-coder-wake:StepScaling', 'jh-owned-coder-backlog:TargetTrackingScaling'] | [{"MinCapacity": 0, "MaxCapacity": 1, "SuspendedState": {"DynamicScalingInSuspended": false, "DynamicScalingOutSuspended": false, "ScheduledScalingSuspended": false}}] |  |

## Log
## 1. State now

- `23:44:22` ApproximateBacklogSize (last 40 min): [('23:04', 0.0), ('23:09', 0.0), ('23:14', 0.0), ('23:19', 0.0), ('23:24', 0.0), ('23:29', 55.0), ('23:34', 55.0), ('23:39', 55.0)]
- `23:44:22` HasBacklogWithoutCapacity (last 40 min): [('23:04', 0.0), ('23:09', 0.0), ('23:14', 0.0), ('23:19', 0.0), ('23:24', 0.0), ('23:29', 1.0), ('23:34', 1.0), ('23:39', 1.0)]
## 2. Re-apply the 5563 registration (idempotent)

- `23:44:24` ✅ target 0..1, backlog target-tracking, wake step policy + alarm re-applied
## 3. Does it wake? (55 exam requests may still be queued)

- `23:44:54` t+ 0 min instances=1 desired=1 status=InService alarm=OK
- `23:44:54` ✅ awake

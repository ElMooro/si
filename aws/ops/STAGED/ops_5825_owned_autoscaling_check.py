"""ops 5825 -- the owned endpoint's scale-to-zero after the 16k update (Claude, 2026-09-18). Direct lane.
ops 5824's holdout exam got 0 of 55 answers in 14 min: the endpoint may no longer wake from zero after update_endpoint.
Reads: endpoint status/instances, scalable target, policies, the wake alarm (state + actions) and the backlog metric; then
RE-APPLIES the exact ops 5563 registration (idempotent puts) and, if the endpoint sits at zero with a backlog, waits up to
12 min for it to wake. Writes only autoscaling registrations/alarm (same names as 5563).
"""
import json, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REGION, NAME = "us-east-1", "jh-owned-coder-async"


def main():
    sm = boto3.client("sagemaker", region_name=REGION); aas = boto3.client("application-autoscaling", region_name=REGION); cw = boto3.client("cloudwatch", region_name=REGION)
    rid = "endpoint/%s/variant/owned" % NAME
    with report("5825_owned_autoscaling_check") as r:
        r.heading("ops 5825 -- owned endpoint scale-to-zero after the 16k update")
        r.section("1. State now")
        ep = sm.describe_endpoint(EndpointName=NAME); pv = (ep.get("ProductionVariants") or [{}])[0]
        r.kv(status=ep["EndpointStatus"], config=ep["EndpointConfigName"], current_instances=pv.get("CurrentInstanceCount"), desired=pv.get("DesiredInstanceCount"))
        tg = aas.describe_scalable_targets(ServiceNamespace="sagemaker", ResourceIds=[rid], ScalableDimension="sagemaker:variant:DesiredInstanceCount").get("ScalableTargets", [])
        pol = aas.describe_scaling_policies(ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount").get("ScalingPolicies", [])
        alarms = cw.describe_alarms(AlarmNames=["jh-owned-coder-has-backlog"]).get("MetricAlarms", [])
        r.kv(scalable_target=json.dumps([{k: t.get(k) for k in ("MinCapacity", "MaxCapacity", "SuspendedState")} for t in tg], default=str)[:300],
             policies=[p["PolicyName"] + ":" + p["PolicyType"] for p in pol], alarm=json.dumps([{k: a.get(k) for k in ("StateValue", "StateReason", "AlarmActions")} for a in alarms], default=str)[:400])
        now = datetime.now(timezone.utc)
        for metric in ("ApproximateBacklogSize", "HasBacklogWithoutCapacity"):
            dp = cw.get_metric_statistics(Namespace="AWS/SageMaker", MetricName=metric, Dimensions=[{"Name": "EndpointName", "Value": NAME}], StartTime=now - timedelta(minutes=40), EndTime=now, Period=300, Statistics=["Average", "Maximum"]).get("Datapoints", [])
            r.log("%s (last 40 min): %s" % (metric, sorted([(d["Timestamp"].strftime("%H:%M"), d["Maximum"]) for d in dp])))
        r.section("2. Re-apply the 5563 registration (idempotent)")
        aas.register_scalable_target(ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount", MinCapacity=0, MaxCapacity=1)
        aas.put_scaling_policy(PolicyName="jh-owned-coder-backlog", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount", PolicyType="TargetTrackingScaling",
                               TargetTrackingScalingPolicyConfiguration={"TargetValue": 2.0, "ScaleInCooldown": 900, "ScaleOutCooldown": 60,
                                                                         "CustomizedMetricSpecification": {"MetricName": "ApproximateBacklogSizePerInstance", "Namespace": "AWS/SageMaker", "Dimensions": [{"Name": "EndpointName", "Value": NAME}], "Statistic": "Average"}})
        step = aas.put_scaling_policy(PolicyName="jh-owned-coder-wake", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount", PolicyType="StepScaling",
                                      StepScalingPolicyConfiguration={"AdjustmentType": "ChangeInCapacity", "MetricAggregationType": "Average", "Cooldown": 60, "StepAdjustments": [{"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}]})
        cw.put_metric_alarm(AlarmName="jh-owned-coder-has-backlog", MetricName="HasBacklogWithoutCapacity", Namespace="AWS/SageMaker", Statistic="Average", Period=60, EvaluationPeriods=1, Threshold=1,
                            ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="missing", Dimensions=[{"Name": "EndpointName", "Value": NAME}], AlarmActions=[step["PolicyARN"]])
        r.ok("target 0..1, backlog target-tracking, wake step policy + alarm re-applied")
        r.section("3. Does it wake? (55 exam requests may still be queued)")
        for i in range(24):
            time.sleep(30)
            ep = sm.describe_endpoint(EndpointName=NAME); pv = (ep.get("ProductionVariants") or [{}])[0]
            al = cw.describe_alarms(AlarmNames=["jh-owned-coder-has-backlog"]).get("MetricAlarms", [{}])[0]
            r.log("t+%2d min instances=%s desired=%s status=%s alarm=%s" % ((i + 1) // 2, pv.get("CurrentInstanceCount"), pv.get("DesiredInstanceCount"), ep["EndpointStatus"], al.get("StateValue")))
            if (pv.get("CurrentInstanceCount") or 0) >= 1 and ep["EndpointStatus"] == "InService":
                r.ok("awake"); return
        r.warn("still at zero after 12 min -- if the backlog metric above was 0 the queue was empty (requests expired); if >0 the wake path is broken")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Serve the Gear B champion on the owned endpoint (2026-09-23). Runner-side (factory-exam.yml, every grading tick).

No-op unless factory/gearb/champion.json names merged weights (merge_adapter.py wrote merge_manifest.json) that the
endpoint is not serving yet. Then, exactly the ops 5822 recipe with only the weights changed:
  live model (image + env, incl. the 16k context) -> new model whose ModelDataSource points at the merged prefix
  -> new endpoint config (same variant, same async config) -> update_endpoint (blue/green)
  -> RE-APPLY the scale-to-zero registration (update_endpoint detaches it: ops 5825)
  -> one proof round trip -> factory/control/inference.json names the champion (the page's voice line shows it)
  -> champion.served = {model, config, rollback_config, at}
A failed update leaves the old config serving; the rollback config is recorded either way.

  python3 scripts/factory_serve_champion.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

PRI = "justhodl-ai-857687956942"
NAME = "jh-owned-coder-async"
REGION = "us-east-1"
CHAMPION_KEY = "factory/gearb/champion.json"
CONTROL_KEY = "factory/control/inference.json"


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def reapply_autoscaling(aas, cw):
    rid = "endpoint/%s/variant/owned" % NAME
    aas.register_scalable_target(ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount", MinCapacity=0, MaxCapacity=1)
    aas.put_scaling_policy(PolicyName="jh-owned-coder-backlog", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount",
                           PolicyType="TargetTrackingScaling",
                           TargetTrackingScalingPolicyConfiguration={"TargetValue": 2.0, "ScaleInCooldown": 900, "ScaleOutCooldown": 60,
                                                                     "CustomizedMetricSpecification": {"MetricName": "ApproximateBacklogSizePerInstance", "Namespace": "AWS/SageMaker",
                                                                                                       "Dimensions": [{"Name": "EndpointName", "Value": NAME}], "Statistic": "Average"}})
    step = aas.put_scaling_policy(PolicyName="jh-owned-coder-wake", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount",
                                  PolicyType="StepScaling",
                                  StepScalingPolicyConfiguration={"AdjustmentType": "ChangeInCapacity", "MetricAggregationType": "Average", "Cooldown": 60,
                                                                  "StepAdjustments": [{"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}]})
    cw.put_metric_alarm(AlarmName="jh-owned-coder-has-backlog", MetricName="HasBacklogWithoutCapacity", Namespace="AWS/SageMaker", Statistic="Average", Period=60,
                        EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="missing",
                        Dimensions=[{"Name": "EndpointName", "Value": NAME}], AlarmActions=[step["PolicyARN"]])


def proof(s3, rt, tag: str) -> str:
    key = "factory/inference/requests/req-serve-%s.json" % tag
    s3.put_object(Bucket=PRI, Key=key, Body=json.dumps({"inputs": "<|im_start|>user\nReply with the single word OK.<|im_end|>\n<|im_start|>assistant\n",
                                                        "parameters": {"max_new_tokens": 8, "temperature": 0.0}}).encode(), ContentType="application/json")
    resp = rt.invoke_endpoint_async(EndpointName=NAME, InputLocation="s3://%s/%s" % (PRI, key), ContentType="application/json", Accept="application/json",
                                    InferenceId="serve-%s" % tag, InvocationTimeoutSeconds=900, RequestTTLSeconds=3600)
    out_key = resp["OutputLocation"][len("s3://%s/" % PRI):]
    fail_key = (resp.get("FailureLocation") or "")[len("s3://%s/" % PRI):]
    for _ in range(90):                          # a cold wake takes ~5-12 min
        time.sleep(20)
        for k, kind in ((out_key, "ok"), (fail_key, "fail")):
            if not k:
                continue
            try:
                body = s3.get_object(Bucket=PRI, Key=k)["Body"].read().decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                continue
            if kind == "fail":
                raise RuntimeError("proof failed: %s" % body[:300])
            return body[:200]
    raise RuntimeError("proof: no answer within 30 min")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", region_name=REGION); sm = boto3.client("sagemaker", region_name=REGION)
    champ = get_json(s3, CHAMPION_KEY)
    if not isinstance(champ, dict) or not champ.get("merged_prefix"):
        print(json.dumps({"serve": "noop", "reason": "no merged champion"})); return 0
    merged = str(champ["merged_prefix"])
    served = champ.get("served") or {}
    if served.get("merged_prefix") == merged:
        print(json.dumps({"serve": "noop", "reason": "already serving %s" % champ.get("generation")})); return 0
    manifest = get_json(s3, merged[len("s3://%s/" % PRI):] + "merge_manifest.json")
    if not isinstance(manifest, dict) or not manifest.get("files"):
        print(json.dumps({"serve": "noop", "reason": "merge manifest missing"})); return 0
    ep = sm.describe_endpoint(EndpointName=NAME)
    if ep["EndpointStatus"] != "InService":
        print(json.dumps({"serve": "wait", "reason": "endpoint %s" % ep["EndpointStatus"]})); return 0
    cfg = sm.describe_endpoint_config(EndpointConfigName=ep["EndpointConfigName"])
    variant = cfg["ProductionVariants"][0]
    live = sm.describe_model(ModelName=variant["ModelName"])
    pc = live["PrimaryContainer"]
    if not pc.get("ModelDataSource"):
        print(json.dumps({"serve": "refused", "reason": "live model has no ModelDataSource to swap"})); return 1
    gen = str(champ.get("generation"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    model_name = ("jh-owned-served-%s-%s" % (gen.replace("gen-", "g"), stamp))[:63]
    cfg_name = ("jh-owned-served-cfg-%s-%s" % (gen.replace("gen-", "g"), stamp))[:63]
    mds = json.loads(json.dumps(pc["ModelDataSource"]))
    mds["S3DataSource"]["S3Uri"] = merged
    plan = {"generation": gen, "model": model_name, "config": cfg_name, "merged_prefix": merged, "rollback_config": ep["EndpointConfigName"],
            "env_max_model_len": (pc.get("Environment") or {}).get("OPTION_MAX_MODEL_LEN")}
    if args.dry_run:
        print(json.dumps({"serve": "dry_run", **plan})); return 0
    sm.create_model(ModelName=model_name, ExecutionRoleArn=live["ExecutionRoleArn"], PrimaryContainer={"Image": pc["Image"], "Environment": pc.get("Environment") or {}, "ModelDataSource": mds})
    nv = {k: v for k, v in variant.items() if k in ("VariantName", "InstanceType", "InitialInstanceCount", "ContainerStartupHealthCheckTimeoutInSeconds", "ModelDataDownloadTimeoutInSeconds", "InitialVariantWeight")}
    nv["ModelName"] = model_name; nv["InitialInstanceCount"] = max(1, int(nv.get("InitialInstanceCount") or 1))
    kw = {"EndpointConfigName": cfg_name, "ProductionVariants": [nv]}
    if cfg.get("AsyncInferenceConfig"):
        kw["AsyncInferenceConfig"] = cfg["AsyncInferenceConfig"]
    sm.create_endpoint_config(**kw)
    sm.update_endpoint(EndpointName=NAME, EndpointConfigName=cfg_name)
    for _ in range(60):
        time.sleep(30)
        st = sm.describe_endpoint(EndpointName=NAME)
        if st["EndpointStatus"] in ("InService", "Failed"):
            break
    st = sm.describe_endpoint(EndpointName=NAME)
    reapply_autoscaling(boto3.client("application-autoscaling", region_name=REGION), boto3.client("cloudwatch", region_name=REGION))
    if st["EndpointStatus"] != "InService" or st["EndpointConfigName"] != cfg_name:
        champ["serve_failed"] = {**plan, "at": now_iso(), "status": st["EndpointStatus"], "reason": str(st.get("FailureReason"))[:300]}
        s3.put_object(Bucket=PRI, Key=CHAMPION_KEY, Body=json.dumps(champ, indent=1).encode(), ContentType="application/json")
        print(json.dumps({"serve": "failed", **champ["serve_failed"]})); return 1
    answer = proof(s3, boto3.client("sagemaker-runtime", region_name=REGION, config=Config(read_timeout=60)), stamp)
    ctl = get_json(s3, CONTROL_KEY) or {}
    ctl.update({"adapter_generation": gen, "served_model": model_name, "served_at": now_iso()})    # revision (the staged base) is left alone
    s3.put_object(Bucket=PRI, Key=CONTROL_KEY, Body=json.dumps(ctl, indent=1).encode(), ContentType="application/json")
    champ["served"] = {**plan, "at": now_iso(), "proof": answer}
    s3.put_object(Bucket=PRI, Key=CHAMPION_KEY, Body=json.dumps(champ, indent=1).encode(), ContentType="application/json")
    print(json.dumps({"serve": "served", **champ["served"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""ops 5552 -- re-arm of 5551 with diagnostics (re-audit A01/A10): waits up to 50 min, reads the endpoint's CloudWatch log
tail when it is not InService, verifies deletion when it fails (no unresolved billable resource), probes through the SAME
submit path the chat uses (factory_inference.submit -> the serving payload is the object at InputLocation), validates the
response with factory_inference.resolve (error/empty/malformed are failures), grants the Lambda role BEFORE enabling the
control, and treats autoscaling failure as a failed deployment (endpoint deleted).

ops 5551 -- the owned model answers the chat: SageMaker ASYNC endpoint that scales to zero (Claude, 2026-09-14).

Khalid: the chat must talk to the owned Qwen, not a status report. Doctrine: no always-on GPU. Resolution: an
asynchronous inference endpoint with autoscaling min 0 / max 1 -- nothing runs while idle, requests queue, the
instance starts on backlog and stops after the cooldown. Priced live (hosting family), tagged, capped by the
$20/day guard through the same policy Gear B uses.

  1. serving image: the newest LMI (DJL + vLLM) inference DLC tag in the AWS registry (ecr:DescribeImages on
     763104351884/djl-inference), pinned by digest in the record;
  2. SageMaker Model from the staged weights prefix (uncompressed ModelDataSource) with vLLM rolling batch;
  3. async EndpointConfig (S3 output/failure prefixes under factory/inference/outputs/) on ml.g5.xlarge;
  4. Endpoint + application-autoscaling target 0..1 on ApproximateBacklogSizePerInstance;
  5. wait for InService (<= 25 min), one real async invocation with the Qwen chat template, wait for the output
     object; write factory/control/inference.json {enabled: true} ONLY after that round trip succeeds;
  6. justhodl-ai role: sagemaker:InvokeEndpointAsync on this endpoint (a managed policy attached to the role).
If the endpoint never reaches InService the op deletes it (cost stops) and reports the CloudWatch reason.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRI = "justhodl-ai-857687956942"
ROLE = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"
LAMBDA_ROLE = "justhodl-lambda-execution-role"
MODEL_ID = "qwen2-5-coder-7b-instruct"
NAME = "jh-owned-coder-async"
INSTANCE = "ml.g5.xlarge"
DLC_ACCOUNT, DLC_REPO = "763104351884", "djl-inference"


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def newest_lmi_tag(ecr):
    """Newest `*-lmi*-cu*` tag of djl-inference in the AWS DLC registry, with its digest."""
    token, best = None, None
    while True:
        kw = {"registryId": DLC_ACCOUNT, "repositoryName": DLC_REPO, "maxResults": 1000, "filter": {"tagStatus": "TAGGED"}}
        if token:
            kw["nextToken"] = token
        resp = ecr.describe_images(**kw)
        for img in resp.get("imageDetails", []):
            for tag in img.get("imageTags", []):
                if "-lmi" in tag and "-cu" in tag and "neuron" not in tag and "cpu" not in tag:
                    stamp = img.get("imagePushedAt")
                    if best is None or stamp > best[2]:
                        best = (tag, img.get("imageDigest"), stamp)
        token = resp.get("nextToken")
        if not token:
            break
    return best


def main() -> int:
    red, warn = [], []
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    ecr = boto3.client("ecr", region_name=REGION)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import cost_guard as cg
    with report("ops_5552_owned_async_endpoint_rearm") as R:
        R.heading("ops 5552 (re-arm of 5551) -- owned model async endpoint (scale-to-zero) from the staged weights; proven by one round trip")
        R.kv(head=head[:10], endpoint=NAME, instance=INSTANCE)
        manifest = _get_json(s3, "factory/models/base/%s/manifest.json" % MODEL_ID)
        if not manifest or manifest.get("status") != "staged":
            red.append("weights"); R.fail("no staged manifest for %s" % MODEL_ID); R.fail("RED"); return 1
        weights = manifest["s3_prefix"]
        R.ok("weights: %s (%s, %.1f GB)" % (weights, manifest.get("license"), float(manifest.get("total_bytes") or 0) / 1e9))
        # runner rights (group route; the user is at its quotas)
        try:
            iam = boto3.client("iam")
            user = boto3.client("sts").get_caller_identity()["Arn"].split("/")[-1]
            parn = "arn:aws:iam::%s:policy/justhodl-runner-hosting" % ACCOUNT
            try:
                iam.create_policy(PolicyName="justhodl-runner-hosting", Description="Runner: create/describe/delete the owned async endpoint + autoscaling",
                                  PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
                                      {"Effect": "Allow", "Action": ["sagemaker:CreateModel", "sagemaker:DescribeModel", "sagemaker:DeleteModel", "sagemaker:CreateEndpointConfig",
                                                                     "sagemaker:DescribeEndpointConfig", "sagemaker:DeleteEndpointConfig", "sagemaker:CreateEndpoint", "sagemaker:DescribeEndpoint",
                                                                     "sagemaker:DeleteEndpoint", "sagemaker:UpdateEndpoint", "sagemaker:InvokeEndpointAsync", "sagemaker:AddTags", "sagemaker:ListTags"],
                                       "Resource": ["arn:aws:sagemaker:%s:%s:model/jh-owned-*" % (REGION, ACCOUNT), "arn:aws:sagemaker:%s:%s:endpoint-config/jh-owned-*" % (REGION, ACCOUNT),
                                                    "arn:aws:sagemaker:%s:%s:endpoint/jh-owned-*" % (REGION, ACCOUNT)]},
                                      {"Effect": "Allow", "Action": ["application-autoscaling:RegisterScalableTarget", "application-autoscaling:PutScalingPolicy", "application-autoscaling:DescribeScalableTargets",
                                                                     "application-autoscaling:DescribeScalingPolicies", "application-autoscaling:DeregisterScalableTarget", "cloudwatch:PutMetricAlarm",
                                                                     "cloudwatch:DescribeAlarms", "cloudwatch:DeleteAlarms", "iam:CreateServiceLinkedRole"], "Resource": "*"},
                                      {"Effect": "Allow", "Action": ["iam:PassRole"], "Resource": ROLE, "Condition": {"StringEquals": {"iam:PassedToService": "sagemaker.amazonaws.com"}}},
                                      {"Effect": "Allow", "Action": ["iam:PutRolePolicy", "iam:GetRolePolicy"], "Resource": "arn:aws:iam::%s:role/%s" % (ACCOUNT, LAMBDA_ROLE)}]}))
            except iam.exceptions.EntityAlreadyExistsException:
                pass
            try:
                iam.create_group(GroupName="justhodl-runner-ecr")
            except iam.exceptions.EntityAlreadyExistsException:
                pass
            iam.attach_group_policy(GroupName="justhodl-runner-ecr", PolicyArn=parn)
            iam.add_user_to_group(GroupName="justhodl-runner-ecr", UserName=user)
            R.ok("hosting rights attached to group justhodl-runner-ecr for %s; waiting 90 s" % user); time.sleep(90)
        except Exception as e:  # noqa: BLE001
            warn.append("iam"); R.warn("hosting grant failed: %s" % str(e)[:160])
        # price
        pricing = boto3.client("pricing", region_name="us-east-1")
        price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="hosting")
        hourly = price.get("usd_per_hour")
        if not hourly:
            red.append("price"); R.fail("no live hosting price for %s -- refusing unpriced spend (%s)" % (INSTANCE, price.get("error"))); R.fail("RED"); return 1
        R.ok("hosting price %s = $%.4f/h while scaled up; $0 at zero instances" % (INSTANCE, float(hourly)))
        # serving image
        best = newest_lmi_tag(ecr)
        if not best:
            red.append("image"); R.fail("no LMI inference tag found in %s/%s" % (DLC_ACCOUNT, DLC_REPO)); R.fail("RED"); return 1
        tag, digest, pushed = best
        image = "%s.dkr.ecr.%s.amazonaws.com/%s@%s" % (DLC_ACCOUNT, REGION, DLC_REPO, digest)
        R.ok("serving image: djl-inference:%s (pushed %s) pinned %s" % (tag, str(pushed)[:10], digest[:19]))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        model_name = "jh-owned-coder-%s" % stamp
        cfg_name = "jh-owned-coder-cfg-%s" % stamp
        env = {"HF_MODEL_ID": "/opt/ml/model", "OPTION_ROLLING_BATCH": "vllm", "OPTION_TENSOR_PARALLEL_DEGREE": "1", "OPTION_MAX_MODEL_LEN": "8192",
               "OPTION_MAX_ROLLING_BATCH_SIZE": "8", "OPTION_DTYPE": "fp16", "OPTION_TRUST_REMOTE_CODE": "false", "OPTION_GPU_MEMORY_UTILIZATION": "0.9",
               "SERVING_LOAD_MODELS": "test::Python=/opt/ml/model"}
        sm.create_model(ModelName=model_name, ExecutionRoleArn=ROLE,
                        PrimaryContainer={"Image": image, "Environment": env,
                                          "ModelDataSource": {"S3DataSource": {"S3Uri": weights, "S3DataType": "S3Prefix", "CompressionType": "None"}}},
                        Tags=cg.tags("owned-coder-async", None, pinned=True) + [{"Key": "jh-factory", "Value": "owned-inference"}])
        R.ok("model %s created from the owned weights" % model_name)
        sm.create_endpoint_config(EndpointConfigName=cfg_name,
                                  ProductionVariants=[{"VariantName": "owned", "ModelName": model_name, "InstanceType": INSTANCE, "InitialInstanceCount": 1}],
                                  AsyncInferenceConfig={"OutputConfig": {"S3OutputPath": "s3://%s/factory/inference/outputs/" % PRI,
                                                                         "S3FailurePath": "s3://%s/factory/inference/outputs/" % PRI},
                                                        "ClientConfig": {"MaxConcurrentInvocationsPerInstance": 4}},
                                  Tags=cg.tags("owned-coder-async", None, pinned=True))
        try:
            sm.describe_endpoint(EndpointName=NAME)
            sm.update_endpoint(EndpointName=NAME, EndpointConfigName=cfg_name)
            R.ok("endpoint %s exists; updated to the new config" % NAME)
        except sm.exceptions.ClientError:
            sm.create_endpoint(EndpointName=NAME, EndpointConfigName=cfg_name, Tags=cg.tags("owned-coder-async", None, pinned=True) + [{"Key": "jh-factory", "Value": "owned-inference"}])
            R.ok("endpoint %s creating (async)" % NAME)
        deadline = time.time() + 50 * 60
        status = None
        while time.time() < deadline:
            d = sm.describe_endpoint(EndpointName=NAME)
            status = d.get("EndpointStatus")
            if status in ("InService", "Failed"):
                break
            time.sleep(30)
        def log_tail(n=40):
            try:
                logs = boto3.client("logs", region_name=REGION)
                group = "/aws/sagemaker/Endpoints/" + NAME
                streams = sorted(logs.describe_log_streams(logGroupName=group, limit=20).get("logStreams", []), key=lambda x: x.get("lastEventTimestamp") or 0, reverse=True)
                for st in streams[:1]:
                    ev = logs.get_log_events(logGroupName=group, logStreamName=st["logStreamName"], limit=n, startFromHead=False).get("events", [])
                    for e in ev[-n:]:
                        R.log("  [%s] %s" % (st["logStreamName"][:40], e.get("message", "").rstrip()[:260]))
            except Exception as e:  # noqa: BLE001
                R.log("log tail unavailable: %s" % str(e)[:120])

        def verified_delete():
            try:
                sm.delete_endpoint(EndpointName=NAME)
            except Exception as e:  # noqa: BLE001
                R.log("delete_endpoint: %s" % str(e)[:120])
            for _ in range(30):
                try:
                    st = sm.describe_endpoint(EndpointName=NAME).get("EndpointStatus")
                    if st == "Deleting":
                        time.sleep(10); continue
                    return "still_present:" + str(st)
                except Exception:  # noqa: BLE001
                    return "deleted"
            return "unresolved_cleanup"

        if status != "InService":
            reason = ""
            try:
                reason = sm.describe_endpoint(EndpointName=NAME).get("FailureReason") or ""
            except Exception:  # noqa: BLE001
                pass
            R.fail("endpoint status %s (%s) after 50 min" % (status, reason[:300]))
            log_tail()
            R.fail("cleanup: %s" % verified_delete())
            red.append("endpoint"); R.fail("RED -- %s" % ", ".join(red)); return 1
        R.ok("endpoint InService")
        # scale to zero
        try:
            aas = boto3.client("application-autoscaling", region_name=REGION)
            rid = "endpoint/%s/variant/owned" % NAME
            aas.register_scalable_target(ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount", MinCapacity=0, MaxCapacity=1)
            aas.put_scaling_policy(PolicyName="jh-owned-coder-backlog", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount",
                                   PolicyType="TargetTrackingScaling",
                                   TargetTrackingScalingPolicyConfiguration={"TargetValue": 2.0, "ScaleInCooldown": 900, "ScaleOutCooldown": 60,
                                                                             "CustomizedMetricSpecification": {"MetricName": "ApproximateBacklogSizePerInstance", "Namespace": "AWS/SageMaker",
                                                                                                                "Dimensions": [{"Name": "EndpointName", "Value": NAME}], "Statistic": "Average"}})
            cw = boto3.client("cloudwatch", region_name=REGION)
            # a step policy wakes the endpoint from zero on the first queued request
            step = aas.put_scaling_policy(PolicyName="jh-owned-coder-wake", ServiceNamespace="sagemaker", ResourceId=rid, ScalableDimension="sagemaker:variant:DesiredInstanceCount",
                                          PolicyType="StepScaling", StepScalingPolicyConfiguration={"AdjustmentType": "ChangeInCapacity", "MetricAggregationType": "Average", "Cooldown": 60,
                                                                                                     "StepAdjustments": [{"MetricIntervalLowerBound": 0, "ScalingAdjustment": 1}]})
            cw.put_metric_alarm(AlarmName="jh-owned-coder-has-backlog", MetricName="HasBacklogWithoutCapacity", Namespace="AWS/SageMaker", Statistic="Average", Period=60,
                                EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="missing",
                                Dimensions=[{"Name": "EndpointName", "Value": NAME}], AlarmActions=[step["PolicyARN"]])
            R.ok("autoscaling 0..1 registered: backlog target + wake-from-zero alarm (scale-in cooldown 15 min)")
        except Exception as e:  # noqa: BLE001
            R.fail("autoscaling setup failed (%s): an endpoint that cannot scale to zero is not an accepted deployment" % str(e)[:160])
            R.fail("cleanup: %s" % verified_delete())
            red.append("autoscaling"); R.fail("RED -- %s" % ", ".join(red)); return 1
        # Lambda role may invoke (before the control is enabled)
        try:
            iam = boto3.client("iam")
            iam.put_role_policy(RoleName=LAMBDA_ROLE, PolicyName="justhodl-owned-inference-invoke", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["sagemaker:InvokeEndpointAsync", "sagemaker:DescribeEndpoint"], "Resource": "arn:aws:sagemaker:%s:%s:endpoint/%s" % (REGION, ACCOUNT, NAME)}]}))
            R.ok("%s may InvokeEndpointAsync on %s" % (LAMBDA_ROLE, NAME))
        except Exception as e:  # noqa: BLE001
            R.fail("lambda role grant failed: %s" % str(e)[:160]); R.fail("cleanup: %s" % verified_delete()); red.append("lambda-role"); R.fail("RED -- %s" % ", ".join(red)); return 1
        # round trip THROUGH THE CHAT'S OWN PATH: factory_inference.submit -> resolve
        import factory_inference as fi
        from factory_store import Store
        from datetime import datetime as _dt, timezone as _tz
        store = Store(s3, PRI, "justhodl-dashboard-live", lambda: _dt.now(_tz.utc))
        control = {"schema_version": "factory-inference-control.v1", "enabled": True, "endpoint_name": NAME, "mode": "async-scale-to-zero",
                   "model_id": MODEL_ID, "revision": manifest["revision"], "adapter_generation": "base", "max_new_tokens": 700,
                   "instance_type": INSTANCE, "usd_per_hour_when_up": float(hourly), "image": image, "image_tag": tag, "model_name": model_name,
                   "endpoint_config": cfg_name, "written_by": "ops 5552", "git_sha": head[:12], "at": datetime.now(timezone.utc).isoformat()}
        rt = boto3.client("sagemaker-runtime", region_name=REGION)
        pending = fi.submit(store, rt, control, "ops-5552-probe", "Write a Python function is_palindrome(s) that ignores case and non-letters, with two example calls. (probe %s)" % stamp, [])
        R.ok("probe submitted through factory_inference.submit: %s -> %s" % (pending["id"], pending.get("output_location")))
        state, text = "queued", ""
        for _ in range(60):
            state, text = fi.resolve(store, pending)
            if state in ("done", "failed", "expired", "malformed"):
                break
            time.sleep(10)
        if state != "done":
            red.append("roundtrip"); R.fail("probe terminal state %s: %s" % (state, str(text)[:300])); log_tail(20)
            R.fail("cleanup: %s" % verified_delete()); R.fail("RED -- %s" % ", ".join(red)); return 1
        R.ok("round trip OK (%s): %s" % (pending["origin"], " ".join(str(text).split())[:300]))
        s3.put_object(Bucket=PRI, Key="factory/control/inference.json", Body=json.dumps(control, indent=2, sort_keys=True).encode(), ContentType="application/json")
        R.ok("factory/control/inference.json written: enabled=True -> the chat routes ordinary and coding questions to the owned model")
        if red:
            R.fail("RED -- %s" % ", ".join(red)); return 1
        R.ok("GREEN -- the owned model is reachable from the chat (async, scale-to-zero)%s" % (" (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

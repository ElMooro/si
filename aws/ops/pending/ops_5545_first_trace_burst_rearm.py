"""ops 5545 -- re-arm of 5544: the curriculum now exists (factory-code-exam run 34793780925 wrote the verified MBPP rows
and froze the HumanEval exam + holdout manifest). Same launch, same caps.

ops 5544 -- the first owned trace burst (Claude, 2026-09-14).

Closes the loop physically: the champion base (Qwen2.5-Coder-7B-Instruct, owned, hashed) samples K=4 candidates
per public task on ONE managed-spot g5.2xlarge, priced live and capped; the verifier workflow then executes every
candidate in a network-less container and keeps only passes as training rows (kind=self_trace, license own).

  1. tasks: prompts only (never tests or solutions) from the runner-verified curriculum rows under
     factory/curriculum/code/verified/, holdout task ids excluded, cap 1000 -> factory/bursts/tasks/<stamp>/tasks.jsonl
  2. runner group: training-job rights for jh-burst-* (Create/Describe/Stop/AddTags) + PassRole -- group route
  3. price ml.g5.2xlarge (training family) -> cap = on-demand $/h x 2 h; refuse unpriced
  4. create the training job on the digest-pinned image with generate.py (burst_spec), spot, tags, record
     factory/bursts/jobs/<name>.json; the verify workflow is dispatched by hand with that name when it completes.
"""
from __future__ import annotations

import json
import re
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
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRI = "justhodl-ai-857687956942"
ROLE = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"
INSTANCE, MAX_S, TASK_CAP, K, TEMP = "ml.g5.2xlarge", 2 * 3600, 1000, 4, 0.8
VERIFIED = "factory/curriculum/code/verified/"


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    red, warn = [], []
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import cost_guard as cg
    import gear_b_own as own
    with report("ops_5545_first_trace_burst_rearm") as R:
        R.heading("ops 5545 (re-arm of 5544) -- first owned trace burst: prompts-only tasks, runner rights, live price, spot job on the digest-pinned image")
        R.kv(head=head[:10])
        control = _get_json(s3, "factory/control/gearb.json") or {}
        if control.get("model_source") != "own" or not control.get("enabled"):
            red.append("control"); R.fail("gearb control is not enabled on the owned lane: %s" % json.dumps({k: control.get(k) for k in ("enabled", "model_source", "model_id")}))
            R.fail("RED -- %s" % ", ".join(red)); return 1
        # 1. tasks (prompts only)
        manifest = _get_json(s3, "factory/holdout/manifest.json") or {}
        holdout = set((manifest.get("code") or {}).get("task_ids") or [])
        tasks, seen, token, scanned = [], set(), None, 0
        while len(tasks) < TASK_CAP:
            kw = {"Bucket": PRI, "Prefix": VERIFIED, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                scanned += 1
                doc = _get_json(s3, o["Key"]) or {}
                tid = str(doc.get("task_id") or "")
                if not tid or tid in seen or tid in holdout or doc.get("kind") == "self_trace" or not isinstance(doc.get("prompt"), str):
                    continue
                seen.add(tid)
                tasks.append({"task_id": tid, "prompt": doc["prompt"], "family": str(doc.get("family") or "code"), "holdout": False})
                if len(tasks) >= TASK_CAP:
                    break
            token = resp.get("NextContinuationToken")
            if not resp.get("IsTruncated") or not token:
                break
        if len(tasks) < 20:
            red.append("tasks"); R.fail("only %d prompts available under %s (scanned %d); run the code-exam curriculum first" % (len(tasks), VERIFIED, scanned))
            R.fail("RED -- %s" % ", ".join(red)); return 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        body = "\n".join(json.dumps(t, sort_keys=True) for t in tasks) + "\n"
        tasks_prefix = "factory/bursts/tasks/%s/" % stamp
        s3.put_object(Bucket=PRI, Key=tasks_prefix + "tasks.jsonl", Body=body.encode(), ContentType="application/jsonl", IfNoneMatch="*")
        fam = {}
        for t in tasks:
            fam[t["family"]] = fam.get(t["family"], 0) + 1
        R.ok("tasks: %d prompts (holdout excluded %d, scanned %d) families=%s -> s3://%s/%s" % (len(tasks), len(holdout), scanned, json.dumps(fam), PRI, tasks_prefix))
        # 2. runner rights for training jobs (group route; per-user quotas are exhausted)
        try:
            iam = boto3.client("iam")
            user = boto3.client("sts").get_caller_identity()["Arn"].split("/")[-1]
            parn = "arn:aws:iam::%s:policy/justhodl-runner-training-bursts" % ACCOUNT
            try:
                iam.create_policy(PolicyName="justhodl-runner-training-bursts", Description="Runner: create/describe tagged jh-burst-* training jobs (owned lane) + PassRole",
                                  PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
                                      {"Effect": "Allow", "Action": ["sagemaker:CreateTrainingJob", "sagemaker:DescribeTrainingJob", "sagemaker:StopTrainingJob", "sagemaker:AddTags", "sagemaker:ListTags"],
                                       "Resource": "arn:aws:sagemaker:%s:%s:training-job/*" % (REGION, ACCOUNT)},
                                      {"Effect": "Allow", "Action": ["iam:PassRole"], "Resource": ROLE, "Condition": {"StringEquals": {"iam:PassedToService": "sagemaker.amazonaws.com"}}}]}))
            except iam.exceptions.EntityAlreadyExistsException:
                pass
            iam.attach_group_policy(GroupName="justhodl-runner-ecr", PolicyArn=parn)
            iam.add_user_to_group(GroupName="justhodl-runner-ecr", UserName=user)
            R.ok("training-job rights attached to group justhodl-runner-ecr for %s; waiting 90 s" % user); time.sleep(90)
        except Exception as e:  # noqa: BLE001
            warn.append("iam"); R.warn("training-job grant failed: %s" % str(e)[:160])
        # 3. price
        pricing = boto3.client("pricing", region_name="us-east-1")
        price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="training")
        hourly = price.get("usd_per_hour")
        if not hourly:
            red.append("price"); R.fail("no live training price for %s -- refusing unpriced spend (%s)" % (INSTANCE, price.get("error")))
            R.fail("RED -- %s" % ", ".join(red)); return 1
        cap = round(float(hourly) * MAX_S / 3600.0, 4)
        R.ok("training price %s = $%.4f/h (%s); burst cap $%.4f at %ds (spot bills less)" % (INSTANCE, float(hourly), price.get("source"), cap, MAX_S))
        # 4. job
        spec = own.burst_spec(s3, PRI, control, mode="burst", tasks_uri="s3://%s/%s" % (PRI, tasks_prefix), samples_per_task=K, temperature=TEMP)
        name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-burst-gen0-%s" % stamp)[:63]
        hp = {k: str(v["default"]) for k, v in spec["hyperparameters"].items()}
        hp.update({"sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20", "sagemaker_region": REGION,
                   "sagemaker_job_name": name, "task_cap": str(TASK_CAP), "max_model_len": "8192"})
        channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                    {"ChannelName": "tasks", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["tasks_uri"], "S3DataDistributionType": "FullyReplicated"}}}]
        out_uri = "s3://%s/factory/bursts/" % PRI
        record = {"schema_version": "factory-burst-job.v1", "job_name": name, "kind": "burst", "generation": 0, "model_id": spec["model_id"],
                  "training_image": spec["training_image"], "bundle_uri": spec["training_script"], "bundle_sha256": spec["training_bundle_sha256"],
                  "tasks_uri": spec["tasks_uri"], "tasks": len(tasks), "samples_per_task": K, "temperature": TEMP, "instance_type": INSTANCE,
                  "spot": True, "max_runtime_s": MAX_S, "usd_per_hour": float(hourly), "cap_usd": cap, "out_uri": out_uri,
                  "launched_at": datetime.now(timezone.utc).isoformat(), "commit": head[:12], "status": "launching",
                  "verify": "dispatch factory-trace-verify.yml with burst=%s when Completed" % name}
        s3.put_object(Bucket=PRI, Key="factory/bursts/jobs/%s.json" % name, Body=json.dumps(record, indent=2, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
        kw = dict(TrainingJobName=name, RoleArn=ROLE,
                  AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
                  HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
                  ResourceConfig={"InstanceType": INSTANCE, "InstanceCount": 1, "VolumeSizeInGB": 120},
                  StoppingCondition={"MaxRuntimeInSeconds": MAX_S, "MaxWaitTimeInSeconds": MAX_S + 3600}, EnableManagedSpotTraining=True,
                  Environment={"JH_BURST": name})
        try:
            sm.create_training_job(**kw, Tags=cg.tags("factory-trace-burst", 3) + [{"Key": "jh-factory", "Value": "burst-gen-0"}])
        except Exception as e:  # noqa: BLE001
            if "AddTags" not in str(e):
                raise
            R.warn("tagged create denied (propagation); launching untagged once"); sm.create_training_job(**kw)
        R.ok("burst %s launched: %d tasks x K=%d at T=%.1f on %s spot, image %s (cap $%.4f)" % (name, len(tasks), K, TEMP, INSTANCE, spec["training_image"].split("@")[-1][:19], cap))
        R.kv(burst=name, cap_usd=cap, tasks=len(tasks))
        if red:
            R.fail("RED -- %s" % ", ".join(red)); return 1
        R.ok("GREEN -- first burst running; on Completed, dispatch factory-trace-verify.yml (burst=%s) to keep only passes%s" % (name, " (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

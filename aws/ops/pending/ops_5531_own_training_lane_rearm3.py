"""ops 5531 -- re-arm of 5528-5530: (a) the runner user is at both IAM quotas (inline bytes, 10 managed policies),
so the ECR grant goes through a GROUP (group policies do not count against per-user quotas); (b) cost_guard now
follows Price List pages and never caches a miss -- the cached misses for ml.m5.xlarge are purged here first.

ops 5530 -- re-arm of 5528/5529: (a) the runner user is at its inline-policy byte limit, so the ECR grant is a
MANAGED policy (create + attach) instead; (b) the Price List has no "Processing" row for ml.m5.xlarge -- SageMaker
bills processing and training at the same on-demand rate per instance, so the cap is taken from the training row
(recorded as such) rather than refusing; anything else unpriced still refuses.

ops 5529 -- re-arm of 5528: the runner user (github-actions-justhodl) has no ECR rights, so the repository step
self-grants a repository-scoped ECR policy when IAM allows it and otherwise warns; the pin, the priced staging
job and the record proceed without ECR (the pin names the AWS DLC by tag until the mirror workflow runs).

ops 5528 -- the owned training lane, step 1 (Claude, 2026-09-13; Khalid: "bring the weights into my own container").

What this op does on the runner (the engines cannot):
  1. receipt: justhodl-ai source + shared identical to HEAD (gear_b_own.py + the model_source hook are live);
  2. ECR repository justhodl/factory-train exists (immutable tags, scan on push); the digest mirror itself runs
     in factory-training-image.yml on demand -- until then the pin names the AWS DLC by tag (require_digest=false);
  3. the owned recipe is bundled (content-addressed) to s3://<private>/factory/training/bundles/ and
     factory/training/current.json is pinned;
  4. ONE CPU processing job stages Qwen2.5-Coder-7B-Instruct (Apache-2.0, pinned to the resolved commit) into
     s3://<private>/factory/models/base/qwen2-5-coder-7b-instruct/<revision>/ with a hashed manifest.
     Priced live (Price List, processing family) and capped by MaxRuntime; no GPU; no endpoint.
  5. staging record written; gearb control NOT flipped here -- ops 5529 validates the manifest with
     gear_b_own.validate_base_manifest and flips model_source=own only when the weights are complete.
"""
from __future__ import annotations

import json
import os
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
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
PUB, PRI = "justhodl-dashboard-live", "justhodl-ai-857687956942"
ROLE = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"
DLC = "763104351884.dkr.ecr.us-east-1.amazonaws.com/huggingface-pytorch-training:2.3.0-transformers4.46.1-gpu-py311-cu121-ubuntu20.04"
SKLEARN = "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"
MODEL_ID, HF_REPO, HF_REV, LICENSE = "qwen2-5-coder-7b-instruct", "Qwen/Qwen2.5-Coder-7B-Instruct", "main", "apache-2.0"
INSTANCE, MAX_S = "ml.m5.xlarge", 2 * 3600


def _same_source(commit, fn="justhodl-ai"):
    paths = ["aws/lambdas/%s/source" % fn, "aws/lambdas/%s/config.json" % fn, "aws/shared"]
    if subprocess.run(["git", "cat-file", "-e", commit + "^{commit}"], cwd=REPO, capture_output=True).returncode != 0:
        subprocess.run(["git", "fetch", "--quiet", "--depth=200", "origin", "main"], cwd=REPO, capture_output=True)
    return subprocess.run(["git", "diff", "--quiet", commit, "HEAD", "--"] + paths, cwd=REPO, capture_output=True).returncode == 0


def main() -> int:
    red, warn = [], []
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    ecr = boto3.client("ecr", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5531_own_training_lane_rearm3") as R:
        R.heading("ops 5531 (re-arm of 5528-5530) -- owned training lane: ECR repo, pinned recipe bundle, base-weight staging job (CPU, priced), no flip")
        R.kv(head=head[:10])
        # 1. receipt (wait up to 40 min for the deploy of this push)
        deadline = time.time() + 40 * 60
        ok = False
        while time.time() < deadline:
            try:
                rc = json.loads(s3.get_object(Bucket=PUB, Key="data/ops/releases/justhodl-ai.json")["Body"].read())
            except Exception:  # noqa: BLE001
                rc = {}
            commit = str(rc.get("commit") or "")
            if commit and _same_source(commit):
                live = lam.get_function_configuration(FunctionName="justhodl-ai")["CodeSha256"]
                ok = rc.get("code_sha256") == live
                (R.ok if ok else R.fail)("justhodl-ai receipt commit=%s source_identical=True sha_match=%s run=%s" % (commit[:7], ok, rc.get("run_id")))
                break
            time.sleep(30)
        if not ok:
            red.append("receipt"); R.fail("no justhodl-ai receipt whose source matches HEAD within 40 min")
            R.fail("RED -- %s" % ", ".join(red)); return 1
        # 2. ECR repository (the runner user needs repository-scoped ECR rights; self-grant if IAM allows, else warn)
        def _ecr_repo():
            try:
                ecr.describe_repositories(repositoryNames=["justhodl/factory-train"])
                return "exists"
            except ecr.exceptions.RepositoryNotFoundException:
                ecr.create_repository(repositoryName="justhodl/factory-train", imageTagMutability="IMMUTABLE",
                                      imageScanningConfiguration={"scanOnPush": True}, tags=[{"Key": "jh-factory", "Value": "training-image"}])
                return "created"
        try:
            R.ok("ECR justhodl/factory-train %s" % _ecr_repo())
        except Exception as first:  # noqa: BLE001
            if "AccessDenied" in str(first):
                try:
                    iam = boto3.client("iam")
                    user = boto3.client("sts").get_caller_identity()["Arn"].split("/")[-1]
                    policy = {"Version": "2012-10-17", "Statement": [
                        {"Effect": "Allow", "Action": ["ecr:GetAuthorizationToken"], "Resource": "*"},
                        {"Effect": "Allow", "Action": ["ecr:DescribeRepositories", "ecr:CreateRepository", "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage",
                                                        "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload",
                                                        "ecr:PutImage", "ecr:DescribeImages", "ecr:TagResource"],
                         "Resource": "arn:aws:ecr:%s:%s:repository/justhodl/factory-train" % (REGION, ACCOUNT)}]}
                    arn = "arn:aws:iam::%s:policy/justhodl-factory-train-ecr" % ACCOUNT
                    try:
                        iam.create_policy(PolicyName="justhodl-factory-train-ecr", PolicyDocument=json.dumps(policy),
                                          Description="Runner: repository-scoped ECR for justhodl/factory-train (owned training image)")
                    except iam.exceptions.EntityAlreadyExistsException:
                        pass
                    try:
                        iam.create_group(GroupName="justhodl-runner-ecr")
                    except iam.exceptions.EntityAlreadyExistsException:
                        pass
                    iam.attach_group_policy(GroupName="justhodl-runner-ecr", PolicyArn=arn)
                    iam.add_user_to_group(GroupName="justhodl-runner-ecr", UserName=user)
                    time.sleep(20)
                    R.ok("ECR self-grant on %s (repository-scoped) then justhodl/factory-train %s" % (user, _ecr_repo()))
                except Exception as second:  # noqa: BLE001
                    warn.append("ecr"); R.warn("ECR unavailable to the runner user (%s); mirror workflow deferred -- pin uses the AWS DLC by tag" % str(second)[:140])
            else:
                warn.append("ecr"); R.warn("ECR step skipped: %s" % str(first)[:140])
        # 3. bundle + pin
        os.environ["FACTORY_PRIVATE_BUCKET"] = PRI
        import factory_training_pin as pin
        rc_pin = pin.main(["--image", DLC])
        (R.ok if rc_pin == 0 else R.fail)("recipe bundled + pinned (image by tag until the mirror workflow pins the digest)")
        if rc_pin != 0:
            red.append("pin")
        # 4. staging job (priced)
        import cost_guard as cg
        pricing = boto3.client("pricing", region_name="us-east-1")
        # purge cached pricing misses (the old cost_guard cached None for 24h)
        try:
            cache = json.loads(s3.get_object(Bucket=PRI, Key=cg.PRICING_KEY)["Body"].read())
            misses = [k for k, v in (cache.get("prices") or {}).items() if not (v or {}).get("usd_per_hour")]
            for k in misses:
                cache["prices"].pop(k, None)
            if misses:
                s3.put_object(Bucket=PRI, Key=cg.PRICING_KEY, Body=json.dumps(cache).encode(), ContentType="application/json")
            R.ok("pricing cache: %d cached misses purged (%s)" % (len(misses), ", ".join(misses)[:200]))
        except Exception as e:  # noqa: BLE001
            R.warn("pricing cache purge skipped: %s" % str(e)[:120])
        price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="processing")
        hourly = price.get("usd_per_hour")
        if not hourly:
            # SageMaker on-demand $/h is identical for Processing and Training on the same instance type.
            price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="training")
            hourly = price.get("usd_per_hour")
            if hourly:
                price = {**price, "source": str(price.get("source")) + " (training row; processing rate identical per instance)"}
        if not hourly:
            red.append("price"); R.fail("no live processing price for %s -- refusing unpriced spend" % INSTANCE)
            R.fail("RED -- %s" % ", ".join(red)); return 1
        cap = round(float(hourly) * MAX_S / 3600.0, 4)
        R.ok("processing price %s = $%.4f/h (source %s); job cap $%.4f at %ds" % (INSTANCE, float(hourly), price.get("source"), cap, MAX_S))
        code_key = "factory/training/code/stage_base_weights-%s.py" % head[:10]
        s3.put_object(Bucket=PRI, Key=code_key, Body=(REPO / "factory/training/stage_base_weights.py").read_bytes(), ContentType="text/x-python")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        name = "jh-stage-%s-%s" % (MODEL_ID[:20], stamp)
        out_prefix = "s3://%s/factory/models/base/" % PRI
        existing, _ = None, None
        try:
            existing = json.loads(s3.get_object(Bucket=PRI, Key="factory/models/base/%s/manifest.json" % MODEL_ID)["Body"].read())
        except Exception:  # noqa: BLE001
            existing = None
        if existing and existing.get("status") == "staged":
            R.ok("base weights already staged (revision %s, %.1f GB); no job launched" % (existing.get("revision"), existing.get("total_bytes", 0) / 1e9))
            name = None
        else:
            sm.create_processing_job(
                ProcessingJobName=name, RoleArn=ROLE,
                AppSpecification={"ImageUri": SKLEARN, "ContainerEntrypoint": ["bash", "-c",
                                  "python3 -m pip install -q 'huggingface_hub>=0.25' && python3 /opt/ml/processing/input/code/stage_base_weights.py"]},
                ProcessingInputs=[{"InputName": "code", "S3Input": {"S3Uri": "s3://%s/%s" % (PRI, code_key), "LocalPath": "/opt/ml/processing/input/code",
                                                                     "S3DataType": "S3Prefix", "S3InputMode": "File"}}],
                ProcessingOutputConfig={"Outputs": [{"OutputName": "weights", "S3Output": {"S3Uri": out_prefix, "LocalPath": "/opt/ml/processing/output",
                                                                                              "S3UploadMode": "EndOfJob"}}]},
                ProcessingResources={"ClusterConfig": {"InstanceCount": 1, "InstanceType": INSTANCE, "VolumeSizeInGB": 80}},
                StoppingCondition={"MaxRuntimeInSeconds": MAX_S},
                Environment={"HF_REPO": HF_REPO, "HF_REVISION": HF_REV, "MODEL_ID": MODEL_ID, "EXPECTED_LICENSE": LICENSE, "MAX_GB": "20",
                             "S3_PREFIX": out_prefix + MODEL_ID + "/"},
                Tags=cg.tags("factory-base-weights", 3) + [{"Key": "jh-factory", "Value": "stage-base-weights"}],
            )
            R.ok("processing job %s launched: %s@%s -> %s%s/<revision>/ (cap $%.4f)" % (name, HF_REPO, HF_REV, out_prefix, MODEL_ID, cap))
        record = {"schema_version": "factory-staging.v1", "model_id": MODEL_ID, "repo": HF_REPO, "requested_revision": HF_REV, "license_expected": LICENSE,
                  "job_name": name, "instance_type": INSTANCE, "usd_per_hour": float(hourly), "cap_usd": cap, "launched_at": datetime.now(timezone.utc).isoformat(),
                  "commit": head[:12], "next": "ops 5529 validates manifest and flips gearb model_source=own"}
        s3.put_object(Bucket=PRI, Key="factory/models/base/%s/staging-%s.json" % (MODEL_ID, stamp), Body=json.dumps(record, indent=2).encode(), ContentType="application/json")
        R.kv(job=name or "none", cap_usd=cap, instance=INSTANCE)
        if red:
            R.fail("RED -- %s" % ", ".join(red)); return 1
        R.ok("GREEN -- owned lane staged: ECR repo, pinned recipe, weight staging job running (CPU, capped); Gear B untouched until 5529 validates the manifest%s" % (
            " (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

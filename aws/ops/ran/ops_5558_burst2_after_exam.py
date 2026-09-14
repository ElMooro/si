"""ops 5558 -- burst 2 only, serialized behind the exam: the account allows ONE spot ml.g5.2xlarge training instance at a time
(ResourceLimitExceeded on 5557 while the base exam ran). This op waits for the exam job to leave the quota, then launches
burst 2 (MBPP+APPS, K=6). The exam is graded separately by factory-exam.yml.

ops 5557 -- step 1 of "make step 3 happen": the base exam + burst 2 (Claude, 2026-09-14).

ops 5546 -- re-arm of 5545: CreateTrainingJob rejected the image reference (InvalidImageDigest) because the pin
carried tag+digest; own_spec now references a digest-pinned image by digest alone. Same launch, same caps.

ops 5545 -- re-arm of 5544: the curriculum now exists (factory-code-exam run 34793780925 wrote the verified MBPP rows
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
    """Step 1 + supply: (a) the BASE EXAM job on the frozen HumanEval prompts (prompts only, greedy, exam mode) so the
    reference score exists; (b) burst 2 over MBPP + APPS at K=6 so the v4-judged supply crosses the 1,500-row floor."""
    red, warn = [], []
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import cost_guard as cg
    import gear_b_own as own
    with report("ops_5558_burst2_after_exam") as R:
        R.heading("ops 5558 -- burst 2 (MBPP+APPS, K=6) after the base exam job frees the single spot g5.2xlarge")
        R.kv(head=head[:10])
        control = _get_json(s3, "factory/control/gearb.json") or {}
        if control.get("model_source") != "own" or not control.get("enabled"):
            red.append("control"); R.fail("gearb control not on the owned lane"); R.fail("RED"); return 1
        # ---- wait for the single spot g5.2xlarge to be free (exam or burst jobs) -- account quota = 1
        deadline = time.time() + 45 * 60
        while time.time() < deadline:
            busy = []
            for prefix in ("jh-exam-", "jh-burst-"):
                for j in sm.list_training_jobs(NameContains=prefix, StatusEquals="InProgress", MaxResults=10).get("TrainingJobSummaries", []):
                    busy.append(j["TrainingJobName"])
            if not busy:
                break
            R.log("quota busy: %s; waiting" % ", ".join(busy)); time.sleep(60)
        else:
            red.append("quota"); R.fail("spot g5.2xlarge still busy after 45 min"); R.fail("RED"); return 1
        R.ok("spot g5.2xlarge free")
        pricing = boto3.client("pricing", region_name="us-east-1")
        price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="training")
        hourly = price.get("usd_per_hour")
        if not hourly:
            red.append("price"); R.fail("no live training price for %s" % INSTANCE); R.fail("RED"); return 1
        cap = round(float(hourly) * MAX_S / 3600.0, 4)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

        def launch(spec, name, tasks_uri, kind, extra_hp, extra_record):
            hp = {k: str(v["default"]) for k, v in spec["hyperparameters"].items()}
            hp.update({"sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20", "sagemaker_region": REGION,
                       "sagemaker_job_name": name, "max_model_len": "8192", **extra_hp})
            channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                        {"ChannelName": "tasks", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": tasks_uri, "S3DataDistributionType": "FullyReplicated"}}}]
            out_uri = "s3://%s/factory/bursts/" % PRI
            record = {"schema_version": "factory-burst-job.v1", "job_name": name, "kind": kind, "generation": 0, "model_id": spec["model_id"],
                      "training_image": spec["training_image"], "bundle_uri": spec["training_script"], "bundle_sha256": spec["training_bundle_sha256"],
                      "tasks_uri": tasks_uri, "instance_type": INSTANCE, "spot": True, "max_runtime_s": MAX_S, "usd_per_hour": float(hourly), "cap_usd": cap,
                      "out_uri": out_uri, "launched_at": datetime.now(timezone.utc).isoformat(), "commit": head[:12], "status": "launching", **extra_record}
            s3.put_object(Bucket=PRI, Key="factory/bursts/jobs/%s.json" % name, Body=json.dumps(record, indent=2, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
            kw = dict(TrainingJobName=name, RoleArn=ROLE, AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
                      HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
                      ResourceConfig={"InstanceType": INSTANCE, "InstanceCount": 1, "VolumeSizeInGB": 120},
                      StoppingCondition={"MaxRuntimeInSeconds": MAX_S, "MaxWaitTimeInSeconds": MAX_S + 3600}, EnableManagedSpotTraining=True, Environment={"JH_BURST": name})
            try:
                sm.create_training_job(**kw, Tags=cg.tags("factory-" + kind, 3) + [{"Key": "jh-factory", "Value": kind}])
            except Exception as e:  # noqa: BLE001
                if "AddTags" not in str(e):
                    raise
                sm.create_training_job(**kw)
            return record

        # ---- (b) burst 2 for supply (K=6)
        manifest = _get_json(s3, "factory/holdout/manifest.json") or {}
        holdout = set((manifest.get("code") or {}).get("task_ids") or [])
        if not holdout:
            red.append("holdout"); R.fail("holdout manifest has no code task_ids; refusing to build tasks"); R.fail("RED"); return 1
        tasks2, seen, token, scanned = [], set(), None, 0
        while len(tasks2) < 1200:
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
                seen.add(tid); tasks2.append({"task_id": tid, "prompt": doc["prompt"], "family": str(doc.get("family") or "code"), "holdout": False})
            token = resp.get("NextContinuationToken")
            if not resp.get("IsTruncated") or not token:
                break
        prefix2 = "factory/bursts/tasks/%s/" % stamp
        s3.put_object(Bucket=PRI, Key=prefix2 + "tasks.jsonl", Body=("\n".join(json.dumps(t, sort_keys=True) for t in tasks2) + "\n").encode(), ContentType="application/jsonl", IfNoneMatch="*")
        spec2 = own.burst_spec(s3, PRI, control, mode="burst", tasks_uri="s3://%s/%s" % (PRI, prefix2), samples_per_task=6, temperature=0.8)
        name2 = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-burst-gen0b2-%s" % stamp)[:63]
        launch(spec2, name2, "s3://%s/%s" % (PRI, prefix2), "burst", {"task_cap": "1200"}, {"burst_index": 2, "tasks": len(tasks2), "samples_per_task": 6, "temperature": 0.8})
        R.ok("BURST 2 launched: %s -- %d prompts x K=6 (holdout excluded %d); verify with factory-trace-verify.yml all_samples=true" % (name2, len(tasks2), len(holdout)))
        R.kv(burst_job=name2, cap_usd=cap)
        R.ok("GREEN -- burst 2 running on the owned lane (quota serialized)")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

"""ops 5575 -- re-arm of 5572 for the relaunched generation-1 job (found from Gear B job records, newest jh-gearb-gen*).

ops 5572 -- step 3: examine generation 1 on the frozen holdout (Claude, 2026-09-15).

Waits for jh-gearb-gen1-20260915-024201 (QLoRA on 570 unique verified tasks, cap $4.55) to complete, verifies the
training manifest, extracts the adapter from the job's model.tar.gz into factory/champions/gen-1/adapter/ (adapter_config
+ weights + tokenizer, hashed), and launches the exam job with that adapter through the same owned lane (generate.py
--mode exam, prompts only, greedy). Grading = factory-exam.yml (burst=<exam job>, generation=gen-1) -> the promotion-shaped
evaluation; the scorecard then computes LEARNING = candidate - base on evaluation humaneval-frozen-848505e1dd021797.
If the training job is still running when the runner's window ends, this op says so and is re-armed; nothing is guessed.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
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

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
ROLE = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"
TRAIN_JOB = None   # resolved from factory/gearb/jobs/ (newest gen-1 job)
INSTANCE, MAX_S = "ml.g5.2xlarge", 2 * 3600
WAIT_S = 70 * 60


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import cost_guard as cg
    import gear_b_own as own
    with report("ops_5575_gen1_adapter_exam2") as R:
        R.heading("ops 5572 -- generation 1: wait for training, extract the adapter, launch the frozen exam with it")
        global TRAIN_JOB
        recs = []
        for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/gearb/jobs/", MaxKeys=200).get("Contents", []):
            d0 = _get_json(s3, o["Key"]) or {}
            if str(d0.get("job_name") or "").startswith("jh-gearb-gen") and d0.get("status") not in ("Failed", "Stopped", "error"):
                recs.append(d0)
        if not recs:
            R.fail("no live gen job record under factory/gearb/jobs/"); return 1
        TRAIN_JOB = sorted(recs, key=lambda r: str(r.get("launched_at") or ""))[-1]["job_name"]
        R.kv(head=head[:10], train_job=TRAIN_JOB)
        deadline = time.time() + WAIT_S
        status = None
        while time.time() < deadline:
            d = sm.describe_training_job(TrainingJobName=TRAIN_JOB)
            status = d.get("TrainingJobStatus")
            if status in ("Completed", "Failed", "Stopped"):
                break
            time.sleep(60)
        d = sm.describe_training_job(TrainingJobName=TRAIN_JOB)
        R.ok("training job %s status=%s secondary=%s train_s=%s billable_s=%s reason=%s" % (
            TRAIN_JOB, status, d.get("SecondaryStatus"), d.get("TrainingTimeInSeconds"), d.get("BillableTimeInSeconds"), (d.get("FailureReason") or "")[:200]))
        if status != "Completed":
            R.fail("generation 1 is not complete (%s); re-arm when it is -- nothing launched" % status); return 1
        artifact = d["ModelArtifacts"]["S3ModelArtifacts"]
        key = artifact.split(PRI + "/", 1)[1]
        blob = s3.get_object(Bucket=PRI, Key=key)["Body"].read()
        tf = tarfile.open(fileobj=io.BytesIO(blob))
        names = tf.getnames()
        manifest = json.loads(tf.extractfile([n for n in names if n.endswith("train_manifest.json")][0]).read())
        R.ok("train manifest: status=%s rows=%s steps=%s loss=%s trainable_params=%s adapter_sha=%s base_rev=%s" % (
            manifest.get("status"), manifest.get("rows"), manifest.get("steps"), manifest.get("train_loss"), manifest.get("trainable_parameters"),
            str(manifest.get("adapter_sha256"))[:12], str(manifest.get("base_revision"))[:12]))
        if manifest.get("status") != "trained":
            R.fail("training manifest status %s -- refusing to examine" % manifest.get("status")); return 1
        adapter_prefix = "factory/champions/gen-1/adapter/"
        files, total = [], 0
        for m in tf.getmembers():
            if m.isfile() and "/adapter/" in ("/" + m.name) or m.name.startswith("adapter/"):
                rel = m.name.split("adapter/", 1)[1]
                data = tf.extractfile(m).read()
                s3.put_object(Bucket=PRI, Key=adapter_prefix + rel, Body=data)
                files.append({"path": rel, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}); total += len(data)
        if not any(f["path"] == "adapter_config.json" for f in files):
            R.fail("no adapter_config.json in the job output (%s)" % names[:10]); return 1
        s3.put_object(Bucket=PRI, Key="factory/champions/gen-1/manifest.json", Body=json.dumps({"schema_version": "factory-adapter.v1", "generation": 1, "job": TRAIN_JOB,
                      "artifact": artifact, "files": files, "total_bytes": total, "train_manifest": manifest, "extracted_at": datetime.now(timezone.utc).isoformat()}, indent=2, sort_keys=True).encode(),
                      ContentType="application/json")
        R.ok("adapter extracted: %d files, %.1f MB -> s3://%s/%s" % (len(files), total / 1e6, PRI, adapter_prefix))
        control = _get_json(s3, "factory/control/gearb.json") or {}
        pricing = boto3.client("pricing", region_name="us-east-1")
        price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="training")
        hourly = price.get("usd_per_hour")
        if not hourly:
            R.fail("no live price for %s" % INSTANCE); return 1
        cap = round(float(hourly) * MAX_S / 3600.0, 4)
        prefixes = sorted(o["Prefix"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/exams/code/prompts-only/", Delimiter="/").get("CommonPrefixes", []))
        tasks_uri = "s3://%s/%s" % (PRI, prefixes[-1])
        spec = own.burst_spec(s3, PRI, control, mode="exam", tasks_uri=tasks_uri, adapter_uri="s3://%s/%s" % (PRI, adapter_prefix))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-exam-gen1-%s" % stamp)[:63]
        hp = {k: str(v["default"]) for k, v in spec["hyperparameters"].items()}
        hp.update({"sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20", "sagemaker_region": REGION,
                   "sagemaker_job_name": name, "max_model_len": "8192", "task_cap": "1000", "adapter_generation": "gen-1"})
        channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                    {"ChannelName": "tasks", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": tasks_uri, "S3DataDistributionType": "FullyReplicated"}}},
                    {"ChannelName": "adapter", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3://%s/%s" % (PRI, adapter_prefix), "S3DataDistributionType": "FullyReplicated"}}}]
        out_uri = "s3://%s/factory/bursts/" % PRI
        record = {"schema_version": "factory-burst-job.v1", "job_name": name, "kind": "exam", "generation": 1, "exam_generation": "gen-1", "model_id": spec["model_id"],
                  "adapter_uri": "s3://%s/%s" % (PRI, adapter_prefix), "training_image": spec["training_image"], "bundle_uri": spec["training_script"],
                  "tasks_uri": tasks_uri, "instance_type": INSTANCE, "spot": True, "max_runtime_s": MAX_S, "usd_per_hour": float(hourly), "cap_usd": cap,
                  "out_uri": out_uri, "launched_at": datetime.now(timezone.utc).isoformat(), "commit": head[:12], "status": "launching"}
        s3.put_object(Bucket=PRI, Key="factory/bursts/jobs/%s.json" % name, Body=json.dumps(record, indent=2, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
        kw = dict(TrainingJobName=name, RoleArn=ROLE, AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
                  HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
                  ResourceConfig={"InstanceType": INSTANCE, "InstanceCount": 1, "VolumeSizeInGB": 120},
                  StoppingCondition={"MaxRuntimeInSeconds": MAX_S, "MaxWaitTimeInSeconds": MAX_S + 3600}, EnableManagedSpotTraining=True, Environment={"JH_BURST": name})
        try:
            sm.create_training_job(**kw, Tags=cg.tags("factory-exam-gen1", 3) + [{"Key": "jh-factory", "Value": "exam-gen-1"}])
        except Exception as e:  # noqa: BLE001
            if "AddTags" not in str(e):
                raise
            sm.create_training_job(**kw)
        R.ok("GEN-1 EXAM launched: %s (adapter gen-1, 164 frozen prompts, greedy); grade with factory-exam.yml burst=%s generation=gen-1" % (name, name))
        R.kv(exam_job=name, adapter=adapter_prefix, cap_usd=cap)
        R.ok("GREEN -- candidate exam running")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

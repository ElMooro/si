"""ops 5542 -- the weights ARE staged (ops 5540 job completed; manifest written). 5541 refused only because the
manifest listed SageMaker/hub cache markers (.cache/huggingface/.gitignore.sagemaker-uploaded, 0 bytes) as files.
This op curates the manifest on the runner (drops metadata paths, keeps the raw copy as manifest-raw.json,
recomputes manifest_sha256), validates with the updated gear_b_own, verifies the objects, and flips Gear B.
No job is launched; nothing is downloaded again.

ops 5541 -- re-arm of 5536/5538 for the job launched by ops 5540 (code path fixed). Owned weights validated, Gear B flipped to model_source=own (Claude, 2026-09-13).

Waits for the staging job launched by ops 5535 (jh-stage-qwen2-5-coder-7b-ins-*) to finish, validates the
manifest it wrote with gear_b_own.validate_base_manifest (schema, license apache-2.0, config.json + safetensors
shards, every file hashed), cross-checks the objects in the bucket against the manifest, proves own_spec()
builds against the pinned recipe, and only then rewrites factory/control/gearb.json:
  model_source=own, model_id=qwen2-5-coder-7b-instruct, enabled=true, budget/approval fields carried over
  from ops 5522 (Khalid: $20/day, $600/season), lora mapped to the owned recipe's names.
Nothing is trained here; the hourly Gear B hook decides launches inside its refusal chain.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PRI = "justhodl-ai-857687956942"
MODEL_ID = "qwen2-5-coder-7b-instruct"
BASE = "factory/models/base/%s/" % MODEL_ID
CONTROL_KEY = "factory/control/gearb.json"
WAIT_S = 5 * 60


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    red = []
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import gear_b_own as own
    with report("ops_5542_own_weights_curate_and_flip") as R:
        R.heading("ops 5542 (curate + flip) -- owned base weights: wait for staging, validate manifest + objects, flip Gear B to model_source=own")
        R.kv(head=head[:10], model_id=MODEL_ID)
        # the staging record names the job
        recs = s3.list_objects_v2(Bucket=PRI, Prefix=BASE + "staging-").get("Contents", [])
        job = None
        for r in sorted(recs, key=lambda x: x["Key"]):
            doc = _get_json(s3, r["Key"]) or {}
            if doc.get("job_name"):
                job = doc["job_name"]
        R.ok("staging job from record: %s" % job)
        deadline = time.time() + WAIT_S
        status = None
        while time.time() < deadline:
            if job:
                try:
                    d = sm.describe_processing_job(ProcessingJobName=job)
                    status = d.get("ProcessingJobStatus")
                    if status in ("Completed", "Failed", "Stopped"):
                        R.ok("job %s -> %s (%s)" % (job, status, (d.get("FailureReason") or d.get("ExitMessage") or "")[:160]))
                        break
                except Exception as e:  # noqa: BLE001
                    R.log("describe failed %s" % str(e)[:100])
            if _get_json(s3, BASE + "manifest.json"):
                break
            time.sleep(45)
        manifest = _get_json(s3, BASE + "manifest.json")
        if manifest and any(own.is_metadata_path(str(f.get("path") or "")) for f in manifest.get("files") or []):
            import hashlib
            raw_copy = json.dumps(manifest, indent=2, sort_keys=True)
            s3.put_object(Bucket=PRI, Key=BASE + "manifest-raw.json", Body=raw_copy.encode(), ContentType="application/json")
            kept = [f for f in manifest["files"] if not own.is_metadata_path(str(f.get("path") or ""))]
            dropped = [f["path"] for f in manifest["files"] if own.is_metadata_path(str(f.get("path") or ""))]
            manifest["files"] = kept
            manifest["total_bytes"] = sum(int(f.get("bytes") or 0) for f in kept)
            manifest["curated_by"] = "ops 5542"
            manifest["dropped_metadata_paths"] = dropped
            manifest.pop("manifest_sha256", None)
            manifest["manifest_sha256"] = hashlib.sha256(json.dumps(manifest, indent=2, sort_keys=True).encode()).hexdigest()
            s3.put_object(Bucket=PRI, Key=BASE + "manifest.json", Body=json.dumps(manifest, indent=2, sort_keys=True).encode(), ContentType="application/json")
            R.ok("manifest curated: dropped %d metadata paths (%s); raw copy kept" % (len(dropped), ", ".join(dropped)[:200]))
        if not manifest:
            red.append("manifest-missing"); R.fail("no manifest at %smanifest.json after %d min (job status %s)" % (BASE, WAIT_S // 60, status))
            R.fail("RED -- %s" % ", ".join(red)); return 1
        try:
            own.validate_base_manifest(manifest, MODEL_ID)
            R.ok("manifest valid: repo=%s revision=%s license=%s files=%d total=%.2f GB status=%s" % (
                manifest.get("repo"), str(manifest.get("revision"))[:12], manifest.get("license"), len(manifest.get("files") or []),
                float(manifest.get("total_bytes") or 0) / 1e9, manifest.get("status")))
        except own.OwnSpecRefused as e:
            red.append("manifest"); R.fail("manifest refused: %s" % e)
            R.fail("RED -- %s" % ", ".join(red)); return 1
        if manifest.get("status") != "staged":
            red.append("status"); R.fail("staging status %s" % manifest.get("status"))
            R.fail("RED -- %s" % ", ".join(red)); return 1
        # objects vs manifest
        prefix = manifest["s3_prefix"].split(PRI + "/", 1)[1] if PRI + "/" in manifest["s3_prefix"] else BASE
        listed, total = {}, 0
        token = None
        while True:
            kw = {"Bucket": PRI, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                listed[o["Key"]] = o["Size"]; total += o["Size"]
            token = resp.get("NextContinuationToken")
            if not resp.get("IsTruncated"):
                break
        rev = manifest["revision"]
        missing = [f["path"] for f in manifest["files"] if not any(k.endswith(rev + "/" + f["path"]) for k in listed)]
        (R.ok if not missing else R.fail)("bucket objects under %s: %d (%.2f GB); missing vs manifest: %d" % (prefix, len(listed), total / 1e9, len(missing)))
        if missing:
            red.append("objects"); R.fail("RED -- %s" % ", ".join(red)); return 1
        if not manifest.get("s3_prefix", "").endswith(rev + "/"):
            manifest_prefix = "s3://%s/%s%s/" % (PRI, BASE, rev)
            manifest["s3_prefix"] = manifest_prefix
            s3.put_object(Bucket=PRI, Key=BASE + "manifest.json", Body=json.dumps(manifest, indent=2, sort_keys=True).encode(), ContentType="application/json")
            R.ok("manifest s3_prefix pinned to the revision folder: %s" % manifest_prefix)
        # spec builds against the pin
        control_old = _get_json(s3, CONTROL_KEY) or {}
        control = {**control_old, "schema_version": "gearb-control.v1", "enabled": True, "model_source": "own", "model_id": MODEL_ID,
                   "model_version": rev, "instance_type": control_old.get("instance_type") or "ml.g5.2xlarge",
                   "exam_instance_type": control_old.get("exam_instance_type") or "ml.g5.2xlarge",
                   "daily_budget_usd": control_old.get("daily_budget_usd") or 20.0, "season_cap_usd": control_old.get("season_cap_usd") or 600.0,
                   "max_runtime_s": control_old.get("max_runtime_s") or 3 * 3600, "exam_max_runtime_s": control_old.get("exam_max_runtime_s") or 3600,
                   "min_sft_rows": control_old.get("min_sft_rows") or 1500, "max_family_share": control_old.get("max_family_share") or 0.25,
                   "max_jobs_per_day": control_old.get("max_jobs_per_day") or 1,
                   "lora": {"lora_r": "16", "learning_rate": "0.0001", "max_seq_len": "2048", "epochs": "1", "max_steps": "400"},
                   "approved_by": control_old.get("approved_by") or "Khalid -- chat 2026-09-13: up to $600/month to train the student",
                   "approved_at": control_old.get("approved_at") or datetime.now(timezone.utc).isoformat(),
                   "season_id": control_old.get("season_id") or "season-2026-09-14", "written_by": "ops 5542", "previous_written_by": control_old.get("written_by"),
                   "git_sha": head[:12], "reason_disabled": None, "flipped_at": datetime.now(timezone.utc).isoformat(),
                   "owned": {"weights": manifest["s3_prefix"], "manifest_sha256": manifest.get("manifest_sha256"), "license": manifest["license"]}}
        try:
            spec = own.own_spec(s3, PRI, control)
            R.ok("own_spec builds: image=%s bundle=%s artifact=%s" % (spec["training_image"].split("/")[-1][:60], spec["training_script"].split("/")[-1], spec["training_artifact"]))
        except own.OwnSpecRefused as e:
            red.append("spec"); R.fail("own_spec refused: %s" % e); R.fail("RED -- %s" % ", ".join(red)); return 1
        s3.put_object(Bucket=PRI, Key=CONTROL_KEY, Body=json.dumps(control, indent=2, sort_keys=True).encode(), ContentType="application/json")
        s3.put_object(Bucket=PRI, Key="factory/control/gearb-%s.json" % datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                      Body=json.dumps(control, indent=2, sort_keys=True).encode(), ContentType="application/json")
        R.ok("gearb control flipped: enabled=True model_source=own model=%s@%s budget $%s/day $%s/season (dated copy kept)" % (
            MODEL_ID, rev[:12], control["daily_budget_usd"], control["season_cap_usd"]))
        R.ok("GREEN -- weights owned and validated; Gear B may now train on Khalid's own lane inside its refusal chain")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

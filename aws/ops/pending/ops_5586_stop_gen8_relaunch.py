"""ops 5586 -- stop the doomed gen8 job (old fixed-400-step bundle, launched by the hourly tick at 00:07 UTC before the
new bundle was pinned) and relaunch generation 1 on the planned-steps + time-budget recipe (Claude, 2026-09-17).

ops 5585 re-pinned the bundle (train-2ae73dfc...) and proved the launcher deploy, but the tick refused to launch:
"a job is still running" -- jh-gearb-gen8-20260917-000739, created at the UTC day boundary from the OLD pin. That job runs
400 fixed steps (~5 h) inside a 3 h cap: it cannot complete, exactly like gen4..gen7. This op proves that from the job's
own hyperparameters (submit directory = the old bundle, no time_budget_s), stops it with the reason written into its job
record, waits for the terminal state, and invokes the Gear B tick (launch=true) so the refusal chain launches the next
generation on the new bundle. Then ops 5584 (idempotent) extracts the adapter and launches the frozen exam.
"""
from __future__ import annotations

import io
import json
import os
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
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI, PUB = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live"
FN = "justhodl-ai"
RECEIPT_WAIT_S = 14 * 60


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _same_source(commit, fn):
    paths = ["aws/lambdas/%s/source" % fn, "aws/lambdas/%s/config.json" % fn, "aws/shared"]
    probe = subprocess.run(["git", "cat-file", "-e", commit + "^{commit}"], cwd=REPO, capture_output=True)
    if probe.returncode != 0:
        subprocess.run(["git", "fetch", "--quiet", "--depth=200", "origin", "main"], cwd=REPO, capture_output=True)
    diff = subprocess.run(["git", "diff", "--quiet", commit, "HEAD", "--"] + paths, cwd=REPO, capture_output=True)
    return diff.returncode == 0


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, connect_timeout=5, retries={"max_attempts": 0}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5586_stop_gen8_relaunch") as R:
        R.heading("ops 5586 -- stop the doomed gen8 job (old bundle) and relaunch generation 1 on the planned-steps + time-budget bundle")
        R.kv(head=head[:10])
        # 1. the live pin (new bundle) and the running job's own hyperparameters (old bundle)
        pin = _get_json(s3, PRI, "factory/training/current.json") or {}
        new_bundle = str(pin.get("bundle_uri") or "")
        sm = boto3.client("sagemaker", region_name=REGION)
        running = []
        for j in sm.list_training_jobs(StatusEquals="InProgress", NameContains="jh-gearb-gen", MaxResults=20).get("TrainingJobSummaries", []):
            running.append(j["TrainingJobName"])
        R.ok("pinned bundle: %s (epochs=%s); InProgress gearb jobs: %s" % (new_bundle.split("/")[-1], pin.get("epochs"), running))
        for job in running:
            d = sm.describe_training_job(TrainingJobName=job)
            hp = d.get("HyperParameters") or {}
            old = str(hp.get("sagemaker_submit_directory") or "") != new_bundle and "time_budget_s" not in hp
            R.ok("%s: secondary=%s started=%s bundle=%s time_budget_s=%s -> %s" % (job, d.get("SecondaryStatus"), d.get("TrainingStartTime"), str(hp.get("sagemaker_submit_directory"))[-40:], hp.get("time_budget_s"),
                                                                             "OLD recipe, cannot finish inside the cap" if old else "new recipe, leaving it alone"))
            if not old:
                continue
            sm.stop_training_job(TrainingJobName=job)
            rec_key = "factory/gearb/jobs/%s.json" % job
            rec = _get_json(s3, PRI, rec_key) or {}
            rec["stop_requested_by"] = "ops 5586"; rec["stop_reason"] = "fixed-400-step recipe (old bundle) cannot complete inside max_runtime_s; superseded by bundle %s" % new_bundle.split("/")[-1]
            rec["stop_requested_at"] = datetime.now(timezone.utc).isoformat()
            s3.put_object(Bucket=PRI, Key=rec_key, Body=json.dumps(rec, indent=2, sort_keys=True).encode(), ContentType="application/json")
            deadline = time.time() + 8 * 60
            status = None
            while time.time() < deadline:
                status = sm.describe_training_job(TrainingJobName=job).get("TrainingJobStatus")
                if status in ("Stopped", "Failed", "Completed"):
                    break
                time.sleep(20)
            (R.ok if status == "Stopped" else R.fail)("%s -> %s (billable so far: %s s)" % (job, status, sm.describe_training_job(TrainingJobName=job).get("BillableTimeInSeconds")))
            if status not in ("Stopped", "Failed", "Completed"):
                return 1
        control = _get_json(s3, PRI, "factory/control/gearb.json") or {}
        if control.get("model_source") != "own" or not control.get("enabled"):
            R.fail("gearb control not enabled on the owned lane"); return 1
        R.ok("gearb control: max_jobs_per_day=%s daily=$%s season=$%s (unchanged by this op)" % (control.get("max_jobs_per_day"), control.get("daily_budget_usd"), control.get("season_cap_usd")))
        # 4. the tick launches inside its own refusal chain
        cfg = lam.get_function_configuration(FunctionName=FN)
        token = cfg.get("Environment", {}).get("Variables", {}).get("JH_SERVICE_TOKEN")
        if not token:
            R.fail("JH_SERVICE_TOKEN not configured on %s" % FN); return 1
        event = {"version": "2.0", "rawPath": "/gearb/tick", "rawQueryString": "", "requestContext": {"http": {"method": "POST", "path": "/gearb/tick"}},
                 "headers": {"x-jh-service-token": token, "x-jh-factory-role": "owner", "x-jh-factory-uid": "ops-5586", "content-type": "application/json"},
                 "body": json.dumps({"launch": True})}
        out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(event).encode())["Payload"].read() or b"{}")
        body_out = out.get("body")
        try:
            doc = json.loads(body_out) if isinstance(body_out, str) else (body_out or out)
        except ValueError:
            doc = {"raw": str(body_out)[:600]}
        R.ok("tick status=%s: %s" % (out.get("statusCode"), json.dumps({k: doc.get(k) for k in ("refusal", "built", "launched", "polled", "error", "reason")}, default=str)[:1500]))
        result = doc.get("result") if isinstance(doc, dict) and isinstance(doc.get("result"), dict) else doc
        launched = result.get("launched") if isinstance(result, dict) else None
        R.ok("tick result: refusal=%s built=%s" % (result.get("refusal") if isinstance(result, dict) else None, json.dumps((result or {}).get("built"), default=str)[:300]))
        if not launched:
            R.fail("no training job launched -- refusal chain: %s" % json.dumps(doc, default=str)[:1200]); return 1
        job = launched.get("job_name") if isinstance(launched, dict) else None
        if job:
            try:
                d = sm.describe_training_job(TrainingJobName=job)
                hp = d.get("HyperParameters") or {}
                R.ok("job %s: status=%s hyperparameters epochs=%s max_steps(ceiling)=%s time_budget_s=%s bundle=%s" % (
                    job, d.get("TrainingJobStatus"), hp.get("epochs"), hp.get("max_steps"), hp.get("time_budget_s") or "(default 9600 inside the recipe)", str(hp.get("sagemaker_submit_directory"))[-40:]))
            except Exception as e:  # noqa: BLE001
                R.warn("describe %s: %s" % (job, str(e)[:120]))
        R.kv(train_job=job, bundle=new_bundle.split("/")[-1])
        R.ok("GEN-1 TRAINING RELAUNCHED: %s" % json.dumps(launched, default=str)[:600])
        R.ok("GREEN -- training inside the refusal chain with a plan that fits the cap; next: ops 5584 extracts the adapter and launches the frozen exam")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

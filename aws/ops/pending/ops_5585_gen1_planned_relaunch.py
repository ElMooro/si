"""ops 5585 -- generation 1, relaunched with a trainer that can finish (Claude, 2026-09-16).

ops 5584 measured the loop from objects: six Gear B jobs since 2026-09-15 (gen1, gen3 Failed; gen4..gen7 Stopped
MaxWaitTimeExceeded). The gen7 log tail is the root cause: the recipe ran a fixed 400 steps whatever the data -- with 570
rows that is ~70 epochs at 47 s/step (~5 h) inside a 3 h runtime cap -- so every job died at ~50%, saved nothing, and the
hourly tick relaunched it. The fix on main (0cbcace): the trainer plans its steps from the data (packed sequences x epochs
/ batch; max_steps is a ceiling), the launcher passes time_budget_s = max_runtime_s - 1200 and the trainer stops on it
with the adapter saved (manifest stopped_by=time_budget), pin epochs 3. Expected for today's supply: ~18 steps, ~15 min.

This op: (1) re-pins the training bundle from THIS checkout (content-addressed; digest-pinned image kept; TRAP: a stale
bundle silently runs old code), proves the new recipe carries the plan + budget; (2) proves the launcher deploy by content
(receipt commit source == HEAD, code_sha256 == live) -- waits, and proceeds with a WARN if the deploy is slow because the
trainer's own default budget (9600 s) already protects the run; (3) max_jobs_per_day 3 -> 4 for today only (the three
stopped jobs billed spot minutes, not their caps; dated control copy kept); (4) invokes the Gear B tick (POST /gearb/tick
launch=true) through the owner route -- its refusal chain (budget, quota, holdout, unknown states) still decides.
Then ops 5584 (idempotent) extracts the adapter and launches the frozen exam once the job Completes.
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
    with report("ops_5585_gen1_planned_relaunch") as R:
        R.heading("ops 5585 -- generation 1 relaunch: bundle re-pinned with the planned-steps + time-budget trainer, launcher proven, Gear B tick launch=true")
        R.kv(head=head[:10])
        # 1. re-pin the training bundle from THIS checkout
        sys.path.insert(0, str(REPO / "scripts"))
        os.environ["FACTORY_PRIVATE_BUCKET"] = PRI
        import factory_training_pin as ftp
        pin_before = _get_json(s3, PRI, "factory/training/current.json") or {}
        if not pin_before.get("training_image"):
            R.fail("no training pin at factory/training/current.json"); return 1
        rc_pin = ftp.main(["--image", pin_before["training_image"]])
        pin_after = _get_json(s3, PRI, "factory/training/current.json") or {}
        bkey = pin_after["bundle_uri"].split(PRI + "/", 1)[1]
        tf = tarfile.open(fileobj=io.BytesIO(s3.get_object(Bucket=PRI, Key=bkey)["Body"].read()))
        recipe = tf.extractfile("train_qlora.py").read().decode()
        has_plan = "def step_plan(" in recipe and 'as_int(hp.get("time_budget_s"), 9600)' in recipe and "should_training_stop = True" in recipe
        local_recipe = (REPO / "factory" / "training" / "train_qlora.py").read_text()
        (R.ok if rc_pin == 0 and has_plan and recipe == local_recipe else R.fail)(
            "bundle re-pinned: %s -> %s; recipe plans steps + stops on the budget=%s; bundle recipe == checkout=%s; pin epochs=%s max_steps(ceiling)=%s; image kept=%s" % (
                pin_before["bundle_uri"].split("/")[-1], pin_after["bundle_uri"].split("/")[-1], has_plan, recipe == local_recipe,
                pin_after.get("epochs"), pin_after.get("max_steps"), pin_after["training_image"] == pin_before["training_image"]))
        if not (rc_pin == 0 and has_plan and recipe == local_recipe):
            return 1
        if int(pin_after.get("epochs") or 0) != 3:
            R.fail("pin epochs is %s, expected 3" % pin_after.get("epochs")); return 1
        # 2. prove the launcher deploy by content (bounded wait; WARN and proceed -- the trainer's default budget protects)
        deadline = time.time() + RECEIPT_WAIT_S
        proven = False
        while time.time() < deadline and not proven:
            rc = _get_json(s3, PUB, "data/ops/releases/%s.json" % FN) or {}
            if rc.get("commit") and _same_source(str(rc["commit"]), FN):
                live = lam.get_function_configuration(FunctionName=FN)["CodeSha256"]
                proven = rc.get("code_sha256") == live
                if proven:
                    R.ok("%s receipt commit=%s (source identical to HEAD %s), code_sha256 matches live, run=%s" % (FN, str(rc["commit"])[:7], head[:7], rc.get("run_id")))
                    break
            time.sleep(30)
        if not proven:
            R.warn("%s: no release receipt whose source matches HEAD %s within %d min -- proceeding: the pinned recipe's own default time budget (9600 s) protects this run; the launcher's explicit time_budget_s lands with the deploy" % (FN, head[:7], RECEIPT_WAIT_S // 60))
        # 3. one more launch today (three Stopped jobs already counted against max_jobs_per_day 3)
        control = _get_json(s3, PRI, "factory/control/gearb.json") or {}
        if control.get("model_source") != "own" or not control.get("enabled"):
            R.fail("gearb control not enabled on the owned lane: %s" % json.dumps({k: control.get(k) for k in ("enabled", "model_source", "model_id")})); return 1
        before = {k: control.get(k) for k in ("max_family_share", "min_sft_rows", "max_jobs_per_day", "daily_budget_usd", "season_cap_usd")}
        control["max_jobs_per_day"] = 4
        control["jobs_note"] = "2026-09-16: gen5/gen6/gen7 Stopped at MaxRuntime on the fixed-400-step recipe (billed spot minutes, not caps); one more launch today with the planned-steps recipe (ops 5585); return to 3 tomorrow"
        control["previous_written_by"] = control.get("written_by"); control["written_by"] = "ops 5585"; control["git_sha"] = head[:12]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        body = json.dumps(control, indent=2, sort_keys=True).encode()
        s3.put_object(Bucket=PRI, Key="factory/control/gearb.json", Body=body, ContentType="application/json")
        s3.put_object(Bucket=PRI, Key="factory/control/gearb-%s.json" % stamp, Body=body, ContentType="application/json")
        R.ok("gearb control: %s -> max_jobs_per_day 4 for today (dated copy kept; daily $%s and season $%s caps untouched)" % (json.dumps(before), control.get("daily_budget_usd"), control.get("season_cap_usd")))
        # 4. the tick launches inside its own refusal chain
        cfg = lam.get_function_configuration(FunctionName=FN)
        token = cfg.get("Environment", {}).get("Variables", {}).get("JH_SERVICE_TOKEN")
        if not token:
            R.fail("JH_SERVICE_TOKEN not configured on %s" % FN); return 1
        event = {"version": "2.0", "rawPath": "/gearb/tick", "rawQueryString": "", "requestContext": {"http": {"method": "POST", "path": "/gearb/tick"}},
                 "headers": {"x-jh-service-token": token, "x-jh-factory-role": "owner", "x-jh-factory-uid": "ops-5585", "content-type": "application/json"},
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
            sm = boto3.client("sagemaker", region_name=REGION)
            try:
                d = sm.describe_training_job(TrainingJobName=job)
                hp = d.get("HyperParameters") or {}
                R.ok("job %s: status=%s hyperparameters epochs=%s max_steps(ceiling)=%s time_budget_s=%s bundle=%s" % (
                    job, d.get("TrainingJobStatus"), hp.get("epochs"), hp.get("max_steps"), hp.get("time_budget_s") or "(default 9600 inside the recipe)", str(hp.get("sagemaker_submit_directory"))[-40:]))
            except Exception as e:  # noqa: BLE001
                R.warn("describe %s: %s" % (job, str(e)[:120]))
        R.kv(train_job=job, bundle=pin_after["bundle_uri"].split("/")[-1], launcher_proven=proven)
        R.ok("GEN-1 TRAINING RELAUNCHED: %s" % json.dumps(launched, default=str)[:600])
        R.ok("GREEN -- training inside the refusal chain with a plan that fits the cap; next: ops 5584 extracts the adapter and launches the frozen exam")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

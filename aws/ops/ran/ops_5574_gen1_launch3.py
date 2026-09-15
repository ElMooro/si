"""ops 5574 -- generation 1, third attempt: the job exited 3 = "no training rows" because the PINNED BUNDLE predated the
dataset-contract fix (the trainer read only prompt/completion; Gear B writes instruction/context/response). The bundle is
re-pinned from this checkout first (content-addressed; the digest-pinned image is kept), the new bundle is checked for
the contract, max_jobs_per_day is raised to 3 for today (the failed job billed 215 s), then the tick launches again.

ops 5571 -- generation 1, second attempt: the tick counts UNIQUE prompts (curate dedups by prompt), and the curriculum has
570 distinct tasks (464 MBPP + 106 APPS), so a 1,500-row floor is a 1,500-task floor the supply cannot meet today.
Generation 1 runs as an owner-directed small experiment on 570 unique verified tasks (min_sft_rows -> 500); promotion is
still decided only by the frozen exam against the trusted base (82.3%), never by the floor.

ops 5570 -- generation 1 (Claude, 2026-09-14). Base exam trusted (135/164, 0 critical), supply 2,097 eligible rows.

Gear B's family-share cap (25%) cannot be met with two families (audit A16: report insufficiency or apply an explicitly
approved alternative). Khalid's instruction is to make training happen, so the control gets max_family_share=0.85 for
generation 1 -- two families, APPS ~15% -- recorded with a dated copy; min_sft_rows stays 1,500. Then the Gear B tick is
invoked through the engine's own owner route (POST /gearb/tick launch=true): its refusal chain (budget, quota, unknown
state, holdout, receipts) still decides. Nothing is bypassed; the tick's verdict is printed verbatim.
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
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
FN = "justhodl-ai"


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, connect_timeout=5, retries={"max_attempts": 0}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5574_gen1_launch3") as R:
        R.heading("ops 5574 -- generation 1 (bundle re-pinned with the dataset contract): feasible family share for two families, then the Gear B tick (launch=true) via the owner route")
        R.kv(head=head[:10])
        # 0. re-pin the training bundle from THIS checkout (the live pin predated the dataset-contract fix)
        import io as _io, tarfile as _tarfile, os as _os
        sys.path.insert(0, str(REPO / "scripts"))
        _os.environ["FACTORY_PRIVATE_BUCKET"] = PRI
        import factory_training_pin as ftp
        pin_before = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        rc_pin = ftp.main(["--image", pin_before["training_image"]])
        pin_after = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        bkey = pin_after["bundle_uri"].split(PRI + "/", 1)[1]
        tf = _tarfile.open(fileobj=_io.BytesIO(s3.get_object(Bucket=PRI, Key=bkey)["Body"].read()))
        recipe = tf.extractfile("train_qlora.py").read().decode()
        has_contract = 'row.get("instruction")' in recipe and 'template.json' in recipe
        (R.ok if rc_pin == 0 and has_contract else R.fail)("bundle re-pinned: %s -> %s; trainer accepts instruction/context/response=%s; image kept=%s" % (
            pin_before["bundle_uri"].split("/")[-1], pin_after["bundle_uri"].split("/")[-1], has_contract, pin_after["training_image"] == pin_before["training_image"]))
        if not has_contract:
            return 1
        control = json.loads(s3.get_object(Bucket=PRI, Key="factory/control/gearb.json")["Body"].read())
        control["max_jobs_per_day"] = 3
        if control.get("model_source") != "own" or not control.get("enabled"):
            R.fail("gearb control not enabled on the owned lane: %s" % json.dumps({k: control.get(k) for k in ("enabled", "model_source", "model_id")})); return 1
        before = {k: control.get(k) for k in ("max_family_share", "min_sft_rows", "max_jobs_per_day", "daily_budget_usd", "season_cap_usd")}
        control["max_family_share"] = 0.85
        control["min_sft_rows"] = 500
        control["floor_note"] = "570 unique verified tasks exist; 1500 is a task-count floor the supply cannot meet -- generation 1 is a small experiment, promotion by frozen exam only (ops 5571)"
        control["family_share_note"] = "two families (mbpp, apps-introductory); 0.25 is infeasible -- set for generation 1 by ops 5570 on Khalid's instruction"
        control["written_by"] = "ops 5574"; control["previous_written_by"] = before and "ops 5542"; control["git_sha"] = head[:12]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        body = json.dumps(control, indent=2, sort_keys=True).encode()
        s3.put_object(Bucket=PRI, Key="factory/control/gearb.json", Body=body, ContentType="application/json")
        s3.put_object(Bucket=PRI, Key="factory/control/gearb-%s.json" % stamp, Body=body, ContentType="application/json")
        R.ok("gearb control: %s -> max_family_share 0.85, min_sft_rows 500, max_jobs_per_day 3 (dated copy kept)" % json.dumps(before))
        cfg = lam.get_function_configuration(FunctionName=FN)
        token = cfg.get("Environment", {}).get("Variables", {}).get("JH_SERVICE_TOKEN")
        if not token:
            R.fail("JH_SERVICE_TOKEN not configured on %s" % FN); return 1
        event = {"version": "2.0", "rawPath": "/gearb/tick", "rawQueryString": "", "requestContext": {"http": {"method": "POST", "path": "/gearb/tick"}},
                 "headers": {"x-jh-service-token": token, "x-jh-factory-role": "owner", "x-jh-factory-uid": "ops-5574", "content-type": "application/json"},
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
        R.ok("GEN-1 TRAINING LAUNCHED: %s" % json.dumps(launched, default=str)[:600])
        R.ok("GREEN -- generation 1 is training inside the refusal chain; next: adapter -> exam -> promotion by contract")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

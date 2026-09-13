"""ops 5524 -- re-arm of ops 5522 (receipt proof by content; see step 1). Original text follows.
ops 5522 -- arm Gear B (Claude Ship 2, 2026-09-13).

Khalid's decision in chat (2026-09-13): a training budget of $600/month for the student's
weights. This op is the owner's hand on the runner -- the engines cannot do any of this:

  1. wait for the justhodl-ai release receipt of this push (gear_b.py + the hourly hook);
  2. raise cost-guard policy.daily_budget_usd to 20.00 (= $600 / 30) -- the same field the
     AI page edits; every SageMaker create is still priced live against it;
  3. resolve the open-weight coder card: a JumpStart hub id containing "qwen" + "coder" whose
     card publishes a training recipe (TrainingSupported). If none resolves, the control is
     written DISABLED with the reason and the op ends RED -- nothing can spend;
  4. write factory/control/gearb.json create-if-absent with the approval line, daily 20 /
     season cap 600, ml.g5.2xlarge spot, 3h MaxRuntime, floor 1,500 verified rows;
  5. dispatch factory-code-exam.yml with freeze_holdout=true: the runner freezes the HumanEval
     exam + factory/holdout/manifest.json (create-if-absent) and starts verifying MBPP/APPS rows
     inside a network-less container;
  6. prove the refusal chain live: mode=gearb before the freeze must refuse on the holdout;
     after the manifest exists it must refuse with "waiting_for_traces: N of 1500". No job is
     launched by this op; the hourly inventory tick launches the first capped SFT only when the
     floor is met.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB, PRI = "justhodl-dashboard-live", "justhodl-ai-857687956942"
FUNC = "justhodl-ai"
WORKFLOW = "factory-code-exam.yml"
CONTROL_KEY = "factory/control/gearb.json"
HOLDOUT_KEY = "factory/holdout/manifest.json"
UA = {"User-Agent": "JustHodl-ops-5522 (+https://justhodl.ai)"}
RECEIPT_WAIT_S = 45 * 60
HOLDOUT_WAIT_S = 20 * 60
DAILY_USD, SEASON_USD = 20.0, 600.0
APPROVAL = "Khalid -- chat 2026-09-13: up to $600/month to train the student; results = coding, tasks, analysis, models"


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _head():
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()


def gh(path, payload=None):
    repo = os.environ.get("GITHUB_REPOSITORY", "ElMooro/si")
    token = os.environ.get("GH_API_TOKEN", "")
    if not token:
        raise RuntimeError("GH_API_TOKEN missing on the runner")
    req = urllib.request.Request("https://api.github.com/repos/" + repo + path,
                                 data=None if payload is None else json.dumps(payload).encode(),
                                 headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                          "Content-Type": "application/json", "X-GitHub-Api-Version": "2026-03-10", **UA},
                                 method="GET" if payload is None else "POST")
    with urllib.request.urlopen(req, timeout=45) as r:
        body = r.read()
        return r.status, (json.loads(body) if body else {})


def invoke(lam, payload):
    raw = lam.invoke(FunctionName=FUNC, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    return json.loads(raw["Payload"].read() or b"{}")


def resolve_coder_card(s3, lam, R):
    """Prefer the catalog the engine already keeps; describe each candidate through the engine (same code path as training)."""
    catalog = _get_json(s3, PRI, "ai/catalog.json") or {}
    ids = []
    for row in (catalog.get("models") or catalog.get("rows") or []):
        mid = str(row.get("model_id") or row.get("id") or "")
        if "qwen" in mid.lower() and "coder" in mid.lower():
            ids.append(mid)
    ids = sorted(set(ids), key=lambda m: ("7b" not in m, m))
    if not ids:
        ids = ["huggingface-llm-qwen2-5-coder-7b-instruct", "huggingface-llm-qwen2-5-coder-7b"]
        R.warn("catalog has no qwen+coder card; probing default ids %s" % ids)
    for mid in ids[:6]:
        out = invoke(lam, {"mode": "model", "body": {"model_id": mid}})
        spec = (out.get("result") or {}) if isinstance(out, dict) else {}
        if spec.get("training_supported") and spec.get("training_image"):
            R.ok("coder card %s: training recipe published (default training instance %s)" % (mid, spec.get("default_training_instance")))
            return mid, spec
        R.log("card %s: no training recipe (%s)" % (mid, str(out.get("error") or spec.get("training_supported"))[:120]))
    return None, None


def main() -> int:
    head = _head()
    red, warn = [], []
    cfg = Config(read_timeout=300, connect_timeout=5, retries={"max_attempts": 0})
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    with report("ops_5524_gear_b_rearm") as R:
        R.heading("ops 5524 (re-arm of 5522) -- arm Gear B: budget, coder card, owner control, holdout freeze dispatch, refusal chain")
        R.kv(head=head[:10], daily_usd=DAILY_USD, season_usd=SEASON_USD)

        # 1. receipt for the deployed engine, proven by CONTENT (source sha256s == this checkout, code_sha256 == live).
        #    ops 5522 waited for receipt.commit == HEAD; the next push (6d1617d) redeployed justhodl-ai and rewrote
        #    the receipt, so that equality could never be met again and 5522 timed out RED without arming anything.
        import hashlib
        src_dir = REPO / "aws/lambdas" / FUNC / "source"
        mine = {str(p.relative_to(src_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(src_dir.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}
        deadline = time.time() + RECEIPT_WAIT_S
        seen = False
        while time.time() < deadline:
            rc = _get_json(s3, PUB, "data/ops/releases/%s.json" % FUNC) or {}
            theirs = {k: v.get("sha256") for k, v in (rc.get("source") or {}).items()}
            live = lam.get_function_configuration(FunctionName=FUNC)["CodeSha256"]
            if theirs == mine and rc.get("code_sha256") == live:
                R.ok("%s receipt commit=%s run=%s: source == checkout, code_sha256 == live" % (FUNC, str(rc.get("commit"))[:7], rc.get("run_id")))
                seen = True
                break
            time.sleep(30)
        if not seen:
            red.append("receipt-missing")
            R.fail("no content-matching release receipt for %s within %d min (receipt commit=%s)" % (
                FUNC, RECEIPT_WAIT_S // 60, str((_get_json(s3, PUB, "data/ops/releases/%s.json" % FUNC) or {}).get("commit"))[:7]))
        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1

        # 2. cost-guard daily budget (owner edit, same bounds the AI page enforces)
        import cost_guard as cg
        policy = cg.save_policy(s3, PRI, {"daily_budget_usd": DAILY_USD})
        R.ok("cost-guard policy.daily_budget_usd=%.2f (training_max_runtime_s=%s, spot=%s)" % (policy.get("daily_budget_usd"), policy.get("training_max_runtime_s"), policy.get("training_spot")))

        # 3. coder card
        model_id, spec = resolve_coder_card(s3, lam, R)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        control = {"schema_version": "gearb-control.v1", "enabled": bool(model_id), "model_id": model_id or "unresolved", "model_version": None,
                   "instance_type": "ml.g5.2xlarge", "exam_instance_type": "ml.g5.2xlarge",
                   "daily_budget_usd": DAILY_USD, "season_cap_usd": SEASON_USD, "max_runtime_s": 3 * 3600, "exam_max_runtime_s": 3600,
                   "min_sft_rows": 1500, "min_dpo_pairs": 0, "max_family_share": 0.25, "max_jobs_per_day": 1,
                   "lora": {"peft_type": "lora", "lora_r": "16", "lora_alpha": "32", "lora_dropout": "0.05", "epoch": "1", "learning_rate": "0.0001",
                            "max_input_length": "2048", "instruction_tuned": "True", "chat_dataset": "False"},
                   "approved_by": APPROVAL, "approved_at": now, "season_id": "season-2026-09-14", "written_by": "ops 5524", "git_sha": head[:12],
                   "reason_disabled": None if model_id else "no qwen+coder hub card with a training recipe resolved; nothing can spend"}
        # 4. control create-if-absent (a later change is a new op, never an edit in place)
        try:
            s3.put_object(Bucket=PRI, Key=CONTROL_KEY, Body=json.dumps(control, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
            R.ok("wrote %s (enabled=%s model=%s)" % (CONTROL_KEY, control["enabled"], control["model_id"]))
        except Exception as e:  # noqa: BLE001
            existing = _get_json(s3, PRI, CONTROL_KEY) or {}
            R.warn("%s already exists (enabled=%s model=%s) -- not overwritten: %s" % (CONTROL_KEY, existing.get("enabled"), existing.get("model_id"), str(e)[:80]))
            control = existing or control
        if not model_id:
            red.append("coder-card")

        # 5. before the freeze: Gear B must refuse on the holdout (or on the disabled control)
        out = invoke(lam, {"mode": "gearb", "body": {"launch": False}})
        res = out.get("result") or {}
        refusal = str(res.get("refusal") or "")
        R.log("gearb pre-freeze: %s" % json.dumps(res)[:400])
        if "holdout" in refusal or "enabled is false" in refusal or "absent" in refusal:
            R.ok("pre-freeze refusal is the right one: %s" % refusal[:120])
        else:
            red.append("pre-freeze"); R.fail("unexpected pre-freeze result: %s" % json.dumps(out)[:300])

        # 6. dispatch the isolated exam workflow with the holdout freeze
        try:
            status, _ = gh("/actions/workflows/%s/dispatches" % WORKFLOW,
                           {"ref": "main", "inputs": {"sources": "mbpp,apps", "max_apps": "600", "freeze_holdout": "true", "dry_run": "false"}})
            (R.ok if status in (200, 204) else R.fail)("dispatched %s freeze_holdout=true http=%s" % (WORKFLOW, status))
            if status not in (200, 204):
                red.append("dispatch")
        except Exception as e:  # noqa: BLE001
            red.append("dispatch"); R.fail("dispatch failed: %s" % str(e)[:200])

        # 7. wait for the manifest, then the post-freeze refusal must be the floor
        deadline = time.time() + HOLDOUT_WAIT_S
        manifest = None
        while time.time() < deadline and "dispatch" not in red:
            manifest = _get_json(s3, PRI, HOLDOUT_KEY)
            if manifest and manifest.get("frozen_at"):
                break
            time.sleep(30)
        if manifest and manifest.get("frozen_at"):
            R.ok("holdout frozen at %s: blocks=%s code_ids=%d drills=%s" % (manifest.get("frozen_at"), [b["id"] for b in manifest.get("holdout_blocks") or []],
                                                                         len((manifest.get("code") or {}).get("task_ids") or []), manifest.get("counts")))
            out = invoke(lam, {"mode": "gearb", "body": {"launch": False}})
            res = out.get("result") or {}
            refusal = str(res.get("refusal") or "")
            R.log("gearb post-freeze: %s" % json.dumps(res)[:400])
            if refusal.startswith("waiting_for_traces") or "enabled is false" in refusal:
                R.ok("post-freeze refusal is the floor: %s" % refusal[:120])
            elif res.get("launched"):
                red.append("launched-early"); R.fail("a job launched during the arm op -- not expected")
            else:
                warn.append("post-freeze:" + refusal[:60]); R.warn("post-freeze result: %s" % refusal[:160])
        else:
            warn.append("holdout-not-yet-frozen"); R.warn("holdout manifest not frozen within %d min -- check the %s run; the hourly tick keeps refusing until it exists" % (HOLDOUT_WAIT_S // 60, WORKFLOW))

        # 8. public read model carries the Gear B block (counts + money only)
        rm = _get_json(s3, PUB, "data/ai.json") or {}
        gb = rm.get("gear_b")
        if isinstance(gb, dict):
            blob = json.dumps(gb)
            leak = ("arn:" in blob) or ("jh-gearb-gen" in blob)
            (R.fail if leak else R.ok)("data/ai.json gear_b: status=%s budget=%s leak=%s" % (gb.get("status"), gb.get("budget"), leak))
            if leak:
                red.append("public-leak")
        else:
            warn.append("read-model-not-refreshed"); R.warn("data/ai.json has no gear_b block yet (next hourly inventory writes it)")

        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- Gear B armed: budget $%.0f/day, control written, holdout freeze dispatched, nothing launched; first SFT fires from the hourly tick once %d verified rows exist%s" % (
            DAILY_USD, control.get("min_sft_rows", 1500), " (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)

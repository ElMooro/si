"""ops 1109 — Arch #4: deploy justhodl-market-interpreter + wire into scheduler.

Steps:
  1. Create-or-update the Lambda from aws/lambdas/justhodl-market-interpreter/.
  2. Attach jhcore layer v1.
  3. Copy ANTHROPIC_API_KEY env var from justhodl-ai-chat → new Lambda.
  4. Add to schedule-manifest.json under tick=hourly (free via the new scheduler).
  5. Invoke for one full pass; verify each of 7 contexts wrote to data/interpretations/<id>.json.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-market-interpreter"
KEY_SOURCE_LAMBDA = "justhodl-ai-chat"  # has ANTHROPIC_API_KEY env var
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
LAYER_ARN = "arn:aws:lambda:us-east-1:857687956942:layer:justhodl-core:1"

events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(name, t=120):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed":
                return False
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        time.sleep(3)
    return False


def get_source_env():
    """Read ANTHROPIC_API_KEY from the existing Lambda."""
    try:
        c = lam.get_function_configuration(FunctionName=KEY_SOURCE_LAMBDA)
        return (c.get("Environment") or {}).get("Variables", {}) or {}
    except ClientError as e:
        print(f"[1109] could not read env from {KEY_SOURCE_LAMBDA}: {e}")
        return {}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

    # ────── Step 1+2+3: Deploy + Layer + Env ──────
    cfg = json.load(open(os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")))
    src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")

    # Pull API key from existing Lambda
    src_env = get_source_env()
    api_key = src_env.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        rpt["steps"]["env_pull"] = f"FAILED — no ANTHROPIC_API_KEY on {KEY_SOURCE_LAMBDA}"
        _save(rpt); return
    rpt["steps"]["env_pull"] = f"OK — pulled key from {KEY_SOURCE_LAMBDA} ({len(api_key)} chars)"

    env_vars = {"ANTHROPIC_API_KEY": api_key, "S3_BUCKET": BUCKET}

    # Create-or-update Lambda
    try:
        lam.get_function_configuration(FunctionName=FN); exists = True
    except ClientError:
        exists = False

    if not exists:
        try:
            lam.create_function(
                FunctionName=FN,
                Runtime=cfg["runtime"],
                Role=cfg["role"],
                Handler=cfg["handler"],
                Code={"ZipFile": zip_src(src_dir)},
                Description=cfg["description"][:255],
                Timeout=cfg["timeout"],
                MemorySize=cfg["memory"],
                Architectures=cfg["architectures"],
                Layers=cfg.get("layers") or [LAYER_ARN],
                Environment={"Variables": env_vars},
            )
            rpt["steps"]["deploy"] = "CREATED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                rpt["steps"]["deploy"] = "RACED"; exists = True
            else:
                rpt["steps"]["deploy_err"] = str(e)[:400]; _save(rpt); return
        wait_active(FN)

    if exists:
        wait_active(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active(FN)
        lam.update_function_configuration(
            FunctionName=FN,
            Timeout=cfg["timeout"],
            MemorySize=cfg["memory"],
            Layers=cfg.get("layers") or [LAYER_ARN],
            Environment={"Variables": env_vars},
            Description=cfg["description"][:255],
        )
        wait_active(FN)
        rpt["steps"]["deploy"] = "SYNCED"

    # ────── Step 4: Add to schedule-manifest hourly tick ──────
    try:
        m_obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        manifest = json.loads(m_obj["Body"].read())
    except Exception as e:
        rpt["steps"]["manifest_err"] = f"could not read manifest: {e}"
        manifest = None

    if manifest:
        hourly = manifest.setdefault("ticks", {}).setdefault("hourly", [])
        if FN not in hourly:
            hourly.append(FN)
            hourly[:] = sorted(set(hourly))
            manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
            s3.put_object(
                Bucket=BUCKET, Key=MANIFEST_KEY,
                Body=json.dumps(manifest, indent=2).encode("utf-8"),
                ContentType="application/json"
            )
            rpt["steps"]["manifest"] = f"ADDED to hourly (now {len(hourly)} jobs)"
        else:
            rpt["steps"]["manifest"] = f"ALREADY in hourly ({len(hourly)} jobs)"

    # ────── Step 5: Invoke once + verify outputs ──────
    try:
        inv = lam.invoke(
            FunctionName=FN,
            InvocationType="RequestResponse",
            Payload=b"{}",
            LogType="Tail",
        )
        body_raw = inv["Payload"].read()
        body = json.loads(body_raw or b"{}")
        if isinstance(body, dict) and "body" in body:
            try:
                body = json.loads(body["body"])
            except Exception:
                pass
        rpt["steps"]["invoke_status"] = inv["StatusCode"]
        rpt["steps"]["invoke_body"] = body
        rpt["steps"]["invoke_fn_err"] = inv.get("FunctionError")

        # Decode log tail for diagnostics
        import base64 as _b64
        log_tail = _b64.b64decode(inv.get("LogResult", "")).decode("utf-8", "replace")[-2000:]
        rpt["steps"]["log_tail"] = log_tail
    except Exception as e:
        rpt["steps"]["invoke_err"] = str(e)[:400]

    # ────── Verify S3 outputs ──────
    time.sleep(3)
    contexts = ["yield-curve", "vix-curve", "credit-spreads", "dollar",
                "eurodollar", "systemic-stress", "real-rates"]
    out_status = {}
    for cid in contexts:
        try:
            h = s3.head_object(Bucket=BUCKET, Key=f"data/interpretations/{cid}.json")
            out_status[cid] = {
                "exists": True,
                "size_kb": round(h["ContentLength"] / 1024, 1),
                "last_modified": h["LastModified"].isoformat(),
            }
        except ClientError:
            out_status[cid] = {"exists": False}
    rpt["outputs"] = out_status
    rpt["outputs_ok"] = sum(1 for v in out_status.values() if v.get("exists"))
    rpt["outputs_expected"] = len(contexts)

    _save(rpt)


def _save(rpt):
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1109.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rpt, open(out, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "steps"}, indent=2, default=str)[:1500])


if __name__ == "__main__":
    main()

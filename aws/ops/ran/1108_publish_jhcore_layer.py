"""ops 1108 — Arch #2: package + publish justhodl-core Lambda Layer.

The layer contains jhcore.{fred, s3io, notify, claude, kb} — eliminating
duplication across the 416-Lambda fleet (487× telegram, 390× s3, 73× fred, 31× claude).

After publish, Lambdas can be attached to the layer ARN. Migration is per-Lambda
(opt-in via config update); zero pressure to migrate fast — layer + duplicated
code coexist safely.
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
LAYER_NAME = "justhodl-core"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
SOURCE_DIR = os.path.join(REPO_ROOT, "aws/layers/justhodl-core")

lam = boto3.client("lambda", region_name=REGION)


def zip_layer(d):
    """Zip the layer directory so that the layout is python/jhcore/*.py."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, f)
                arc = os.path.relpath(fp, d)
                z.write(fp, arc)
    return buf.getvalue()


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}

    if not os.path.isdir(os.path.join(SOURCE_DIR, "python", "jhcore")):
        rpt["err"] = f"Layer source missing at {SOURCE_DIR}/python/jhcore"
        _save(rpt); return

    zip_bytes = zip_layer(SOURCE_DIR)
    rpt["zip_size_kb"] = round(len(zip_bytes) / 1024, 1)
    rpt["modules"] = sorted(os.listdir(os.path.join(SOURCE_DIR, "python", "jhcore")))

    # Publish layer version
    try:
        r = lam.publish_layer_version(
            LayerName=LAYER_NAME,
            Description=f"jhcore — JustHodl shared helpers (fred/s3io/notify/claude/kb). Published {datetime.now(timezone.utc).isoformat()}",
            Content={"ZipFile": zip_bytes},
            CompatibleRuntimes=["python3.11", "python3.12"],
            CompatibleArchitectures=["x86_64", "arm64"],
        )
        rpt["layer_arn"] = r["LayerVersionArn"]
        rpt["version"] = r["Version"]
        rpt["created"] = r.get("CreatedDate")
    except ClientError as e:
        rpt["publish_err"] = str(e)[:400]
        _save(rpt); return

    # Smoke test: create a tiny test Lambda using the layer, invoke it, then delete
    test_fn = f"jhcore-smoke-test-{int(time.time())}"
    test_code = '''
def lambda_handler(event, context):
    from jhcore import fred, s3io, notify, claude, kb
    return {"jhcore_version": __import__("jhcore").__version__,
            "modules_ok": all([hasattr(fred,"latest"), hasattr(s3io,"get_json"),
                               hasattr(notify,"telegram"), hasattr(claude,"complete"),
                               hasattr(kb,"lookup")])}
'''
    # Zip test code
    tbuf = io.BytesIO()
    with zipfile.ZipFile(tbuf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", test_code)
    try:
        lam.create_function(
            FunctionName=test_fn,
            Runtime="python3.12",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": tbuf.getvalue()},
            Timeout=10,
            MemorySize=256,
            Layers=[rpt["layer_arn"]],
            Description="jhcore smoke test (auto-deleted)",
        )
        # Wait for active
        for _ in range(20):
            c = lam.get_function_configuration(FunctionName=test_fn)
            if c.get("State") == "Active": break
            time.sleep(1)
        inv = lam.invoke(FunctionName=test_fn, InvocationType="RequestResponse")
        body = json.loads(inv["Payload"].read())
        rpt["smoke_test"] = body
        rpt["smoke_status"] = inv["StatusCode"]
        rpt["smoke_err"] = inv.get("FunctionError")
    except Exception as e:
        rpt["smoke_err"] = str(e)[:400]
    finally:
        try: lam.delete_function(FunctionName=test_fn)
        except Exception: pass

    # Count Lambdas that could be migrated (have any of the duplicated patterns)
    # (read-only inventory — no changes)
    paginator = lam.get_paginator("list_functions")
    total = 0
    has_layer = 0
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            if not f["FunctionName"].startswith("justhodl") and not f["FunctionName"].startswith("jhk"):
                continue
            total += 1
            layers = f.get("Layers") or []
            if any("justhodl-core" in (l.get("Arn") or "") for l in layers):
                has_layer += 1
    rpt["fleet_total"] = total
    rpt["already_using_layer"] = has_layer
    rpt["candidates_for_migration"] = total - has_layer

    _save(rpt)


def _save(rpt):
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1108.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rpt, open(out, "w"), indent=2, default=str)
    print(json.dumps(rpt, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()

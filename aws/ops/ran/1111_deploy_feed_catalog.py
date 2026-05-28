"""ops 1111 — Arch #5: deploy justhodl-feed-catalog + register in scheduler daily-eve."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-feed-catalog"
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
LAYER_ARN = "arn:aws:lambda:us-east-1:857687956942:layer:justhodl-core:1"

_cfg = Config(connect_timeout=10, read_timeout=240, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=120):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        cfg = json.load(open(os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")))
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")

        try:
            lam.get_function_configuration(FunctionName=FN); exists = True
        except ClientError:
            exists = False

        if not exists:
            lam.create_function(
                FunctionName=FN, Runtime=cfg["runtime"], Role=cfg["role"],
                Handler=cfg["handler"], Code={"ZipFile": zip_src(src_dir)},
                Description=cfg["description"][:255], Timeout=cfg["timeout"],
                MemorySize=cfg["memory"], Architectures=cfg["architectures"],
                Layers=cfg.get("layers") or [LAYER_ARN],
            )
            rpt["deploy"] = "CREATED"
        else:
            wait_active()
            lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
            wait_active()
            lam.update_function_configuration(
                FunctionName=FN, Timeout=cfg["timeout"], MemorySize=cfg["memory"],
                Layers=cfg.get("layers") or [LAYER_ARN],
                Description=cfg["description"][:255],
            )
            rpt["deploy"] = "SYNCED"
        wait_active()

        # Register in scheduler manifest at daily-eve
        try:
            m = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
            eve = m.setdefault("ticks", {}).setdefault("daily-eve", [])
            if FN not in eve:
                eve.append(FN); eve[:] = sorted(set(eve))
                m["generated_at"] = datetime.now(timezone.utc).isoformat()
                s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                              Body=json.dumps(m, indent=2).encode("utf-8"),
                              ContentType="application/json")
                rpt["manifest"] = f"ADDED to daily-eve (now {len(eve)} jobs)"
            else:
                rpt["manifest"] = f"ALREADY in daily-eve ({len(eve)} jobs)"
        except Exception as e:
            rpt["manifest_err"] = str(e)[:200]

        # Invoke once
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}", LogType="Tail")
        body = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body, dict) and "body" in body:
            try: body = json.loads(body["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_body"] = body
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # Verify catalog written
        time.sleep(2)
        try:
            h = s3.head_object(Bucket=BUCKET, Key="data/feed-catalog.json")
            rpt["catalog"] = {
                "exists": True,
                "size_kb": round(h["ContentLength"]/1024, 1),
                "last_modified": h["LastModified"].isoformat(),
            }
            # Sample summary
            cat = json.loads(s3.get_object(Bucket=BUCKET, Key="data/feed-catalog.json")["Body"].read())
            rpt["catalog_summary"] = {
                "total_feeds": cat.get("total_feeds"),
                **(cat.get("summary") or {}),
            }
        except ClientError:
            rpt["catalog"] = {"exists": False}
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1111.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")}, indent=2, default=str)[:1800])


if __name__ == "__main__":
    main()

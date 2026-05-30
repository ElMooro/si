"""ops 1123 — Redeploy LCE Lambda (with retry-on-429) + reinvoke + verify the 7
previously-unavailable series are now populated."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-liquidity-credit-engine"
BUCKET = "justhodl-dashboard-live"
WATCH = ["RESPPALGUONNWW", "WGCAL", "RESPPNTEPNWW", "WRESBAL",
          "WCURCIR", "RRPONTSYD", "WLRRAL"]

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
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
            if c.get("LastUpdateStatus") == "Failed":
                return False
        except ClientError: pass
        time.sleep(2)
    return False


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # Invoke
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        body_raw = inv["Payload"].read()
        try:
            body = json.loads(body_raw or b"{}")
            if isinstance(body, dict) and "body" in body:
                body = json.loads(body["body"])
            rpt["invoke_body"] = body
        except Exception:
            rpt["invoke_body_raw"] = (body_raw or b"").decode("utf-8", "replace")[:600]
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # Re-read LCE JSON, check our 7 series
        time.sleep(2)
        try:
            lce = json.loads(s3.get_object(Bucket=BUCKET, Key="data/liquidity-credit-engine.json")["Body"].read())
            series = lce.get("series", {}) or {}
            rpt["new_lce_generated_at"] = lce.get("generated_at")
            check = {}
            for sid in WATCH:
                s = series.get(sid, {})
                check[sid] = {
                    "available": s.get("available"),
                    "error": s.get("error"),
                    "latest_value": s.get("latest_value"),
                    "signal": s.get("signal"),
                }
            rpt["check_seven"] = check
            rpt["n_now_available"] = sum(1 for v in check.values() if v.get("available"))
        except Exception as e:
            rpt["read_back_err"] = str(e)[:300]
    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1123.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")}, indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()

"""ops 1110 — redeploy market-interpreter (with cross-indicator fallback) + verify all 7."""
import io, json, os, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-market-interpreter"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
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

    src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
    wait_active()
    lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
    wait_active()
    rpt["redeploy"] = "OK"

    # Inspect episode-reference to log what indicator IDs exist (for visibility)
    try:
        ref = json.loads(s3.get_object(Bucket=BUCKET, Key="data/episode-reference.json")["Body"].read())
        rpt["episode_ref_indicators"] = sorted(list((ref.get("indicators") or {}).keys()))
    except Exception as e:
        rpt["episode_ref_err"] = str(e)[:200]

    # Invoke
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}", LogType="Tail")
    body_raw = inv["Payload"].read()
    body = json.loads(body_raw or b"{}")
    if isinstance(body, dict) and "body" in body:
        try: body = json.loads(body["body"])
        except Exception: pass
    rpt["invoke_status"] = inv["StatusCode"]
    rpt["invoke_body"] = body
    rpt["invoke_fn_err"] = inv.get("FunctionError")
    rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1800:]

    # Verify each
    time.sleep(3)
    contexts = ["yield-curve","vix-curve","credit-spreads","dollar","eurodollar","systemic-stress","real-rates"]
    out = {}
    for cid in contexts:
        try:
            h = s3.head_object(Bucket=BUCKET, Key=f"data/interpretations/{cid}.json")
            out[cid] = {"exists": True, "size_kb": round(h["ContentLength"]/1024,1),
                        "last_modified": h["LastModified"].isoformat()}
        except ClientError:
            out[cid] = {"exists": False}
    rpt["outputs"] = out
    rpt["outputs_ok"] = sum(1 for v in out.values() if v.get("exists"))
    rpt["outputs_expected"] = len(contexts)

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1110.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p,"w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","episode_ref_indicators")}, indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()

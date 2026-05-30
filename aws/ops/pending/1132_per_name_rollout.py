"""ops 1132 — Per-name brief rollout. Adds 7 per-name contexts (baggers, eps-velocity,
insider-clusters, smart-money, activist-13d, deep-value, momentum). Redeploys router
with brief_type dispatch, uploads 30-context registry, targeted-invokes the 7 new ones.
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"

NEW_NAMES = [
    "baggers-names",
    "eps-velocity-names",
    "insider-clusters-names",
    "smart-money-names",
    "activist-13d-names",
    "deep-value-names",
    "momentum-names",
]

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


def wait_active(t=180):
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
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {"n_contexts": len(registry.get("contexts") or {})}

        print(f"[1132] invoking router for {len(NEW_NAMES)} new per-name contexts")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": NEW_NAMES}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["invoke_summary"] = {
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            "n_contexts": body_resp.get("n_contexts") if isinstance(body_resp, dict) else None,
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
        }
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2500:]

        # Verify each new per-name brief landed
        time.sleep(3)
        verify = []
        for ctx_id in NEW_NAMES:
            ctx_cfg = (registry.get("contexts") or {}).get(ctx_id, {})
            out_key = f"data/{ctx_cfg.get('output_key', ctx_id)}.json"
            row = {"context": ctx_id, "output_key": out_key}
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=out_key)
                brief = json.loads(obj["Body"].read())
                age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
                names = brief.get("names") or []
                row["status"] = "OK"
                row["title"] = brief.get("title")
                row["regime_note"] = brief.get("regime_note")
                row["n_names"] = len(names)
                row["age_sec"] = round(age, 1)
                row["fresh"] = age < 600
                # Sample top 3 ticker briefs
                row["sample"] = []
                for n in names[:3]:
                    row["sample"].append({
                        "ticker": n.get("ticker"),
                        "rank": n.get("rank"),
                        "score": n.get("primary_score"),
                        "fit": n.get("regime_fit"),
                        "one_liner": (n.get("one_liner") or "")[:130],
                        "catalyst": (n.get("catalyst") or "")[:90],
                        "analog": (n.get("historical_analog") or {}).get("ticker"),
                        "asym": n.get("asymmetric_estimate"),
                    })
            except ClientError:
                row["status"] = "NOT_WRITTEN"
            except Exception as e:
                row["status"] = "ERR"; row["err"] = str(e)[:200]
            verify.append(row)
        rpt["verify"] = verify
        rpt["n_briefs"] = sum(1 for r in verify if r.get("status") == "OK" and r.get("fresh"))

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1132.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()

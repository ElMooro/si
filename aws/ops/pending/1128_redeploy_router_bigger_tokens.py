"""ops 1128 — Redeploy ai-brief-router (max_tokens 5500->8000 + JSON repair fallback) + reinvoke + verify all 6 briefs."""
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

        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        body = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body, dict) and "body" in body:
            try: body = json.loads(body["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_body"] = body
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2000:]

        # Verify all 6 contexts
        time.sleep(3)
        registry = json.loads(s3.get_object(Bucket=BUCKET, Key=REGISTRY_KEY)["Body"].read())
        verify = []
        for ctx_id, ctx_cfg in (registry.get("contexts") or {}).items():
            out_key = f"data/{ctx_cfg.get('output_key', ctx_id)}.json"
            row = {"context": ctx_id, "output_key": out_key}
            try:
                brief = json.loads(s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read())
                row["regime"] = brief.get("regime")
                row["confidence"] = brief.get("confidence")
                row["one_liner"] = (brief.get("one_liner") or "")[:160]
                row["n_predictions"] = len(brief.get("historical_predictions") or [])
                row["n_trades"] = len(brief.get("trade_ideas") or [])
                row["n_tripwires"] = len(brief.get("tripwires") or [])
                row["generated_at"] = brief.get("generated_at")
                for p in (brief.get("historical_predictions") or []):
                    if p.get("ticker") == "BTC":
                        row["btc_pred"] = {
                            "dir": p.get("prediction_direction"),
                            "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                            "horizon_wk": p.get("prediction_horizon_weeks"),
                            "prob": p.get("probability_pct"),
                            "analog": p.get("best_analog_period"),
                        }
                        break
                row["status"] = "OK"
            except ClientError:
                row["status"] = "NOT_WRITTEN"
            except Exception as e:
                row["status"] = "ERR"; row["err"] = str(e)[:200]
            verify.append(row)
        rpt["verify"] = verify
        rpt["n_briefs"] = sum(1 for r in verify if r.get("status") == "OK")

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1128.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()

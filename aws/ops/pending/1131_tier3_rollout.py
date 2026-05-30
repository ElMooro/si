"""ops 1131 — Tier-3 rollout. Adds 10 new contexts (liquidity, macro-data, crisis,
divergence, trend-engine, regime, valuations, conviction, fundamentals, consumer-pulse).
Redeploys router with MAX_WORKERS=12, uploads 23-context registry, targeted-invokes
the 10 new contexts only.
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

NEW_CONTEXTS = [
    "liquidity-decisive-call",
    "macro-data-decisive-call",
    "crisis-decisive-call",
    "divergence-decisive-call",
    "trend-engine-decisive-call",
    "regime-decisive-call",
    "valuations-decisive-call",
    "conviction-decisive-call",
    "fundamentals-decisive-call",
    "consumer-pulse-decisive-call",
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
        # 1) Redeploy router (MAX_WORKERS=12 change)
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Upload 23-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {"n_contexts": len(registry.get("contexts") or {}),
                            "all": sorted((registry.get("contexts") or {}).keys())}

        # 3) Targeted invocation — just the 10 new
        print(f"[1131] invoking router for {len(NEW_CONTEXTS)} new contexts")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": NEW_CONTEXTS}).encode(),
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
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2000:]

        # 4) Verify each new brief
        time.sleep(3)
        verify = []
        for ctx_id in NEW_CONTEXTS:
            ctx_cfg = (registry.get("contexts") or {}).get(ctx_id, {})
            out_key = f"data/{ctx_cfg.get('output_key', ctx_id)}.json"
            row = {"context": ctx_id, "output_key": out_key}
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=out_key)
                brief = json.loads(obj["Body"].read())
                age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
                row["regime"] = brief.get("regime")
                row["confidence"] = brief.get("confidence")
                row["one_liner"] = (brief.get("one_liner") or "")[:160]
                row["n_predictions"] = len(brief.get("historical_predictions") or [])
                row["n_trades"] = len(brief.get("trade_ideas") or [])
                row["n_tripwires"] = len(brief.get("tripwires") or [])
                row["age_sec"] = round(age, 1)
                row["fresh"] = age < 600
                for p in (brief.get("historical_predictions") or []):
                    if p.get("ticker") in ("BTC", "SPX"):
                        if "btc" not in row and p.get("ticker") == "BTC":
                            row["btc"] = {
                                "dir": p.get("prediction_direction"),
                                "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                                "wk": p.get("prediction_horizon_weeks"),
                                "prob": p.get("probability_pct"),
                                "analog": p.get("best_analog_period"),
                            }
                        if "spx" not in row and p.get("ticker") == "SPX":
                            row["spx"] = {
                                "dir": p.get("prediction_direction"),
                                "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                                "wk": p.get("prediction_horizon_weeks"),
                                "prob": p.get("probability_pct"),
                            }
                row["status"] = "OK"
            except ClientError:
                row["status"] = "NOT_WRITTEN"
            except Exception as e:
                row["status"] = "ERR"; row["err"] = str(e)[:200]
            verify.append(row)
        rpt["verify"] = verify
        rpt["n_fresh"] = sum(1 for r in verify if r.get("fresh"))
        rpt["n_briefs_exist"] = sum(1 for r in verify if r.get("status") == "OK")

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1131.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()

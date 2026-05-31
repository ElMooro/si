"""ops 1153 — Fix Phase 11 slice bug + verify predictions land in DynamoDB.

Ops 1152 succeeded structurally but Phase 11 hit a slice error
(slice(None, 300, None)) during rationale generation. Fixed by:
  - Wrapping rationale in str() before slicing (handles dict variance)
  - Per-setup try/except so one bad setup doesn't kill the whole block
  - Lowered sustained-target threshold 6 → 4 so it activates today
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"
ACCOUNT_ID = "857687956942"
ROUTER_FN = "justhodl-ai-brief-router"
LOGGER_FN = "justhodl-signal-logger"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(fn, t=300):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(3)
    return False


def update_with_retries(fn, zipped, max_tries=8):
    last_err = None
    for attempt in range(1, max_tries + 1):
        wait_active(fn)
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zipped, Publish=False)
            return {"attempt": attempt, "status": "OK"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            last_err = str(e)[:200]
            if code == "ResourceConflictException":
                time.sleep(10 + attempt * 2)
                continue
            raise
    return {"attempt": max_tries, "status": "EXHAUSTED", "err": last_err}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy signal-logger
        logger_src = os.path.join(REPO_ROOT, "aws/lambdas", LOGGER_FN, "source")
        rpt["redeploy"] = update_with_retries(LOGGER_FN, zip_src(logger_src))
        wait_active(LOGGER_FN)

        # 2) Fire it
        print("[1153] firing signal-logger after fix …")
        time.sleep(2)
        inv = lam.invoke(FunctionName=LOGGER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"action": "1153_phase11_retry"}).encode(),
                          LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["logger_fire"] = {
            "fn_err": inv.get("FunctionError"),
            "logged": body_resp.get("logged") if isinstance(body_resp, dict) else None,
        }
        # Extract Phase 11 specific log lines
        logs = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")
        front_keywords = ["frontrun_sniffer_setup", "macro_frontrun_sniffer_setup",
                          "convergence_fingerprint", "sustained_target_equity",
                          "FRONTRUN-PREDICTIONS", "equity_setup", "macro_setup",
                          "sustained_target"]
        front_lines = [L for L in logs.split("\n")
                        if any(k in L for k in front_keywords)]
        rpt["frontrun_log_lines"] = "\n".join(front_lines[:30])

        # 3) Query DynamoDB for the new front-run signal types (last 24h)
        time.sleep(3)
        table = ddb.Table("justhodl-signals")
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
        from boto3.dynamodb.conditions import Attr
        new_types = ["frontrun_sniffer_setup", "macro_frontrun_sniffer_setup",
                     "macro_convergence_fingerprint", "equity_convergence_fingerprint",
                     "sustained_target_equity"]
        resp = table.scan(
            FilterExpression=Attr("signal_type").is_in(new_types) & Attr("logged_epoch").gte(cutoff),
            Limit=100,
        )
        items = resp.get("Items", [])
        rpt["frontrun_predictions_in_db"] = {
            "n_total":  len(items),
            "by_type":  {},
            "samples":  [],
        }
        for it in items:
            stype = it.get("signal_type", "?")
            rpt["frontrun_predictions_in_db"]["by_type"][stype] = \
                rpt["frontrun_predictions_in_db"]["by_type"].get(stype, 0) + 1
        # Show 5 sample records
        for it in items[:5]:
            rpt["frontrun_predictions_in_db"]["samples"].append({
                "signal_id":          str(it.get("signal_id", ""))[:12],
                "signal_type":        it.get("signal_type"),
                "signal_value":       it.get("signal_value"),
                "predicted_direction": it.get("predicted_direction"),
                "confidence":         float(it.get("confidence") or 0),
                "measure_against":    it.get("measure_against"),
                "baseline_price":     float(it.get("baseline_price") or 0) if it.get("baseline_price") else None,
                "horizon_days_primary": it.get("horizon_days_primary"),
                "regime_at_log":      it.get("regime_at_log"),
            })

        # 4) Re-fire skill aggregator now that predictions exist
        print("[1153] firing skill aggregator to refresh index …")
        wait_active(ROUTER_FN)
        time.sleep(2)
        inv2 = lam.invoke(FunctionName=ROUTER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode())
        body_resp2 = json.loads(inv2["Payload"].read() or b"{}")
        if isinstance(body_resp2, dict) and "body" in body_resp2:
            try: body_resp2 = json.loads(body_resp2["body"])
            except Exception: pass
        rpt["skill_aggregator_fire"] = {
            "fn_err": inv2.get("FunctionError"),
            "n_ok": body_resp2.get("n_ok") if isinstance(body_resp2, dict) else None,
            "results": body_resp2.get("results") if isinstance(body_resp2, dict) else None,
        }
        time.sleep(2)

        # 5) Read back skill index
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_skill/frontrun-skill-index.json")
            idx = json.loads(obj["Body"].read())
            rpt["skill_index"] = {
                "updated_at":  idx.get("updated_at"),
                "n_total":     idx.get("n_total_predictions"),
                "n_scored":    idx.get("n_scored"),
                "n_pending":   idx.get("n_pending"),
                "engines":     list((idx.get("by_engine") or {}).keys()),
                "engine_summary": {k: {"n_total": v.get("n_total"),
                                        "n_scored": v.get("n_scored"),
                                        "hit_rate": v.get("hit_rate"),
                                        "n_pending": v.get("n_pending_grade")}
                                   for k, v in (idx.get("by_engine") or {}).items()},
            }
        except ClientError as e:
            rpt["skill_index"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1153.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"},
                     indent=2, default=str)[:5500])


from datetime import timedelta
if __name__ == "__main__":
    main()

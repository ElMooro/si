"""ops 1125 — Redeploy auction-interpreter (with historical_predictions) + reinvoke + verify."""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-auction-interpreter"
BUCKET = "justhodl-dashboard-live"

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

        # Invoke
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}", LogType="Tail")
        body = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body, dict) and "body" in body:
            try: body = json.loads(body["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_body"] = body
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # Inspect brief
        time.sleep(2)
        brief = json.loads(s3.get_object(Bucket=BUCKET, Key="data/auction-decisive-call.json")["Body"].read())
        rpt["brief_summary"] = {
            "version": brief.get("version"),
            "regime": brief.get("regime"),
            "confidence": brief.get("confidence"),
            "one_liner": brief.get("one_liner"),
            "n_evidence": len(brief.get("supporting_evidence") or []),
            "n_analogs": len(brief.get("historical_analogs") or []),
            "n_cross_asset": len(brief.get("cross_asset") or []),
            "n_trades": len(brief.get("trade_ideas") or []),
            "n_tripwires": len(brief.get("tripwires") or []),
            "n_predictions": len(brief.get("historical_predictions") or []),
            "n_next_auctions": len(brief.get("next_auctions_to_watch") or []),
        }
        # Sample the predictions — this is what we're verifying
        preds = brief.get("historical_predictions") or []
        rpt["predictions"] = []
        for p in preds:
            rpt["predictions"].append({
                "asset": p.get("asset"),
                "ticker": p.get("ticker"),
                "direction": p.get("prediction_direction"),
                "range_low_pct": p.get("prediction_range_low_pct"),
                "range_high_pct": p.get("prediction_range_high_pct"),
                "horizon_weeks": p.get("prediction_horizon_weeks"),
                "confidence": p.get("confidence"),
                "probability_pct": p.get("probability_pct"),
                "best_analog": p.get("best_analog_period"),
                "analog_outcome": p.get("analog_outcome_summary"),
                "upside_trigger": p.get("upside_trigger"),
                "downside_pct": p.get("downside_scenario_pct"),
                "downside_trigger": p.get("downside_trigger"),
                "reasoning": p.get("key_reasoning"),
            })
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1125.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("log_tail", "traceback")},
                     indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()

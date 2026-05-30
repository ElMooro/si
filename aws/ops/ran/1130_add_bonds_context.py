"""ops 1130 — Add bonds-decisive-call context. Uploads the updated registry (now 13 contexts)
to S3, runs a TARGETED invocation against just the new bonds context (to avoid burning Claude
on 12 already-fresh briefs), verifies the bonds brief lands.
"""
import json, os, time, traceback, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"
NEW_CTX = "bonds-decisive-call"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"),
                       ContentType="application/json")
        registry = json.loads(body)
        ctx_ids = sorted((registry.get("contexts") or {}).keys())
        rpt["registry_uploaded"] = {"n_contexts": len(ctx_ids), "contexts": ctx_ids}
        rpt["new_context_present"] = NEW_CTX in ctx_ids

        # Targeted invocation: only the new bonds context
        print(f"[1130] invoking router with contexts=[{NEW_CTX}]")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": [NEW_CTX]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["invoke_status"] = inv["StatusCode"]
        rpt["invoke_fn_err"] = inv.get("FunctionError")
        rpt["invoke_body"] = body_resp
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # Verify the brief landed
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/{NEW_CTX}.json")
            brief = json.loads(obj["Body"].read())
            age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
            rpt["brief"] = {
                "regime": brief.get("regime"),
                "confidence": brief.get("confidence"),
                "one_liner": brief.get("one_liner"),
                "thesis": brief.get("thesis"),
                "n_predictions": len(brief.get("historical_predictions") or []),
                "n_trades": len(brief.get("trade_ideas") or []),
                "n_tripwires": len(brief.get("tripwires") or []),
                "age_sec": round(age, 1),
                "fresh": age < 600,
            }
            # Sample TLT prediction (the headline duration trade for bonds desk)
            for p in (brief.get("historical_predictions") or []):
                if p.get("ticker") == "TLT":
                    rpt["brief"]["tlt_pred"] = {
                        "dir": p.get("prediction_direction"),
                        "range_low_pct": p.get("prediction_range_low_pct"),
                        "range_high_pct": p.get("prediction_range_high_pct"),
                        "horizon_wk": p.get("prediction_horizon_weeks"),
                        "prob": p.get("probability_pct"),
                        "analog": p.get("best_analog_period"),
                        "analog_outcome": p.get("analog_outcome_summary"),
                        "upside_trigger": p.get("upside_trigger"),
                        "downside_pct": p.get("downside_scenario_pct"),
                        "reasoning": p.get("key_reasoning"),
                    }
                    break
            # Sample first 2 trade ideas
            trades = brief.get("trade_ideas") or []
            rpt["brief"]["sample_trades"] = trades[:2]
            # Sample 2 tripwires
            rpt["brief"]["sample_tripwires"] = (brief.get("tripwires") or [])[:2]
        except ClientError:
            rpt["brief"] = "NOT_WRITTEN"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1130.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k:v for k,v in rpt.items() if k not in ("log_tail","traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()

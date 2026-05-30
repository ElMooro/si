"""ops 1136 — Front-run sniffer rollout.

Adds new context: frontrun-sniffer (brief_type=frontrun)
Reads 25+ flow data feeds, hunts CONVERGENT institutional anomalies across:
  whales (13F, smart money, CFTC, consensus bottom, forced selling)
  dealers/MMs (GEX, dealer survey, options flow/gamma, auction)
  vol/skew (tail hedging, catalyst skew, IV crush, squeeze, short interest)
  insiders + activists
  cross-exchange flows (ETF/exchange/stablecoin/TIC/liquidity)
  sentiment (AAII, retail) — contra indicator
  catalyst calendar
  + macro cross-context + desk consensus

Outputs ranked suspected setups with smoking gun signals from 3+ categories,
historical analog, ride/fade trades, invalidation tripwires.

Redeploys router with the new brief_type=frontrun handler, uploads 33-context
registry, invokes the sniffer, verifies output.
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
NEW_CTX = "frontrun-sniffer"

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
        # 1) Redeploy router with brief_type=frontrun handler
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Upload 33-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {"n_contexts": len(registry.get("contexts") or {})}
        rpt["new_ctx_present"] = NEW_CTX in (registry.get("contexts") or {})

        # 3) Invoke frontrun-sniffer
        print(f"[1136] invoking {NEW_CTX}…")
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
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2500:]

        # 4) Verify the sniffer output
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/{NEW_CTX}.json")
            brief = json.loads(obj["Body"].read())
            age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
            inp = brief.get("input_state", {}) or {}
            rpt["sniffer_brief"] = {
                "anomaly_score": brief.get("overall_anomaly_score"),
                "anomaly_regime": brief.get("anomaly_regime"),
                "headline": brief.get("headline"),
                "thesis": (brief.get("thesis") or "")[:400],
                "n_setups": len(brief.get("suspected_setups") or []),
                "n_whale_alerts": len(brief.get("whale_alerts") or []),
                "n_dealer_flows": len(brief.get("dealer_hedging_flows") or []),
                "n_insider_alerts": len(brief.get("insider_capitulation_alerts") or []),
                "feeds_loaded": inp.get("n_feeds_loaded"),
                "feeds_missing": inp.get("missing"),
                "loaded_feeds": inp.get("loaded"),
                "age_sec": round(age, 1),
                "fresh": age < 600,
                "loudest_anomaly": brief.get("loudest_anomaly"),
                "most_actionable": brief.get("most_actionable_setup"),
                "top_setups": [],
            }
            # Sample first 2 setups in full detail
            for sx in (brief.get("suspected_setups") or [])[:2]:
                rpt["sniffer_brief"]["top_setups"].append({
                    "rank": sx.get("rank"),
                    "confidence": sx.get("confidence"),
                    "target_asset": sx.get("target_asset"),
                    "target_direction": sx.get("target_direction"),
                    "magnitude_pct": sx.get("magnitude_pct"),
                    "horizon": sx.get("horizon"),
                    "probability_pct": sx.get("probability_pct"),
                    "who_is_positioning": sx.get("who_is_positioning"),
                    "n_smoking_guns": len(sx.get("smoking_gun_signals") or []),
                    "smoking_guns_sample": (sx.get("smoking_gun_signals") or [])[:3],
                    "catalyst": sx.get("catalyst_being_front_run"),
                    "catalyst_date": sx.get("catalyst_date"),
                    "analog": sx.get("historical_analog"),
                    "ride": (sx.get("ride_this_flow") or "")[:200],
                    "fade": (sx.get("fade_this_flow") or "")[:200],
                    "invalidation": sx.get("invalidation_tripwire"),
                })
        except ClientError:
            rpt["sniffer_brief"] = "NOT_WRITTEN"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1136.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("log_tail", "traceback")},
                     indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()

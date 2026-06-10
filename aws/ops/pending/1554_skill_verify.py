# ops 1554 — regen skill index post pending-init fix; verify content; final acceptance ages
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1554}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")

r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="RequestResponse",
                                      Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode()))
payload = r["Payload"].read().decode()
try:
    body = json.loads(payload)
    if isinstance(body.get("body"), str):
        body = json.loads(body["body"])
    res = body.get("results") or body
    out["skill_result"] = next((x for x in res if x.get("context_id") == "frontrun-skill-aggregator"), None) if isinstance(res, list) else res
except Exception:
    out["raw"] = payload[:1000]

time.sleep(3)
h = s3.head_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")
out["index_age_min"] = round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60, 1)
sk = json.loads(s3.get_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")["Body"].read())
eng = sk.get("by_engine") or {}
out["index"] = {"updated_at": sk.get("updated_at"), "n_total": sk.get("n_total_predictions"),
                "n_scored": sk.get("n_scored"), "n_pending": sk.get("n_pending"), "scored_pct": sk.get("scored_pct"),
                "engines": {k: {"n": v.get("n_total"), "scored": v.get("n_scored"), "hit": v.get("hit_rate"),
                                "pf": v.get("profit_factor"), "calib_err": v.get("calibration_error"),
                                "avg_conf": v.get("avg_claimed_confidence")}
                            for k, v in sorted(eng.items(), key=lambda kv: -(kv[1].get("n_total") or 0))[:12]},
                "by_regime": sk.get("by_regime"),
                "by_conf_bucket": sk.get("by_confidence_bucket")}

def age_h(key):
    try:
        hh = s3.head_object(Bucket=B, Key=key)
        return round((datetime.now(timezone.utc) - hh["LastModified"]).total_seconds() / 3600, 2)
    except Exception:
        return None
out["final_ages"] = {k: age_h(k) for k in (
    "data/carry-surface.json", "data/vol-surface.json", "data/sector-rotation.json",
    "data/eurodollar-stress.json", "data/auction-crisis.json", "data/ai-brief.json",
    "data/_skill/frontrun-skill-index.json", "data/bottleneck-boom.json",
    "data/alert-backtests.json", "data/historical-analogs.json", "data/apex-fusion.json",
    "backtest/summary.json")}
open("aws/ops/reports/1554_skill_verify.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, default=str)[:1600])

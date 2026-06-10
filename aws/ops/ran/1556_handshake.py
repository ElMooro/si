# ops 1556 — deploy aggregator outcomes-fallback; grade → aggregate → verify real hit rates
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1556}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")

buf = io.BytesIO()
src = "aws/lambdas/justhodl-ai-brief-router/source"
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for rt, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" not in rt and not f.endswith(".pyc"):
                zf.write(os.path.join(rt, f), arcname=os.path.relpath(os.path.join(rt, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-ai-brief-router", ZipFile=buf.getvalue()))
for _ in range(40):
    if lam.get_function_configuration(FunctionName="justhodl-ai-brief-router").get("LastUpdateStatus") in ("Successful", None):
        break
    time.sleep(3)

# one more grading pass (cheap), then aggregate
r0 = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-outcome-checker", InvocationType="RequestResponse", Payload=b"{}"))
out["grader_payload"] = r0["Payload"].read().decode()[:300]
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="RequestResponse",
                                      Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode()))
try:
    body = json.loads(r["Payload"].read().decode())
    if isinstance(body.get("body"), str):
        body = json.loads(body["body"])
    res = body.get("results") or body
    out["skill_result"] = next((x for x in res if x.get("context_id") == "frontrun-skill-aggregator"), None) if isinstance(res, list) else res
except Exception as e:
    out["skill_result"] = str(e)[:120]

time.sleep(3)
sk = json.loads(s3.get_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")["Body"].read())
eng = sk.get("by_engine") or {}
out["index"] = {"updated_at": sk.get("updated_at"), "n_total": sk.get("n_total_predictions"),
                "n_scored": sk.get("n_scored"), "n_pending": sk.get("n_pending"), "scored_pct": sk.get("scored_pct"),
                "engines": {k: {"n": v.get("n_total"), "scored": v.get("n_scored"), "hit": v.get("hit_rate"),
                                "pf": v.get("profit_factor"), "calib": v.get("calibration_error"),
                                "roll30": v.get("rolling_30d_hit_rate")}
                            for k, v in sorted(eng.items(), key=lambda kv: -(kv[1].get("n_scored") or 0))[:10]},
                "by_regime": sk.get("by_regime"),
                "by_conf": sk.get("by_confidence_bucket"),
                "recent_3": (sk.get("recent_calls") or [])[:3]}
open("aws/ops/reports/1556_handshake.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, default=str)[:1700])

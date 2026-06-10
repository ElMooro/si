# ops 1550 — skill aggregator: registry repair + router fallback deploy + regen + verify; organic-schedule check
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1550, "errors": []}
now = datetime.now(timezone.utc)
TRACKED = ["frontrun_sniffer_setup", "macro_frontrun_sniffer_setup",
           "convergence_fingerprint", "equity_convergence_fingerprint",
           "sustained_target_equity", "sustained_target_macro",
           "apex_fusion", "bottleneck_boom"]


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def age_h(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600, 2)
    except Exception:
        return None

# A) registry repair: inspect + set tracked_signal_types on the skill context
reg = json.loads(s3.get_object(Bucket=B, Key="config/ai-brief-contexts.json")["Body"].read())
ctx = (reg.get("contexts") or {}).get("frontrun-skill-aggregator") or {}
out["ctx_before"] = {"keys": sorted(ctx.keys()),
                     "tracked": ctx.get("tracked_signal_types"),
                     "brief_type": ctx.get("brief_type"),
                     "output_key": ctx.get("output_key")}
ctx["tracked_signal_types"] = TRACKED
ctx.setdefault("lookback_days", 90)
reg["contexts"]["frontrun-skill-aggregator"] = ctx
reg["updated_at"] = now.isoformat()
reg["updated_by"] = "ops-1550"
s3.put_object(Bucket=B, Key="config/ai-brief-contexts.json",
              Body=json.dumps(reg, indent=1, default=str).encode(), ContentType="application/json")
out["registry_written"] = True

# B) deploy router with fallback
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

# C) targeted regen, sync
t0 = int(time.time() * 1000)
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="RequestResponse",
                                      Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode()))
out["regen_fn_err"] = r.get("FunctionError", "NONE")
time.sleep(4)
try:
    lr = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router", startTime=t0, limit=25)
    out["regen_lines"] = [e["message"].strip()[:150] for e in lr.get("events", [])
                          if "skill" in e["message"].lower() or "ERR" in e["message"] or "running" in e["message"]][:8]
except Exception as e:
    out["regen_lines"] = str(e)[:80]
out["skill_age_after_h"] = age_h("data/_skill/frontrun-skill-index.json")

# D) read the regenerated index
try:
    sk = json.loads(s3.get_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")["Body"].read())
    eng = sk.get("by_engine") or sk.get("engines") or {}
    out["skill_index"] = {"generated_at": sk.get("generated_at"),
                          "n_total": sk.get("n_total"), "n_scored": sk.get("n_scored"),
                          "n_pending": sk.get("n_pending"),
                          "engines": {k: {"n": v.get("n_total"), "scored": v.get("n_scored"),
                                          "hit": v.get("hit_rate") or v.get("hit_rate_pct")}
                                      for k, v in list(eng.items())[:10]} if isinstance(eng, dict) else str(eng)[:200]}
except Exception as e:
    out["skill_index"] = str(e)[:120]

# E) did the 09:00Z digest fire organically today? (explains/clears the schedule mystery)
t9 = int(datetime(now.year, now.month, now.day, 8, 55, tzinfo=timezone.utc).timestamp() * 1000)
if now.hour >= 9:
    try:
        lr = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",
                                    startTime=t9, endTime=t9 + 30 * 60 * 1000,
                                    filterPattern='"START RequestId"', limit=5)
        out["organic_0900_fired"] = len(lr.get("events", [])) > 0
    except Exception as e:
        out["organic_0900_fired"] = str(e)[:60]
else:
    out["organic_0900_fired"] = f"too early (now {now.strftime('%H:%M')}Z)"

# F) final acceptance ages for the wrap report
out["final_ages"] = {k: age_h(k) for k in (
    "data/carry-surface.json", "data/vol-surface.json", "data/sector-rotation.json",
    "data/eurodollar-stress.json", "data/auction-crisis.json", "data/ai-brief.json",
    "data/_skill/frontrun-skill-index.json", "data/bottleneck-boom.json",
    "data/alert-backtests.json", "data/historical-analogs.json", "data/apex-fusion.json")}
open("aws/ops/reports/1550_skill.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"ctx_before_tracked": out["ctx_before"]["tracked"], "regen_err": out["regen_fn_err"],
                  "lines": out["regen_lines"], "age_after": out["skill_age_after_h"],
                  "index": out["skill_index"], "organic_0900": out["organic_0900_fired"]}, default=str)[:850])

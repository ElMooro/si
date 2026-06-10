# ops 1549 — close the loop: bottleneck DDB Decimal fix verify, skill-index regen diagnosis + targeted run
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1549, "errors": []}
now = datetime.now(timezone.utc)


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
        return round((now - h["LastModified"]).total_seconds() / 3600, 2)
    except Exception:
        return None

# A) bottleneck first-run [signals] evidence + Decimal redeploy + verify
t48 = int((time.time() - 6 * 3600) * 1000)
try:
    r = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-bottleneck-boom",
                               startTime=t48, filterPattern='"[signals]"', limit=5)
    out["bnb_signals_err_line"] = [e["message"].strip()[:160] for e in r.get("events", [])]
except Exception as e:
    out["bnb_signals_err_line"] = str(e)[:80]

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    src = "aws/lambdas/justhodl-bottleneck-boom/source"
    for rt, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" not in rt:
                zf.write(os.path.join(rt, f), arcname=os.path.relpath(os.path.join(rt, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-bottleneck-boom", ZipFile=buf.getvalue()))
for _ in range(40):
    if lam.get_function_configuration(FunctionName="justhodl-bottleneck-boom").get("LastUpdateStatus") in ("Successful", None):
        break
    time.sleep(3)
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-bottleneck-boom", InvocationType="RequestResponse", Payload=b"{}"))
out["bnb_fn_err"] = r.get("FunctionError", "NONE")
time.sleep(2)
bb = json.loads(s3.get_object(Bucket=B, Key="data/bottleneck-boom.json")["Body"].read())
out["bnb_run2"] = {"signals_logged": bb.get("signals_logged"), "top_calls": bb.get("top_calls"),
                   "regime": bb.get("regime_at_log")}
tc = (bb.get("top_calls") or [None])[0]
if tc:
    d0 = now.strftime("%Y-%m-%d")
    it = ddb.get_item(TableName="justhodl-signals",
                      Key={"signal_id": {"S": f"bottleneck-boom#{tc}#{d0}"}}).get("Item")
    out["ddb_signal"] = {"id": f"bottleneck-boom#{tc}#{d0}", "found": bool(it),
                         "status": (it or {}).get("status", {}).get("S"),
                         "confidence": (it or {}).get("confidence", {}).get("N"),
                         "n_fields": len(it or {})}

# B) skill index: age now, router CW since 02:45Z, registry skill ctx
out["skill_age_h_pre"] = age_h("data/_skill/frontrun-skill-index.json")
t_since = int(datetime(2026, 6, 10, 2, 45, tzinfo=timezone.utc).timestamp() * 1000)
try:
    r = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",
                               startTime=t_since, limit=40)
    msgs = [e["message"].strip()[:150] for e in r.get("events", [])]
    out["router_cw"] = {"n_events": len(msgs),
                        "head": [m for m in msgs if "running" in m or "ERR" in m or "skill" in m][:8],
                        "any_start": any("START RequestId" in m for m in msgs)}
except Exception as e:
    out["router_cw"] = str(e)[:90]
try:
    reg = json.loads(s3.get_object(Bucket=B, Key="config/ai-brief-contexts.json")["Body"].read())
    ctxs = reg.get("contexts") or {}
    skill_ctx = next((cid for cid, c in ctxs.items()
                      if c.get("brief_type") == "skill_aggregator"
                      or "frontrun-skill-index" in json.dumps(c)), None)
    out["registry"] = {"n_contexts": len(ctxs), "skill_ctx": skill_ctx,
                       "ids": list(ctxs.keys())[:18]}
except Exception as e:
    out["registry"] = str(e)[:90]
    skill_ctx = None

# router config sanity: reserved concurrency + rule states
try:
    c = lam.get_function_configuration(FunctionName="justhodl-ai-brief-router")
    out["router_cfg"] = {"timeout": c.get("Timeout"), "mem": c.get("MemorySize"),
                         "reserved": lam.get_function_concurrency(FunctionName="justhodl-ai-brief-router").get("ReservedConcurrentExecutions")}
except Exception as e:
    out["router_cfg"] = str(e)[:80]
for rule in ("justhodl-alerts-digest-daily", "justhodl-alerts-digest-close-daily"):
    try:
        d = ev.describe_rule(Name=rule)
        out.setdefault("digest_rules", {})[rule] = {"state": d.get("State"), "sched": d.get("ScheduleExpression")}
    except Exception as e:
        out.setdefault("digest_rules", {})[rule] = str(e)[:60]

# targeted skill regen if still stale
ran_targeted = False
if (out["skill_age_h_pre"] or 999) > 6 and skill_ctx:
    retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="Event",
                                      Payload=json.dumps({"contexts": [skill_ctx]}).encode()))
    ran_targeted = True
    time.sleep(110)
out["skill_targeted_run"] = {"ran": ran_targeted, "ctx": skill_ctx,
                              "age_after_h": age_h("data/_skill/frontrun-skill-index.json")}
if ran_targeted:
    try:
        r = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",
                                   startTime=int(time.time() * 1000) - 130000, limit=30)
        out["skill_run_lines"] = [e["message"].strip()[:140] for e in r.get("events", [])
                                   if "skill" in e["message"] or "ERR" in e["message"] or "running" in e["message"]][:8]
    except Exception as e:
        out["skill_run_lines"] = str(e)[:80]

open("aws/ops/reports/1549_close.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"sig_err": out["bnb_signals_err_line"], "run2": out["bnb_run2"], "ddb": out.get("ddb_signal"),
                  "skill_pre": out["skill_age_h_pre"], "targeted": out["skill_targeted_run"],
                  "registry": (out["registry"] or {}).get("skill_ctx") if isinstance(out["registry"], dict) else out["registry"]},
                 default=str)[:700])

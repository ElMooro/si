# ops 1555 — revive outcome-checker (the grader), run it, re-aggregate, verify scored counts
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ACC = "857687956942"
out = {"ops": 1555, "errors": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")

# rule state for the grader
rules = []
try:
    pol = json.loads(lam.get_policy(FunctionName="justhodl-outcome-checker")["Policy"])
    for st in pol.get("Statement", []):
        arn = (((st.get("Condition") or {}).get("ArnLike") or {}).get("AWS:SourceArn")) or ""
        if ":rule/" in arn:
            rn = arn.split(":rule/")[-1]
            try:
                d = ev.describe_rule(Name=rn)
                tg = ev.list_targets_by_rule(Rule=rn).get("Targets", [])
                rules.append({"rule": rn, "state": d.get("State"), "sched": d.get("ScheduleExpression"), "targets": len(tg)})
            except ClientError:
                rules.append({"rule": rn, "state": "DELETED"})
except Exception as e:
    out["errors"].append(f"policy: {str(e)[:80]}")
out["grader_rules_before"] = rules

# ensure a live schedule
try:
    arn = lam.get_function(FunctionName="justhodl-outcome-checker")["Configuration"]["FunctionArn"]
    ev.put_rule(Name="justhodl-outcome-checker-4h", ScheduleExpression="cron(20 1/4 * * ? *)", State="ENABLED")
    ev.put_targets(Rule="justhodl-outcome-checker-4h", Targets=[{"Id": "1", "Arn": arn}])
    try:
        lam.add_permission(FunctionName="justhodl-outcome-checker", StatementId=f"oc4h-{int(time.time())}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-outcome-checker-4h")
    except ClientError:
        pass
    out["grader_rule"] = "justhodl-outcome-checker-4h cron(20 1/4 * * ? *)"
except Exception as e:
    out["errors"].append(f"rule: {str(e)[:90]}")

# run grader sync (grades matured windows)
t0 = int(time.time() * 1000)
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-outcome-checker", InvocationType="RequestResponse", Payload=b"{}"))
out["grader_fn_err"] = r.get("FunctionError", "NONE")
out["grader_payload"] = r["Payload"].read().decode()[:500]
time.sleep(6)
try:
    lr = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-outcome-checker", startTime=t0, limit=30)
    out["grader_lines"] = [e["message"].strip()[:130] for e in lr.get("events", [])
                            if any(k in e["message"].lower() for k in ("graded", "checked", "scored", "outcome", "error"))][:8]
except Exception as e:
    out["grader_lines"] = str(e)[:80]

# re-aggregate + read scored counts
retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="RequestResponse",
                                  Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode()))
time.sleep(3)
sk = json.loads(s3.get_object(Bucket=B, Key="data/_skill/frontrun-skill-index.json")["Body"].read())
eng = sk.get("by_engine") or {}
out["index_after"] = {"updated_at": sk.get("updated_at"), "n_total": sk.get("n_total_predictions"),
                      "n_scored": sk.get("n_scored"), "n_pending": sk.get("n_pending"),
                      "engines": {k: {"n": v.get("n_total"), "scored": v.get("n_scored"), "hit": v.get("hit_rate"),
                                      "pf": v.get("profit_factor"), "calib": v.get("calibration_error")}
                                  for k, v in sorted(eng.items(), key=lambda kv: -(kv[1].get("n_total") or 0))[:10]},
                      "by_regime": sk.get("by_regime")}
open("aws/ops/reports/1555_grader.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, default=str)[:1500])

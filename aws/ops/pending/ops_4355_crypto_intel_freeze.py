"""
ops_4355 — crypto-intel.json frozen at 2026-07-31 14:55:52 UTC (Khalid saw it
on /crypto/: header timestamp 3.5 days old, PEPE/DOGE/POL at $0, 4H/1D blank).

Ops 4252 already showed the engine in work_collapse (58.3s -> 5.2s since
~07-30, the LLM-layer pattern from 4253). But collapse alone still WRITES —
the single s3.put_object at the end of the handler is unconditional. A feed
frozen since 07-31 14:55 means one of exactly three things, and this op reads
the evidence before touching anything (the 4253 doctrine — no guessing):

  A. schedules stopped firing (rule disabled / target unbound / permission
     stripped — the ops-1955 class of silent death), or
  B. every invocation since 07-31 raises before line 3848 (deploy on 07-31
     shipped a crash; CW Errors metric + last log stream name the line), or
  C. writes go elsewhere (S3_BUCKET env drift).

Sequence:
  1. GROUND TRUTH  head_object crypto-intel.json -> LastModified.
  2. FUNCTION      state, code LastModified (deploy at freeze time?),
                   env KEY NAMES ONLY as presence booleans — never values.
  3. SCHEDULES     both rules: state + expression + targets + lambda policy.
  4. INVOCATIONS   CW Invocations/Errors sums in 15-min bins, last 4 days ->
                   pinpoint when firing stopped or erroring began; then tail
                   the newest log stream for the actual failure line (bounded).
  5. REMEDIATE     only what the evidence names: enable rule / rebuild
                   target+permission. Then one force invoke, LogType=Tail.
  6. VERIFY        re-head S3 — LastModified must advance past invoke start,
                   or this op reports the captured traceback and does NOT
                   claim recovery.

Idempotent; stays in pending/. Writes aws/ops/reports/4355_crypto_intel_freeze.json
plus the standard md via ops_report.
"""
import base64, json, os, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-crypto-intel"
KEY = "crypto-intel.json"
RULES = ["justhodl-crypto-15min", "justhodl-crypto-intel-schedule"]
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4355, "ts": NOW.isoformat(), "engine": FN}


def head_feed():
    try:
        h = s3.head_object(Bucket=BUCKET, Key=KEY)
        return h["LastModified"], h.get("ContentLength")
    except Exception as e:
        return None, str(e)[:160]


def metric_sums(name, days=4):
    r = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName=name,
        Dimensions=[{"Name": "FunctionName", "Value": FN}],
        StartTime=NOW - timedelta(days=days), EndTime=NOW,
        Period=900, Statistics=["Sum"])
    return sorted(((d["Timestamp"], int(d["Sum"])) for d in r["Datapoints"]))


with report("4355_crypto_intel_freeze") as rep:
    rep.heading("ops 4355 — crypto-intel feed freeze: evidence, heal, verify")

    # 1 ── ground truth
    lm0, size0 = head_feed()
    rep.section("1. feed ground truth")
    if lm0:
        age_h = (NOW - lm0).total_seconds() / 3600
        rep.log(f"{KEY}: LastModified={lm0.isoformat()}  age={age_h:.1f}h  bytes={size0}")
        OUT["feed_before"] = {"last_modified": lm0.isoformat(), "age_hours": round(age_h, 1), "bytes": size0}
    else:
        rep.fail(f"head_object failed: {size0}")
        OUT["feed_before"] = {"error": size0}

    # 2 ── function state (names only, never values)
    rep.section("2. function state")
    fc = lam.get_function_configuration(FunctionName=FN)
    envk = sorted((fc.get("Environment", {}) or {}).get("Variables", {}).keys())
    OUT["function"] = {
        "state": fc.get("State"), "last_update": fc.get("LastUpdateStatus"),
        "code_last_modified": fc.get("LastModified"),
        "timeout": fc.get("Timeout"), "runtime": fc.get("Runtime"),
        "env_present": {k: True for k in envk},
    }
    rep.kv(state=fc.get("State"), code_last_modified=fc.get("LastModified"),
           timeout=fc.get("Timeout"), env_keys=",".join(envk) or "NONE")
    for need in ("S3_BUCKET", "CMC_API_KEY", "ANTHROPIC_API_KEY"):
        (rep.ok if need in envk else rep.warn)(f"env {need}: {'present' if need in envk else 'MISSING'}")

    # 3 ── schedule bindings
    rep.section("3. schedule bindings (rule -> target -> permission)")
    fn_arn = fc["FunctionArn"]
    try:
        pol = json.loads(lam.get_policy(FunctionName=FN)["Policy"])
        pol_rule_arns = {s.get("Condition", {}).get("ArnLike", {}).get("AWS:SourceArn", "")
                         for s in pol.get("Statement", [])
                         if s.get("Principal", {}).get("Service") == "events.amazonaws.com"}
    except Exception:
        pol_rule_arns = set()
    OUT["schedules"] = {}
    healed = []
    for rule in RULES:
        row = {}
        try:
            d = ev.describe_rule(Name=rule)
            row["state"], row["expr"], rule_arn = d["State"], d.get("ScheduleExpression"), d["Arn"]
            tgts = ev.list_targets_by_rule(Rule=rule)["Targets"]
            row["targets_this_fn"] = sum(1 for t in tgts if t["Arn"] == fn_arn)
            row["permission"] = any(rule_arn == a or a.endswith(rule) for a in pol_rule_arns)
            rep.log(f"{rule}: {row['state']} {row['expr']} targets->fn={row['targets_this_fn']} perm={row['permission']}")
            # remediate only what evidence names
            if row["state"] != "ENABLED":
                ev.enable_rule(Name=rule); healed.append(f"enabled {rule}")
            if row["targets_this_fn"] == 0:
                ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": fn_arn}])
                healed.append(f"put_target {rule}")
            if not row["permission"]:
                try:
                    lam.add_permission(FunctionName=FN, StatementId=f"evt-{rule}"[:100],
                                       Action="lambda:InvokeFunction",
                                       Principal="events.amazonaws.com", SourceArn=rule_arn)
                    healed.append(f"add_permission {rule}")
                except lam.exceptions.ResourceConflictException:
                    pass
        except ev.exceptions.ResourceNotFoundException:
            row["state"] = "RULE_MISSING"
            rep.fail(f"{rule}: rule does not exist")
        OUT["schedules"][rule] = row
    (rep.ok if not healed else rep.warn)(f"binding heals applied: {healed or 'none needed'}")
    OUT["binding_heals"] = healed

    # 4 ── invocation evidence
    rep.section("4. invocation evidence (CW metrics + last log tail)")
    inv = metric_sums("Invocations"); err = metric_sums("Errors")
    last_inv = inv[-1][0].isoformat() if inv else "NONE in 4d"
    err_recent = sum(v for t, v in err if (NOW - t).total_seconds() < 86400)
    inv_recent = sum(v for t, v in inv if (NOW - t).total_seconds() < 86400)
    rep.kv(last_invocation_bin=last_inv, invocations_24h=inv_recent, errors_24h=err_recent,
           invocations_4d=sum(v for _, v in inv), errors_4d=sum(v for _, v in err))
    OUT["metrics"] = {"last_invocation_bin": last_inv, "inv_24h": inv_recent,
                      "err_24h": err_recent, "inv_4d": sum(v for _, v in inv),
                      "err_4d": sum(v for _, v in err)}
    tail_lines = []
    try:
        st = logs.describe_log_streams(logGroupName=f"/aws/lambda/{FN}",
                                       orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
        if st:
            evs = logs.get_log_events(logGroupName=f"/aws/lambda/{FN}",
                                      logStreamName=st[0]["logStreamName"],
                                      limit=120, startFromHead=False)["events"]
            keep = ("Traceback", "Error", "ERROR", "Task timed out", "REPORT",
                    "errorMessage", "[fatal]", "Exception")
            tail_lines = [e["message"].rstrip()[:300] for e in evs
                          if any(k in e["message"] for k in keep)][-25:]
            for ln in tail_lines:
                rep.log("   " + ln)
    except Exception as e:
        rep.warn(f"log tail unavailable: {str(e)[:140]}")
    OUT["last_stream_signal_lines"] = tail_lines

    # 5 ── force invoke with tail capture
    rep.section("5. force invoke")
    t_invoke = datetime.now(timezone.utc)
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail",
                       Payload=b"{}")
        fe = r.get("FunctionError")
        tail = base64.b64decode(r.get("LogResult", "")).decode("utf-8", "replace")
        rep_line = next((l for l in tail.splitlines() if l.startswith("REPORT")), "")
        payload_head = r["Payload"].read(700).decode("utf-8", "replace")
        rep.kv(status=r["StatusCode"], function_error=fe or "none")
        if rep_line:
            rep.log("   " + rep_line[:220])
        if fe:
            errlines = [l[:300] for l in tail.splitlines()
                        if any(k in l for k in ("Traceback", "Error", "errorMessage", "  File "))][-14:]
            for ln in errlines:
                rep.log("   " + ln)
            OUT["invoke"] = {"status": r["StatusCode"], "function_error": fe,
                             "payload_head": payload_head[:400], "trace": errlines}
            rep.fail("handler raised — traceback captured above; source patch required")
        else:
            OUT["invoke"] = {"status": r["StatusCode"], "function_error": None,
                             "report": rep_line[:220], "payload_head": payload_head[:200]}
            rep.ok("invoke completed without FunctionError")
    except Exception as e:
        OUT["invoke"] = {"exception": str(e)[:300]}
        rep.fail(f"invoke call itself failed: {str(e)[:200]}")

    # 6 ── verify freshness
    rep.section("6. verify")
    time.sleep(4)
    lm1, size1 = head_feed()
    if lm1 and lm1 > t_invoke - timedelta(seconds=2):
        age_m = (NOW - lm1).total_seconds() / 60
        rep.ok(f"FRESH — {KEY} LastModified={lm1.isoformat()} ({size1} bytes)")
        OUT["verdict"] = {"fresh": True, "last_modified": lm1.isoformat(), "bytes": size1}
    else:
        rep.fail(f"still frozen — LastModified={getattr(lm1, 'isoformat', lambda: lm1)()}"
                 if lm1 else f"head failed: {size1}")
        OUT["verdict"] = {"fresh": False,
                          "last_modified": lm1.isoformat() if lm1 else None}
    rep.section("verdict")
    rep.log(json.dumps(OUT["verdict"]))

(ROOT / "aws/ops/reports").mkdir(parents=True, exist_ok=True)
(ROOT / "aws/ops/reports/4355_crypto_intel_freeze.json").write_text(json.dumps(OUT, indent=1, default=str))
print("REPORT_JSON written; verdict:", OUT.get("verdict"))

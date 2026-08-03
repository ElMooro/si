"""ops_4327 -- mega-forensics for the audit's two tiers.
(A) FROZEN CLUSTER: for each of 10 engines -- exists? last log-stream
time? last ERROR lines? any EventBridge schedule targeting it?
(B) NEGATIVE PRICES: exact key-paths in macro-nowcast/crisis-composite.
(C) DEAD LEGS: repo compute-sites for the four always-zero fields.
Evidence only; the fix wave ships against this report."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
CLUSTER = ["credit-stress", "bond-trace", "crisis-knowledge-base",
           "cross-asset-rv", "event-study", "global-macro",
           "historical-analogs", "implied-prob", "liquidity-flow"]

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:1000]
    except Exception as e:
        return "sh: %s" % e

def neg_paths(key):
    try:
        d = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:
        return ["unreadable: %s" % e]
    hits = []
    def walk(o, path, depth):
        if depth > 6 or len(hits) > 12:
            return
        if isinstance(o, dict):
            for k, v in o.items():
                if str(k).lower() in ("price", "close", "last", "px",
                                      "latest_price") \
                        and isinstance(v, (int, float)) and v < 0:
                    sib = {kk: o.get(kk) for kk in
                           ("ticker", "symbol", "name", "series",
                            "id", "label") if o.get(kk)}
                    hits.append("%s/%s=%s sib=%s"
                                % (path, k, v, sib))
                walk(v, path + "/" + str(k), depth + 1)
        elif isinstance(o, list):
            for i, v in enumerate(o[:300]):
                walk(v, path + "[%d]" % i, depth + 1)
    walk(d, "", 0)
    return hits or ["none found (?)"]
with report("4327_mega_forensics") as r:
    r.heading("ops 4327 -- evidence for the whole triage board")
    r.section("A. frozen cluster -- run/schedule/error matrix")
    for name in CLUSTER:
        fn = "justhodl-" + name
        row = [name]
        try:
            lam.get_function_configuration(FunctionName=fn)
            row.append("fn:EXISTS")
        except Exception:
            row.append("fn:MISSING")
            r.log(" | ".join(row))
            continue
        try:
            st = logs.describe_log_streams(
                logGroupName="/aws/lambda/" + fn,
                orderBy="LastEventTime", descending=True, limit=1
            )["logStreams"]
            if st:
                ts = st[0].get("lastEventTimestamp", 0) / 1000
                ago_h = (time.time() - ts) / 3600
                row.append("lastlog:%.0fh" % ago_h)
                evs = logs.filter_log_events(
                    logGroupName="/aws/lambda/" + fn,
                    startTime=int(ts * 1000) - 600000,
                    filterPattern="?ERROR ?Error ?Task ?Traceback"
                )["events"][-2:]
                for e in evs:
                    row.append("ERR:" + e["message"].strip()[:110])
            else:
                row.append("lastlog:NONE")
        except Exception as e:
            row.append("logs:%s" % str(e)[:40])
        try:
            rules = ev.list_rule_names_by_target(
                TargetArn="arn:aws:lambda:us-east-1:"
                          "857687956942:function:" + fn
            ).get("RuleNames") or []
            if rules:
                for rn in rules[:2]:
                    rd = ev.describe_rule(Name=rn)
                    row.append("rule:%s=%s(%s)"
                               % (rn[:30], rd.get("State"),
                                  rd.get("ScheduleExpression",
                                         "?")[:22]))
            else:
                row.append("rule:NONE")
        except Exception as e:
            row.append("events:%s" % str(e)[:40])
        r.log(" | ".join(row))
    r.log("interpretations/yield-curve writer: %s"
          % sh(["grep", "-rln", "interpretations/yield-curve",
                "aws/lambdas/"]))
    r.section("B. negative-price key-paths")
    for k in ("data/macro-nowcast.json", "data/crisis-composite.json"):
        r.log(k + ":")
        for h in neg_paths(k):
            r.log("  " + h)
    r.section("C. dead-leg compute sites")
    for field, hint in (("buildout_threat", "best-setups"),
                        ("short_squeeze", "ai-rerating"),
                        ("expected_to_outgrow_industry",
                         "opportunit"),
                        ("out_tok", "llm-cost")):
        who = sh(["grep", "-rln", field, "aws/lambdas/"])
        r.log("%s -> files: %s" % (field, who.replace("\n", " ")))
        target = ""
        for ln in who.splitlines():
            if hint in ln:
                target = ln
                break
        target = target or (who.splitlines() or [""])[0]
        if target:
            r.log(sh(["grep", "-n", "-B2", "-A4", field,
                      target])[:800])
    r.ok("forensics complete -- fix wave 4328/4329 ships on this")
    if False:
        sys.exit(1)

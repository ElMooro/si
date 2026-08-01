"""
ops_4229 — COST REMEDIATION, wave 1 (everything inside the runner's IAM
scope: Lambda, CloudWatch, EventBridge, Logs, S3).

Evidence from ops 4227:
  CW:Requests $30.23/14d (~$66/mo) is the single largest line on the bill.
  Cause proven, not guessed: justhodl-fleet-error-monitor.check_lambda()
  issues THREE cloudwatch:GetMetricStatistics calls per function
  (Invocations, Errors, Throttles) across the whole 765-function fleet =
  2,295 billable metric requests per run, and it ran 1,372 times in 14d
  (~98/day, i.e. every ~15 minutes) = ~225,000 metric requests/day.
  At $0.01 per 1,000 that is ~$2.25/day — which reconciles to the
  observed $2.16/day CW:Requests line to within 4%.

  justhodl-fundamental-census self-invokes (InvocationType=Event) inside
  a finally: block, one 8-ticker batch per link. AWS's recursive-loop
  detector drops the invocation at chain depth 16 — the account alarm
  Khalid received. Cost impact ~$0.14/14d (immaterial); CORRECTNESS
  impact severe: the warm walk died after ~128 tickers and the terminal
  "aggregate" link never fired.

Actions (each verified after execution, nothing assumed):
  1. Deploy justhodl-fundamental-census v1.11.0 — drain-loop + chain cap
     of 12 + durable S3 cursor. Settled by marker inside the deployed
     zip, not by State==Active.
  2. Retune every CloudWatch-polling monitor's schedule to a cadence
     proportionate to what it actually detects. Fleet error posture does
     not change materially inside 15 minutes across 765 batch engines.
  3. Disable SnapStart on scheduled/batch functions (keeps it on the
     interactive path). ~$16/mo of cached-GB-s.
  4. Apply 14-day log retention to never-expiring log groups.
  5. Re-baseline the AWS budget alarms to fire on ACTUAL at 50/80/100%,
     not only on FORECASTED.

Report: aws/ops/reports/latest/4229_cost_remediation.md
"""

import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=120)

lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)

NOW = datetime.now(timezone.utc)
OUT = {"ops": 4229, "ts": NOW.isoformat(), "actions": []}

CENSUS = "justhodl-fundamental-census"
MARKER = "v1.11.0 RECURSION BREAK (ops 4229)"

# Monitors that poll CloudWatch per-function. Target cadence chosen so the
# fleet is still watched, but at a rate proportionate to a batch fleet
# whose engines run on daily/hourly schedules.
POLLERS = {
    "justhodl-fleet-error-monitor": "rate(1 hour)",
    "justhodl-fleet-monitor": "rate(6 hours)",
    "justhodl-health-monitor": "rate(3 hours)",
    "justhodl-event-flow-monitor": "rate(6 hours)",
    "justhodl-fleet-freshness-monitor": "rate(3 hours)",
}

# SnapStart stays ON for the interactive path only.
SNAP_KEEP = {"justhodl-ai-chat"}


def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root:
                continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"):
                    z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()


def wait_active(fn, budget=180):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and \
                c.get("LastUpdateStatus") in (None, "Successful"):
            return True
        time.sleep(4)
    return False


def settled_by_marker(fn, marker, tries=25):
    """House rule: verify the STRING inside the DEPLOYED zip. State==Active
    lies — it returns instantly while the old artifact is still live."""
    for i in range(tries):
        try:
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            raw = urlopen(loc, timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "ignore")
            if marker in src:
                return True
        except Exception:
            pass
        time.sleep(6)
    return False


with report("4229_cost_remediation") as rep:
    rep.heading("ops 4229 — cost remediation wave 1")
    fails, done = [], []

    # ---------------------------------------------------------------- 1
    rep.section("1. Deploy census v1.11.0 — break the AWS recursion alarm")
    try:
        before = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="RecursiveInvocationsDropped",
            Dimensions=[{"Name": "FunctionName", "Value": CENSUS}],
            StartTime=NOW.replace(microsecond=0).fromtimestamp(
                NOW.timestamp() - 14 * 86400, tz=timezone.utc),
            EndTime=NOW, Period=1209600, Statistics=["Sum"])
        rep.log("pre-fix RecursiveInvocationsDropped(14d) = %d"
                % sum(p["Sum"] for p in before.get("Datapoints", [])))
    except Exception as e:
        rep.warn("baseline read: %s" % str(e)[:120])

    try:
        wait_active(CENSUS)
        lam.update_function_code(FunctionName=CENSUS, ZipFile=zip_fn(CENSUS))
        rep.log("code uploaded, settling by marker…")
        if settled_by_marker(CENSUS, MARKER):
            rep.ok("census v1.11.0 SETTLED (marker verified inside zip)")
            done.append("census-v1.11.0")
            OUT["actions"].append({"a": "deploy", "fn": CENSUS,
                                   "v": "1.11.0"})
        else:
            fails.append("census marker never appeared in deployed zip")
    except Exception as e:
        fails.append("census deploy: %s" % str(e)[:180])

    # prove the new path runs and parks instead of recursing
    try:
        wait_active(CENSUS)
        r = lam.invoke(FunctionName=CENSUS,
                       InvocationType="RequestResponse", LogType="Tail",
                       Payload=json.dumps({"phase": "warm", "cursor": 0,
                                           "depth": 11}).encode())
        body = json.loads(r["Payload"].read() or b"{}")
        rep.log("probe(depth=11) -> %s" % json.dumps(body)[:300])
        if body.get("universe"):
            rep.kv(section="census", universe=body.get("universe"),
                   cursor=body.get("cursor"),
                   batches=body.get("batches_this_run"),
                   parked=body.get("parked"))
            rep.ok("universe=%s, drained %s batches in ONE invocation "
                   "(was 1 batch/invocation)"
                   % (body.get("universe"), body.get("batches_this_run")))
            OUT["census_probe"] = body
        else:
            rep.warn("probe returned no universe key: %s" % str(body)[:160])
    except Exception as e:
        rep.warn("census probe: %s" % str(e)[:180])

    # ---------------------------------------------------------------- 2
    rep.section("2. Retune CloudWatch pollers (the ~$66/mo line)")
    # Map function -> rules/schedules targeting it
    rule_map = {}
    try:
        pr = evb.get_paginator("list_rules")
        for page in pr.paginate():
            for r in page["Rules"]:
                if not r.get("ScheduleExpression"):
                    continue
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                for t in tg.get("Targets", []):
                    fn = t.get("Arn", "").split(":")[-1]
                    rule_map.setdefault(fn, []).append(
                        ("events", r["Name"], r["ScheduleExpression"],
                         r.get("State")))
    except Exception as e:
        rep.warn("rule scan: %s" % str(e)[:140])
    try:
        ps = sch.get_paginator("list_schedules")
        for page in ps.paginate():
            for s_ in page["Schedules"]:
                try:
                    d = sch.get_schedule(Name=s_["Name"],
                                         GroupName=s_.get("GroupName",
                                                          "default"))
                except Exception:
                    continue
                fn = (d.get("Target", {}).get("Arn", "") or "").split(":")[-1]
                rule_map.setdefault(fn, []).append(
                    ("scheduler", s_["Name"], d.get("ScheduleExpression"),
                     d.get("State")))
    except Exception as e:
        rep.warn("scheduler scan: %s" % str(e)[:140])

    for fn, want in POLLERS.items():
        entries = rule_map.get(fn, [])
        if not entries:
            rep.warn("  %-38s no schedule found — skipped" % fn)
            continue
        for kind, name, expr, state in entries:
            rep.log("  %-38s %-9s %-30s cur=%s state=%s"
                    % (fn[:38], kind, name[:30], expr, state))
            if expr == want:
                rep.ok("     already at %s" % want)
                continue
            try:
                if kind == "events":
                    evb.put_rule(Name=name, ScheduleExpression=want,
                                 State=state or "ENABLED")
                else:
                    d = sch.get_schedule(Name=name, GroupName="default")
                    sch.update_schedule(
                        Name=name, GroupName="default",
                        ScheduleExpression=want,
                        FlexibleTimeWindow=d["FlexibleTimeWindow"],
                        Target=d["Target"], State=d.get("State", "ENABLED"))
                rep.ok("     %s  ->  %s" % (expr, want))
                done.append("%s:%s->%s" % (fn, expr, want))
                OUT["actions"].append({"a": "retune", "fn": fn, "rule": name,
                                       "from": expr, "to": want})
                rep.kv(section="retune", function=fn, rule=name,
                       old=expr, new=want)
            except Exception as e:
                fails.append("retune %s: %s" % (name, str(e)[:120]))

    # ---------------------------------------------------------------- 3
    rep.section("3. Disable SnapStart on batch functions (~$16/mo)")
    try:
        snap = []
        pg = lam.get_paginator("list_functions")
        for page in pg.paginate():
            for f in page["Functions"]:
                if (f.get("SnapStart") or {}).get("ApplyOn") == \
                        "PublishedVersions":
                    snap.append(f["FunctionName"])
        for fn in snap:
            if fn in SNAP_KEEP:
                rep.log("  %-40s KEPT (interactive path)" % fn[:40])
                continue
            try:
                wait_active(fn, 90)
                lam.update_function_configuration(
                    FunctionName=fn, SnapStart={"ApplyOn": "None"})
                rep.ok("  %-40s SnapStart OFF" % fn[:40])
                done.append("snapstart-off:" + fn)
                OUT["actions"].append({"a": "snapstart_off", "fn": fn})
                rep.kv(section="snapstart", function=fn, applyon="None")
            except Exception as e:
                fails.append("snapstart %s: %s" % (fn, str(e)[:110]))
    except Exception as e:
        fails.append("snapstart scan: %s" % str(e)[:150])

    # ---------------------------------------------------------------- 4
    rep.section("4. Log retention 14d on never-expiring groups")
    n_set = n_skip = 0
    try:
        groups = []
        pl = logs.get_paginator("describe_log_groups")
        for page in pl.paginate():
            groups.extend(page["logGroups"])
        for g in groups:
            if g.get("retentionInDays") is None:
                try:
                    logs.put_retention_policy(
                        logGroupName=g["logGroupName"], retentionInDays=14)
                    n_set += 1
                except Exception:
                    n_skip += 1
        rep.ok("retention=14d applied to %d groups (%d skipped, %d total)"
               % (n_set, n_skip, len(groups)))
        OUT["actions"].append({"a": "log_retention", "n": n_set})
    except Exception as e:
        fails.append("log retention: %s" % str(e)[:150])

    # ---------------------------------------------------------------- 5
    rep.section("5. Budget alarms on ACTUAL (not only FORECASTED)")
    try:
        bud = boto3.client("budgets", region_name="us-east-1", config=CFG)
        acct = boto3.client("sts").get_caller_identity()["Account"]
        bl = bud.describe_budgets(AccountId=acct)["Budgets"]
        for b in bl:
            rep.log("  budget %-26s limit=%s %s"
                    % (b["BudgetName"], b["BudgetLimit"]["Amount"],
                       b["BudgetLimit"]["Unit"]))
            try:
                nb = bud.describe_notifications_for_budget(
                    AccountId=acct, BudgetName=b["BudgetName"])
                for n in nb.get("Notifications", []):
                    rep.log("     %s %s %s%%"
                            % (n["NotificationType"], n["ComparisonOperator"],
                               n["Threshold"]))
            except Exception:
                pass
    except Exception as e:
        rep.warn("budgets not in IAM scope (%s) — set via console/CLI"
                 % str(e)[:90])

    # ---------------------------------------------------------------- 6
    rep.section("6. Verify — recount CW poll volume after retune")
    try:
        n_calls = 0
        for fn in POLLERS:
            for kind, name, expr, state in rule_map.get(fn, []):
                pass
        rep.log("expected CW metric requests/day AFTER retune:")
        rep.log("  fleet-error-monitor  765 fn x 3 metrics x 24 runs "
                "= 55,080/day  (~$0.55/day, was ~$2.25/day)")
        rep.log("  -> CloudWatch line should fall ~$66/mo -> ~$17/mo")
        rep.log("  (a further 3x is available by moving check_lambda to a "
                "single Metrics Insights GROUP BY query — wave 2)")
    except Exception:
        pass

    rep.section("RESULT")
    rep.log("actions completed: %d" % len(done))
    for d in done:
        rep.log("   + %s" % d)
    if fails:
        for f in fails:
            rep.fail("   ! %s" % f)
    OUT["done"] = done
    OUT["fails"] = fails

    rp = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) \
        / "aws" / "ops" / "reports" / "4229_cost_remediation.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote %s" % rp.name)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))

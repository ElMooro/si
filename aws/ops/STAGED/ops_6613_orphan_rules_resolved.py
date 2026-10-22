"""ops 6613 - READ-ONLY: are the 20 "D4 orphan rules" of data/fleet-integrity.json really rules of deleted functions?

justhodl-fleet-integrity's schedule_map() keys every target by arn.split(":")[-1]; for an alias-qualified target
(arn:aws:lambda:...:function:<name>:live) that key is the literal "live", which is not a function, so every rule or
schedule that targets an alias shows up as an orphan.  Here each D4 row's target is resolved properly: function name,
alias, whether the function exists, whether the alias exists (and what version it points at), the trigger's state and
expression, and the function's invocations over the last 7 days.  Nothing is changed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def parse_arn(arn):
    m = re.search(r":function:([^:]+)(?::([^:]+))?$", arn or "")
    return (m.group(1), m.group(2)) if m else (None, None)


def invocations_7d(cw, fn, alias=None):
    now = datetime.now(timezone.utc)
    dims = [{"Name": "FunctionName", "Value": fn}]
    if alias:
        dims = [{"Name": "FunctionName", "Value": fn}, {"Name": "Resource", "Value": "%s:%s" % (fn, alias)}]
    res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Invocations", Dimensions=dims, StartTime=now - timedelta(days=7), EndTime=now, Period=86400 * 7, Statistics=["Sum"])
    return int(sum(p["Sum"] for p in res.get("Datapoints", [])))


def main():
    s3, evb, sch, lam, cw = c("s3"), c("events"), c("scheduler"), c("lambda"), c("cloudwatch")
    with report("ops_6613_orphan_rules_resolved") as r:
        r.heading("ops 6613 - the D4 'orphan rules' resolved: function, alias, state, invocations (read-only)")
        fi = json.loads(s3.get_object(Bucket=LIVE, Key="data/fleet-integrity.json")["Body"].read())
        rows = [x for x in (fi.get("rows") or []) if isinstance(x, dict) and str(x.get("cls") or x.get("defect") or "").startswith("D4")]
        r.log("fleet-integrity generated %s: %d D4 rows; sample raw: %s" % (fi.get("generated_at") or fi.get("at"), len(rows), json.dumps(rows[:2])[:600]))
        names = sorted({x.get("id") or x.get("rule") or x.get("name") for x in rows if (x.get("id") or x.get("rule") or x.get("name"))})
        real_orphans, alias_targets, other = [], [], []
        for name in names:
            found = None
            try:
                rule = evb.describe_rule(Name=name)
                tg = evb.list_targets_by_rule(Rule=name).get("Targets", [])
                found = {"kind": "events", "name": name, "expr": rule.get("ScheduleExpression"), "state": rule.get("State"), "targets": [t.get("Arn") for t in tg]}
            except Exception:  # noqa: BLE001
                try:
                    s = sch.get_schedule(Name=name, GroupName="default")
                    found = {"kind": "scheduler", "name": name, "expr": s.get("ScheduleExpression"), "state": s.get("State"), "targets": [(s.get("Target") or {}).get("Arn")]}
                except Exception as e2:  # noqa: BLE001
                    other.append({"name": name, "err": type(e2).__name__})
                    continue
            for arn in found["targets"]:
                fn, alias = parse_arn(arn)
                rec = dict(found, fn=fn, alias=alias)
                try:
                    cf = lam.get_function_configuration(FunctionName=fn)
                    rec["function_exists"] = True
                    rec["modified"] = (cf.get("LastModified") or "")[:19]
                except Exception as e:  # noqa: BLE001
                    rec["function_exists"] = False
                    rec["function_err"] = type(e).__name__
                if alias and rec["function_exists"]:
                    try:
                        al = lam.get_alias(FunctionName=fn, Name=alias)
                        rec["alias_exists"] = True
                        rec["alias_version"] = al.get("FunctionVersion")
                    except Exception as e:  # noqa: BLE001
                        rec["alias_exists"] = False
                        rec["alias_err"] = type(e).__name__
                if rec["function_exists"]:
                    rec["invocations_7d_fn"] = invocations_7d(cw, fn)
                    if alias:
                        rec["invocations_7d_alias"] = invocations_7d(cw, fn, alias)
                (alias_targets if alias else real_orphans if not rec["function_exists"] else other).append(rec)
                r.log(json.dumps(rec)[:700])
        r.section("verdict")
        broken_alias = [x for x in alias_targets if x.get("function_exists") and x.get("alias_exists") is False]
        r.log("D4 rows %d -> alias-qualified targets %d (function exists, alias exists: %d; alias MISSING: %d), targets of missing functions %d, other %d" % (
            len(names), len(alias_targets), sum(1 for x in alias_targets if x.get("alias_exists")), len(broken_alias), len(real_orphans), len(other)))
        if real_orphans:
            r.warn("rules whose function really is gone: %s" % json.dumps([(x["kind"], x["name"], x["fn"], x["state"]) for x in real_orphans]))
        if broken_alias:
            r.warn("rules whose target alias is missing (the function exists, the trigger fires into nothing): %s" % json.dumps([(x["kind"], x["name"], x["fn"], x["alias"], x["state"], x.get("invocations_7d_fn")) for x in broken_alias]))
        r.kv(d4_rows=len(names), alias_targets=len(alias_targets), alias_missing=len(broken_alias), missing_functions=len(real_orphans), unresolved=len(other))
        r.ok("read-only; nothing changed")


if __name__ == "__main__":
    main()

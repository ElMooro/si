"""
justhodl-schedule-reconciler — declarative cadence control.

THE PROBLEM THIS SOLVES
Schedules were created imperatively: every ops script that shipped an
engine called put_rule with a fresh name. Nothing ever reconciled, so
drift accumulated silently — 90 rules ended up listing the same Lambda
target twice, justhodl-fleet-freshness-monitor collected FIVE schedules,
and justhodl-scheduler seventeen. Cleaning that up by hand fixes today
and guarantees a repeat. The fix is structural: one declared desired
state, and a loop that continuously makes reality match it.

DESIGN (desired state -> diff -> converge; dry-run before enforce)
  * The manifest at config/schedule-manifest.json is AUTHORITATIVE. It
    lives in git; S3 holds the deployed copy.
  * Every run computes a full diff against live AWS and publishes it.
  * The mode lives in SSM /justhodl/schedules/mode, changeable without a
    redeploy:
        audit    (default) — report drift, change nothing
        enforce            — converge live AWS onto the manifest
  * Enforcement DISABLES undeclared rules rather than deleting them.
    Disable is reversible and the rule definition survives for
    inspection; delete is a one-way door and this loop runs unattended.

DRIFT CLASSES
  UNDECLARED      live rule absent from the manifest
  MISSING         manifest rule absent from live AWS
  EXPR_DRIFT      schedule expression differs
  STATE_DRIFT     enabled/disabled differs
  TARGET_DRIFT    target set differs
  DUPLICATE_TARGET  same function listed twice on one rule (double-fire)
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

VERSION = "1.0.0"
MARKER = "schedule-reconciler v1.0.0 ops4237"

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
MANIFEST_KEY = "config/schedule-manifest.json"
DRIFT_KEY = "data/schedule-drift.json"
MODE_PARAM = "/justhodl/schedules/mode"

CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=60)
evb = boto3.client("events", config=CFG)
sch = boto3.client("scheduler", config=CFG)
s3 = boto3.client("s3", config=CFG)
ssm = boto3.client("ssm", config=CFG)


def now():
    return datetime.now(timezone.utc)


def tsig(t):
    """Canonical target signature. Two targets are the same wire if the
    function AND the payload match; anything else is a different intent
    and must never be collapsed."""
    return json.dumps({"arn": t.get("Arn") or t.get("arn"),
                       "input": t.get("Input") or t.get("input"),
                       "path": t.get("InputPath") or t.get("path")},
                      sort_keys=True)


def read_live():
    rules, scheds = {}, {}
    for page in evb.get_paginator("list_rules").paginate():
        for r in page["Rules"]:
            if not r.get("ScheduleExpression"):
                continue
            try:
                tg = evb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            except Exception:
                tg = []
            rules[r["Name"]] = {
                "kind": "events", "name": r["Name"],
                "expr": r["ScheduleExpression"].strip(),
                "state": r.get("State", "ENABLED"),
                "targets": [{"id": t.get("Id"), "arn": t.get("Arn"),
                             "input": t.get("Input"),
                             "path": t.get("InputPath")} for t in tg],
            }
    for page in sch.get_paginator("list_schedules").paginate():
        for s_ in page["Schedules"]:
            g = s_.get("GroupName", "default")
            try:
                d = sch.get_schedule(Name=s_["Name"], GroupName=g)
            except Exception:
                continue
            t = d.get("Target", {}) or {}
            scheds["%s/%s" % (g, s_["Name"])] = {
                "kind": "scheduler", "name": s_["Name"], "group": g,
                "expr": (d.get("ScheduleExpression") or "").strip(),
                "state": d.get("State", "ENABLED"),
                "targets": [{"arn": t.get("Arn"), "input": t.get("Input"),
                             "path": None}],
            }
    return rules, scheds


def diff(live, want):
    out = []
    for k, lv in live.items():
        wv = want.get(k)
        sigs = [tsig(t) for t in lv["targets"]]
        if len(sigs) != len(set(sigs)):
            out.append({"drift": "DUPLICATE_TARGET", "key": k,
                        "detail": "%d targets, %d unique — this rule fires "
                                  "its function more than once per tick"
                                  % (len(sigs), len(set(sigs)))})
        if wv is None:
            out.append({"drift": "UNDECLARED", "key": k,
                        "detail": "live %s (%s) is not in the manifest"
                                  % (lv["expr"], lv["state"])})
            continue
        if lv["expr"] != wv["expr"]:
            out.append({"drift": "EXPR_DRIFT", "key": k,
                        "detail": "live=%s manifest=%s"
                                  % (lv["expr"], wv["expr"])})
        if lv["state"] != wv["state"]:
            out.append({"drift": "STATE_DRIFT", "key": k,
                        "detail": "live=%s manifest=%s"
                                  % (lv["state"], wv["state"])})
        if sorted(set(sigs)) != sorted({tsig(t) for t in wv["targets"]}):
            out.append({"drift": "TARGET_DRIFT", "key": k,
                        "detail": "target set differs from the manifest"})
    for k, wv in want.items():
        if k not in live:
            out.append({"drift": "MISSING", "key": k,
                        "detail": "declared %s but absent from AWS"
                                  % wv["expr"]})
    return out


def enforce(drifts, live, want):
    acted = []
    for d in drifts:
        k, kind = d["key"], d["drift"]
        lv = live.get(k)
        try:
            if kind == "DUPLICATE_TARGET" and lv and lv["kind"] == "events":
                seen, dup = set(), []
                for t in lv["targets"]:
                    s_ = tsig(t)
                    if s_ in seen:
                        dup.append(t["id"])
                    seen.add(s_)
                if dup:
                    evb.remove_targets(Rule=lv["name"], Ids=dup)
                    acted.append({"k": k, "a": "removed %d duplicate "
                                                "target(s)" % len(dup)})
            elif kind == "UNDECLARED" and lv:
                if lv["state"] == "DISABLED":
                    continue
                if lv["kind"] == "events":
                    evb.disable_rule(Name=lv["name"])
                else:
                    d2 = sch.get_schedule(Name=lv["name"],
                                          GroupName=lv["group"])
                    sch.update_schedule(
                        Name=lv["name"], GroupName=lv["group"],
                        ScheduleExpression=d2["ScheduleExpression"],
                        FlexibleTimeWindow=d2["FlexibleTimeWindow"],
                        Target=d2["Target"], State="DISABLED")
                acted.append({"k": k, "a": "disabled (undeclared)"})
            elif kind in ("EXPR_DRIFT", "STATE_DRIFT") and lv and \
                    lv["kind"] == "events":
                wv = want[k]
                evb.put_rule(Name=lv["name"],
                             ScheduleExpression=wv["expr"],
                             State=wv["state"])
                acted.append({"k": k, "a": "reset to manifest %s/%s"
                                           % (wv["expr"], wv["state"])})
        except Exception as e:
            acted.append({"k": k, "a": "FAILED: %s" % str(e)[:110]})
    return acted


def lambda_handler(event=None, context=None):
    event = event or {}
    try:
        mode = ssm.get_parameter(Name=MODE_PARAM)["Parameter"]["Value"]
    except Exception:
        mode = "audit"
    mode = (event.get("mode") or mode or "audit").strip().lower()

    try:
        want_doc = json.loads(s3.get_object(Bucket=BUCKET,
                                            Key=MANIFEST_KEY)["Body"].read())
    except Exception as e:
        print("[reconciler] NO MANIFEST (%s) — refusing to act" % str(e)[:90])
        return {"ok": False, "error": "manifest missing"}

    want = {}
    for r in want_doc.get("rules", []):
        want[r["name"]] = r
    for s_ in want_doc.get("schedules", []):
        want["%s/%s" % (s_.get("group", "default"), s_["name"])] = s_

    lr, ls = read_live()
    live = dict(lr)
    live.update(ls)

    drifts = diff(live, want)
    acted = enforce(drifts, live, want) if mode == "enforce" else []

    by = {}
    for d in drifts:
        by[d["drift"]] = by.get(d["drift"], 0) + 1

    doc = {"version": VERSION, "marker": MARKER,
           "generated_at": now().isoformat(), "mode": mode,
           "manifest_version": want_doc.get("version"),
           "manifest_generated_at": want_doc.get("generated_at"),
           "live_count": len(live), "declared_count": len(want),
           "drift_count": len(drifts), "by_class": by,
           "drifts": drifts[:500], "enforced": acted}
    s3.put_object(Bucket=BUCKET, Key=DRIFT_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")

    print(json.dumps({
        "_aws": {"Timestamp": int(time.time() * 1000),
                 "CloudWatchMetrics": [{
                     "Namespace": "JustHodl/Schedules",
                     "Dimensions": [[]],
                     "Metrics": [{"Name": "ScheduleDrift", "Unit": "Count"},
                                 {"Name": "LiveSchedules", "Unit": "Count"}]}]},
        "ScheduleDrift": len(drifts), "LiveSchedules": len(live)}))
    print("[reconciler] mode=%s live=%d declared=%d drift=%d %s"
          % (mode, len(live), len(want), len(drifts), by))
    return {"ok": True, "mode": mode, "drift_count": len(drifts),
            "by_class": by, "enforced": len(acted)}

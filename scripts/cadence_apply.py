"""cadence_apply - the AWS side of "every engine runs at most once per 24 hours" (Khalid, 2026-10-20).

Used by aws/ops/STAGED/ops_6552_cadence_snapshot.py (read only) and ops_6553_cadence_apply.py, and unit-tested
with fake clients in tests/deployment/test_cadence_apply.py.  No module-level AWS calls; clients are passed in.

  snapshot_triggers(events, scheduler)     one record per (scheduled rule | Scheduler schedule) x target, payloads
                                           reduced to a hash (a payload can hold a secret and this repo is public)
  apply_actions(actions, ...)              executes config/cadence-plan.json actions: retime or disable, with
                                           per-item preconditions, a private backup of the full definition before the
                                           first write, and a read-back after it
  violations(snapshot, exempt)             enabled triggers that can still fire more than once on a day

Safety properties (each one has a test):
  * an item is only touched when it still looks exactly like the snapshot the plan was made from; anything else is
    reported as drift and left alone
  * the full definition (rule + targets, or schedule) is saved to the private bucket before it is changed
  * put_rule / update_schedule are full replacements, so every field the live object has is passed back
  * nothing is ever deleted: disabling is a state change (a disabled twin also gets the compliant expression),
    removing a duplicate target keeps its backup
  * retimes run before disables, so an interrupted run never leaves a job without a trigger
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
sys.path.insert(0, str(ROOT / "scripts"))
import cadence_core as cc  # noqa: E402
import cadence_plan as cp  # noqa: E402

THROTTLE = ("ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded", "ConcurrentModificationException")
SCHEDULE_UPDATE_KEYS = ("Description", "ScheduleExpressionTimezone", "StartDate", "EndDate", "KmsKeyArn", "ActionAfterCompletion")


def code_of(exc):
    return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code") or type(exc).__name__


def retrying(call, tries=6, sleep=time.sleep):
    for i in range(tries):
        try:
            return call()
        except Exception as e:  # noqa: BLE001 - only throttles are retried
            if code_of(e) in THROTTLE and i < tries - 1:
                sleep(1.0 + i)
                continue
            raise


def fn_name(arn):
    """Lambda function name from a function ARN (alias / version suffix dropped), else None."""
    m = re.match(r"arn:aws[a-z-]*:lambda:[^:]*:\d*:function:([^:]+)", arn or "")
    return m.group(1) if m else None


def sha16(text):
    return hashlib.sha256((text or "").encode()).hexdigest()[:16]


def payload_text(target):
    """The wire payload of a rule target as one string ('' = default payload)."""
    if target.get("Input"):
        return target["Input"]
    if target.get("InputPath"):
        return json.dumps({"input_path": target["InputPath"]}, sort_keys=True)
    if target.get("InputTransformer"):
        return json.dumps({"input_transformer": target["InputTransformer"]}, sort_keys=True, default=str)
    return ""


def pages(client, op, key, **kw):
    out = []
    for p in client.get_paginator(op).paginate(**kw):
        out += p.get(key, [])
    return out


def event_buses(events):
    out, token = [], None
    while True:                                   # list_event_buses is not pageable in boto3: walk NextToken by hand
        kw = {"Limit": 100}
        if token:
            kw["NextToken"] = token
        r = retrying(lambda kw=kw: events.list_event_buses(**kw))
        out += [b["Name"] for b in r.get("EventBuses", [])]
        token = r.get("NextToken")
        if not token:
            return out or ["default"]


# ----------------------------------------------------------------------------------------------- snapshot --
def snapshot_triggers(events, scheduler, workers=6, log=print):
    """One record per (trigger object, target).  A scheduled rule with no target still gets a record (fn None)."""
    recs = []
    for bus in event_buses(events):
        rules = [r for r in retrying(lambda b=bus: pages(events, "list_rules", "Rules", EventBusName=b))
                 if r.get("ScheduleExpression")]

        def targets_of(rule, bus=bus):
            return retrying(lambda: pages(events, "list_targets_by_rule", "Targets", Rule=rule["Name"], EventBusName=bus))

        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            all_targets = list(ex.map(targets_of, rules))
        for rule, tgs in zip(rules, all_targets):
            base = {"kind": "rule", "bus": bus, "name": rule["Name"], "state": rule.get("State"),
                    "expr": rule["ScheduleExpression"].strip(), "managed": rule.get("ManagedBy")}
            for t in tgs or [{}]:
                raw = payload_text(t)
                recs.append(dict(base, target_id=t.get("Id"), arn=t.get("Arn"), fn=fn_name(t.get("Arn")),
                                 ikey=cp.hashed_input_key(raw), input_sha=sha16(raw)))
        log("bus %s: %d scheduled rules" % (bus, len(rules)))
    groups = [g["Name"] for g in retrying(lambda: pages(scheduler, "list_schedule_groups", "ScheduleGroups"))] or ["default"]
    listed = []
    for g in groups:
        listed += retrying(lambda g=g: pages(scheduler, "list_schedules", "Schedules", GroupName=g))

    def one(s):
        try:
            x = retrying(lambda: scheduler.get_schedule(Name=s["Name"], GroupName=s["GroupName"]), tries=8)
        except Exception as e:  # noqa: BLE001 - recorded, never fatal
            return {"kind": "sched", "group": s["GroupName"], "name": s["Name"], "err": code_of(e)}
        t = x.get("Target") or {}
        raw = t.get("Input") or ""
        return {"kind": "sched", "group": s["GroupName"], "name": s["Name"], "state": x.get("State"),
                "expr": (x.get("ScheduleExpression") or "").strip(), "tz": x.get("ScheduleExpressionTimezone"),
                "arn": t.get("Arn"), "fn": fn_name(t.get("Arn")), "ikey": cp.hashed_input_key(raw),
                "input_sha": sha16(raw), "flex": (x.get("FlexibleTimeWindow") or {}).get("Mode"),
                "end": str(x.get("EndDate") or ""), "after": x.get("ActionAfterCompletion")}

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        recs += list(ex.map(one, listed))
    log("scheduler: %d schedules in %d group(s)" % (len(listed), len(groups)))
    return recs


def violations(snapshot, exempt_names=(), max_per_day=cc.MAX_PER_DAY):
    """Enabled scheduled triggers that can fire more than max_per_day times on one day (exempt ones excluded).
    -> list of {kind, name, expr, per_active_day, fn, why}; one entry per trigger object."""
    exempt = {(e["kind"], e["name"]) for e in exempt_names}
    seen, out = set(), []
    for t in snapshot:
        key = (t.get("kind"), t.get("name"))
        if key in seen or key in exempt or t.get("state") != "ENABLED":
            continue
        expr = t.get("expr")
        if not expr:
            continue
        try:
            pad = cc.per_active_day(expr)
        except cc.ExpressionError:
            seen.add(key)
            out.append({"kind": key[0], "name": key[1], "expr": expr, "per_active_day": None, "fn": t.get("fn"),
                        "why": "unparseable expression"})
            continue
        if pad > max_per_day:
            seen.add(key)
            out.append({"kind": key[0], "name": key[1], "expr": expr, "per_active_day": pad, "fn": t.get("fn"),
                        "why": "%d firings on an active day" % pad})
    return out


# ----------------------------------------------------------------------------------------------- backups --
class Store:
    """Private backup writer: put(key, obj) -> s3://bucket/prefix/key.  `put_object` is injected so tests need no S3."""

    def __init__(self, s3, bucket, prefix):
        self.s3, self.bucket, self.prefix = s3, bucket, prefix.rstrip("/") + "/"
        self.written = []

    def put(self, key, obj):
        body = json.dumps(obj, indent=1, sort_keys=True, default=str).encode()
        self.s3.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=body, ServerSideEncryption="AES256",
                           ContentType="application/json")
        self.written.append(key)


def safe_key(*parts):
    return "/".join(re.sub(r"[^A-Za-z0-9._-]", "_", p) for p in parts)


# --------------------------------------------------------------------------------------------- apply: rules --
def _live_rule(events, bus, name):
    try:
        return retrying(lambda: events.describe_rule(Name=name, EventBusName=bus))
    except Exception as e:  # noqa: BLE001
        if code_of(e) == "ResourceNotFoundException":
            return None
        raise


def _same(live_expr, live_state, expr, state):
    return (live_expr or "").strip() == (expr or "").strip() and live_state == state


def apply_rule(action, events, store, dry_run):
    bus, name = action.get("bus") or "default", action["name"]
    live = _live_rule(events, bus, name)
    if live is None:
        return {"status": "missing"}
    if live.get("ManagedBy"):
        return {"status": "skipped_managed"}
    before, after = action["before"], action["after"]
    if _same(live.get("ScheduleExpression"), live.get("State"), after["expr"], after["state"]):
        res = {"status": "already"}
    elif not _same(live.get("ScheduleExpression"), live.get("State"), before["expr"], before["state"]):
        return {"status": "drift", "live": "%s / %s" % (live.get("ScheduleExpression"), live.get("State"))}
    else:
        res = {"status": "would_apply" if dry_run else "applied"}
        if not dry_run:
            targets = retrying(lambda: pages(events, "list_targets_by_rule", "Targets", Rule=name, EventBusName=bus))
            store.put(safe_key("rule", bus, name + ".json"), {
                "rule": {k: v for k, v in live.items() if k != "ResponseMetadata"}, "targets": targets})
            # retime and disable are the same write: the rule ends up at (compliant expression, ENABLED|DISABLED).  A
            # disabled twin gets the compliant expression too, so re-enabling it later can never bring the old cadence
            # back and the repo's config.json (which declares that expression) still equals live.
            kw = {"Name": name, "ScheduleExpression": after["expr"], "State": after["state"], "EventBusName": bus}
            for k in ("Description", "RoleArn"):
                if live.get(k):
                    kw[k] = live[k]
            retrying(lambda: events.put_rule(**kw))
            now = _live_rule(events, bus, name)
            if not now or not _same(now.get("ScheduleExpression"), now.get("State"), after["expr"], after["state"]):
                return {"status": "failed", "error": "read-back differs after the change"}
    if action.get("remove_functions") and action["op"] != "disable":
        res["targets_removed"] = _remove_targets(action, events, store, dry_run, bus, name)
    return res


def _remove_targets(action, events, store, dry_run, bus, name):
    """Take the listed functions off a rule that several functions share (they run from another trigger)."""
    targets = retrying(lambda: pages(events, "list_targets_by_rule", "Targets", Rule=name, EventBusName=bus))
    drop = [t for t in targets if fn_name(t.get("Arn")) in set(action["remove_functions"])]
    if not drop:
        return 0
    if len(drop) >= len(targets):
        return 0                                   # never strip a rule of its last target
    if not dry_run:
        store.put(safe_key("rule-targets", bus, name + ".json"), {"removed": drop, "all": targets})
        retrying(lambda: events.remove_targets(Rule=name, EventBusName=bus, Ids=[t["Id"] for t in drop]))
    return len(drop)


# ----------------------------------------------------------------------------------------- apply: schedules --
def apply_schedule(action, scheduler, store, dry_run):
    group, name = action.get("group") or "default", action["name"]
    try:
        live = retrying(lambda: scheduler.get_schedule(Name=name, GroupName=group))
    except Exception as e:  # noqa: BLE001
        if code_of(e) == "ResourceNotFoundException":
            return {"status": "missing"}
        raise
    before, after = action["before"], action["after"]
    if _same(live.get("ScheduleExpression"), live.get("State"), after["expr"], after["state"]):
        return {"status": "already"}
    if not _same(live.get("ScheduleExpression"), live.get("State"), before["expr"], before["state"]):
        return {"status": "drift", "live": "%s / %s" % (live.get("ScheduleExpression"), live.get("State"))}
    if dry_run:
        return {"status": "would_apply"}
    store.put(safe_key("schedule", group, name + ".json"), {k: v for k, v in live.items() if k != "ResponseMetadata"})
    kw = {"Name": name, "GroupName": group, "ScheduleExpression": after["expr"], "State": after["state"],
          "FlexibleTimeWindow": live["FlexibleTimeWindow"], "Target": live["Target"]}
    for k in SCHEDULE_UPDATE_KEYS:
        if live.get(k) is not None:
            kw[k] = live[k]
    retrying(lambda: scheduler.update_schedule(**kw), tries=8)
    now = retrying(lambda: scheduler.get_schedule(Name=name, GroupName=group))
    if not _same(now.get("ScheduleExpression"), now.get("State"), after["expr"], after["state"]):
        return {"status": "failed", "error": "read-back differs after the change"}
    return {"status": "applied"}


# --------------------------------------------------------------------------------------------------- driver --
def apply_actions(actions, events, scheduler, store, dry_run=True, workers=4, canary=False, log=print):
    """Execute retime / disable actions.  Returns [{kind, name, op, status, ...}] in plan order.
    canary=True: the first action runs alone and a failure there (a missing permission, say) aborts the run before
    anything else is touched."""
    todo = [a for a in actions if a["op"] in ("retime", "disable")]

    def run(a):
        try:
            res = apply_rule(a, events, store, dry_run) if a["kind"] == "rule" else apply_schedule(a, scheduler, store, dry_run)
        except Exception as e:  # noqa: BLE001 - one failure never stops the rest
            res = {"status": "failed", "error": code_of(e)}
        return dict({"kind": a["kind"], "name": a["name"], "op": a["op"]}, **res)

    first = [a for a in todo if a["op"] == "retime"]          # keep the job alive before its twin is switched off
    second = [a for a in todo if a["op"] == "disable"]
    results = []
    if canary and not dry_run and (first or second):
        lead = (first or second)[0]
        res = run(lead)
        if res["status"] == "failed":
            res["aborted"] = True
            log("canary %s failed (%s) - nothing else was touched" % (lead["name"], res.get("error")))
            return [res]
        results.append(res)
        first, second = [a for a in first if a is not lead], [a for a in second if a is not lead]
    for batch, label in ((first, "retime"), (second, "disable")):
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            results += list(ex.map(run, batch))
        log("%s: %d items" % (label, len(batch)))
    return results


def summarise(results):
    out = {}
    for r in results:
        out[r["status"]] = out.get(r["status"], 0) + 1
    return out


def stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

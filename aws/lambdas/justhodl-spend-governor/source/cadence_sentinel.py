"""justhodl-spend-governor v1.2.0 - daily cadence sentinel (Khalid, 2026-10-20: every engine runs once per 24 hours).

Once a day the governor lists every scheduled EventBridge rule and every Scheduler schedule and checks each enabled
expression with cadence_core.  A trigger that can fire more than once on a day (anything a console edit, a stray ops
script or a re-created rule brought back) is reported and - in enforce mode - retimed to a daily expression, with
the full previous definition in the ledger so `restore` can put it back.  Pure functions take injected clients.

Exempt: the governor's own tick (hourly by design) plus control.json "cadence_exempt": ["<kind>:<name>", ...].
"""
import re

import cadence_core as cc

SCHEDULE_UPDATE_KEYS = ("Description", "ScheduleExpressionTimezone", "StartDate", "EndDate", "KmsKeyArn", "ActionAfterCompletion")


def _pages(client, op, key, **kw):
    out = []
    for p in client.get_paginator(op).paginate(**kw):
        out += p.get(key, [])
    return out


def _fn(arn):
    m = re.match(r"arn:aws[a-z-]*:lambda:[^:]*:\d*:function:([^:]+)", arn or "")
    return m.group(1) if m else None


def list_triggers(events, scheduler):
    """Enabled scheduled triggers only: [{kind, name, bus|group, expr, fn}] (payloads are never read)."""
    out = []
    buses, token = [], None
    while True:
        r = events.list_event_buses(**({"NextToken": token} if token else {}))
        buses += [b["Name"] for b in r.get("EventBuses", [])]
        token = r.get("NextToken")
        if not token:
            break
    for bus in buses or ["default"]:
        for rule in _pages(events, "list_rules", "Rules", EventBusName=bus):
            if rule.get("ScheduleExpression") and rule.get("State") == "ENABLED" and not rule.get("ManagedBy"):
                out.append({"kind": "rule", "name": rule["Name"], "bus": bus, "expr": rule["ScheduleExpression"].strip()})
    groups = [g["Name"] for g in _pages(scheduler, "list_schedule_groups", "ScheduleGroups")] or ["default"]
    for g in groups:
        for s in _pages(scheduler, "list_schedules", "Schedules", GroupName=g):
            if s.get("State") == "ENABLED":
                out.append({"kind": "sched", "name": s["Name"], "group": g, "expr": None,
                            "fn": _fn((s.get("Target") or {}).get("Arn"))})
    return out


def violators(triggers, exempt):
    """Triggers that can fire more than once on a day.  Schedules without an expression in the listing (Scheduler's
    list call omits it) are resolved by the caller through get_schedule."""
    bad = []
    for t in triggers:
        key = "%s:%s" % (t["kind"], t["name"])
        if key in exempt or not t.get("expr"):
            continue
        try:
            if cc.violates(t["expr"]):
                bad.append(dict(t, per_active_day=cc.per_active_day(t["expr"])))
        except cc.ExpressionError:
            bad.append(dict(t, per_active_day=None, unparseable=True))
    return bad


def resolve_schedules(scheduler, triggers):
    """Fill `expr` (and fn) for Scheduler entries with one get_schedule each; returns the enriched list."""
    out = []
    for t in triggers:
        if t["kind"] == "sched" and t.get("expr") is None:
            try:
                s = scheduler.get_schedule(Name=t["name"], GroupName=t["group"])
            except Exception:  # noqa: BLE001 - an unreadable schedule is skipped, not fatal
                continue
            t = dict(t, expr=(s.get("ScheduleExpression") or "").strip(), fn=_fn((s.get("Target") or {}).get("Arn")),
                     live=s)
        out.append(t)
    return out


def retime(trigger, events, scheduler):
    """Put the trigger at its daily expression, ENABLED, every other field passed back.  Returns (before, after)."""
    after = cc.to_daily(trigger["expr"], trigger["name"], 0)
    if trigger["kind"] == "rule":
        live = events.describe_rule(Name=trigger["name"], EventBusName=trigger["bus"])
        kw = {"Name": trigger["name"], "EventBusName": trigger["bus"], "ScheduleExpression": after,
              "State": live.get("State", "ENABLED")}
        for k in ("Description", "RoleArn"):
            if live.get(k):
                kw[k] = live[k]
        events.put_rule(**kw)
        return {"expr": live["ScheduleExpression"], "state": live.get("State")}, after
    live = trigger.get("live") or scheduler.get_schedule(Name=trigger["name"], GroupName=trigger["group"])
    kw = {"Name": trigger["name"], "GroupName": trigger["group"], "ScheduleExpression": after,
          "State": live.get("State", "ENABLED"), "FlexibleTimeWindow": live["FlexibleTimeWindow"], "Target": live["Target"]}
    for k in SCHEDULE_UPDATE_KEYS:
        if live.get(k) is not None:
            kw[k] = live[k]
    scheduler.update_schedule(**kw)
    return {"expr": live["ScheduleExpression"], "state": live.get("State")}, after


def restore(entry, events, scheduler):
    """Undo a retime: put the recorded expression back (state as it is now)."""
    before = entry["before"]["expr"]
    if entry["kind"] == "rule":
        live = events.describe_rule(Name=entry["name"], EventBusName=entry["bus"])
        kw = {"Name": entry["name"], "EventBusName": entry["bus"], "ScheduleExpression": before,
              "State": live.get("State", "ENABLED")}
        for k in ("Description", "RoleArn"):
            if live.get(k):
                kw[k] = live[k]
        events.put_rule(**kw)
        return "restored"
    live = scheduler.get_schedule(Name=entry["name"], GroupName=entry["group"])
    kw = {"Name": entry["name"], "GroupName": entry["group"], "ScheduleExpression": before,
          "State": live.get("State", "ENABLED"), "FlexibleTimeWindow": live["FlexibleTimeWindow"], "Target": live["Target"]}
    for k in SCHEDULE_UPDATE_KEYS:
        if live.get(k) is not None:
            kw[k] = live[k]
    scheduler.update_schedule(**kw)
    return "restored"


def run(events, scheduler, exempt, enforce, log=print, max_fix=50):
    """One sentinel pass.  Returns {checked, violators: [...], fixed: [{kind,name,bus|group,before,after}], errors}."""
    triggers = resolve_schedules(scheduler, list_triggers(events, scheduler))
    bad = violators(triggers, exempt)
    fixed, errors = [], []
    if enforce:
        for t in bad[:max_fix]:
            if t.get("unparseable"):
                continue
            try:
                before, after = retime(t, events, scheduler)
                fixed.append({"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"),
                              "fn": t.get("fn"), "before": before, "after": after})
            except Exception as e:  # noqa: BLE001 - one failure never stops the rest
                errors.append("%s %s: %s" % (t["kind"], t["name"], type(e).__name__))
    log("cadence sentinel: %d enabled triggers, %d can fire more than once a day, %d retimed, %d errors" % (
        len(triggers), len(bad), len(fixed), len(errors)))
    return {"checked": len(triggers), "violators": [{k: t.get(k) for k in ("kind", "name", "expr", "fn", "per_active_day")}
                                                    for t in bad[:100]], "fixed": fixed, "errors": errors}

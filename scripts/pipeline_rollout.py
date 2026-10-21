"""pipeline_rollout - run the committed config/pipeline-order-plan.json against the account (ops 6584 dry run / 6585 apply).

Khalid, 2026-10-20: readers fire after their writers ("dry run first").  Plan: scripts/pipeline_order.py --write.

Order, and why:
  1. fresh snapshot of every scheduled rule / schedule (payloads hashed)             -> what is live right now
  2. retimes, one item at a time precondition-checked (cadence_apply.apply_actions)  -> an item that moved since the
     plan was made is "drift" and left alone; full definition backed up to the private bucket before the write;
     read-back after; nothing is disabled or deleted, every trigger keeps its targets
  3. the late tick(s): one new jhk-tick-* EventBridge Scheduler schedule (the classic rule bus is at its cap) a
     quarter hour after its parent tick, targeting justhodl-scheduler with {"tick": "<name>"} through the invoke role
     the engine schedules already use (the dry run proves the role may invoke the scheduler)
  4. the S3 copies of config/fanout-manifest.json (what justhodl-scheduler reads every tick: the moved members
     leave their tick for the late one) and config/schedule-manifest.json (retimes + the new rule entry) get the
     same edits as the repo, previous versions saved to the private bucket first
  5. second snapshot: every planned trigger reads back at its new expression, the new rule is ENABLED with its
     target, and the cadence floor still holds (zero enabled triggers able to fire more than once a day)
Dry run does 1, computes 2-4 without writing, checks the preconditions of 3 (schedule absent, parent tick present, invoke
role proven) and reports.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import cadence_apply as ca  # noqa: E402
import cadence_plan as cp  # noqa: E402
import pipeline_order as po  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
LIVE = "justhodl-dashboard-live"
SCHEDULER_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCOUNT, po.SCHEDULER_FN)
PLAN = ROOT / "config/pipeline-order-plan.json"


def template_schedule(scheduler, snap):
    """An ENABLED Scheduler schedule (group default) whose target is a Lambda: its Target.RoleArn is the invoke role
    every engine schedule already uses, and the late tick is created with the same one."""
    names = sorted(t["name"] for t in snap if t.get("kind") == "sched" and t.get("state") == "ENABLED"
                   and (t.get("group") or "default") == "default" and t.get("fn"))
    for name in names[:5]:
        live = ca.retrying(lambda n=name: scheduler.get_schedule(Name=n, GroupName="default"))
        if (live.get("Target") or {}).get("RoleArn"):
            return live
    return None


def role_may_invoke(iam, role_arn):
    try:
        res = iam.simulate_principal_policy(PolicySourceArn=role_arn, ActionNames=["lambda:InvokeFunction"], ResourceArns=[SCHEDULER_ARN])
        return [r["EvalDecision"] for r in res.get("EvaluationResults", [])] == ["allowed"]
    except Exception as e:  # noqa: BLE001
        return "unknown (%s)" % ca.code_of(e)


def late_tick_state(events, scheduler, iam, plan, snap, log):
    """What the account says about each late tick: schedule present?, parent tick rule shape, invoke role."""
    tmpl = template_schedule(scheduler, snap)
    role = (tmpl or {}).get("Target", {}).get("RoleArn")
    allowed = role_may_invoke(iam, role) if role else False
    out = {}
    for name, lt in sorted(plan["late_ticks"].items()):
        try:
            live = ca.retrying(lambda: scheduler.get_schedule(Name=lt["name"], GroupName=lt["group"]))
        except Exception as e:  # noqa: BLE001
            if ca.code_of(e) != "ResourceNotFoundException":
                raise
            live = None
        parent = ca._live_rule(events, "default", "jhk-tick-" + lt["from_tick"])
        parent_targets = ca.retrying(lambda: ca.pages(events, "list_targets_by_rule", "Targets", Rule="jhk-tick-" + lt["from_tick"], EventBusName="default")) if parent else []
        out[name] = {"schedule": lt["name"], "present": live is not None, "live": (live or {}).get("ScheduleExpression"), "live_state": (live or {}).get("State"),
                     "live_target_fn": ca.fn_name(((live or {}).get("Target") or {}).get("Arn")),
                     "parent_present": parent is not None, "parent_expr": (parent or {}).get("ScheduleExpression"), "parent_state": (parent or {}).get("State"),
                     "parent_target_is_scheduler": any(ca.fn_name(t.get("Arn")) == po.SCHEDULER_FN for t in parent_targets),
                     "template_schedule": (tmpl or {}).get("Name"), "role": role, "role_may_invoke_scheduler": allowed}
        log("late tick %s: %s" % (name, json.dumps(out[name])))
    return out, role


def create_late_tick(scheduler, name, lt, role, store, log):
    kw = {"Name": lt["name"], "GroupName": lt["group"], "ScheduleExpression": lt["expr"], "ScheduleExpressionTimezone": "UTC",
          "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED",
          "Description": "pipeline order (ops 6585): members of %s that read other members run a quarter hour later" % lt["from_tick"],
          "Target": {"Arn": SCHEDULER_ARN, "RoleArn": role, "Input": json.dumps({"tick": name})}}
    try:
        ca.retrying(lambda: scheduler.create_schedule(**kw), tries=8)
        how = "created"
    except Exception as e:  # noqa: BLE001
        if ca.code_of(e) != "ConflictException":
            raise
        ca.retrying(lambda: scheduler.update_schedule(**kw), tries=8)
        how = "updated"
    live = ca.retrying(lambda: scheduler.get_schedule(Name=lt["name"], GroupName=lt["group"]))
    ok = live.get("State") == "ENABLED" and live.get("ScheduleExpression") == lt["expr"] and ca.fn_name(live["Target"].get("Arn")) == po.SCHEDULER_FN \
        and json.loads(live["Target"].get("Input") or "{}").get("tick") == name
    store.put(ca.safe_key("late-tick", lt["name"] + ".json"), {k: v for k, v in live.items() if k != "ResponseMetadata"})
    log("late tick %s: schedule %s %s (%s) -> %s" % (name, lt["name"], live.get("State"), how, "ok" if ok else "NOT AS PLANNED"))
    return ok


def manifest_edits(s3, plan, snap, log):
    """-> {key: (old_text, new_text, note)} for the two S3 manifests (new_text None = nothing to change)."""
    final = {(t["kind"], t["name"]): (t.get("expr") or "", t.get("state")) for t in snap if t.get("name") and not t.get("err")}
    for a in plan["actions"]:
        final[(a["kind"], a["name"])] = (a["after"]["expr"], a["after"]["state"])
    out = {}
    key = "config/fanout-manifest.json"
    old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
    fan = json.loads(old)
    style = next(((ind, ea, nl) for ind in (1, 2, None) for ea in (False, True) for nl in ("\n", "")
                  if json.dumps(fan, indent=ind, ensure_ascii=ea) + nl == old), None)
    moved = po.apply_fanout(fan, plan)
    ind, ea, nl = style or (1, False, "\n")
    new = json.dumps(fan, indent=ind, ensure_ascii=ea) + nl
    repo = json.loads((ROOT / key).read_text(encoding="utf-8"))
    note = "%d member(s) move to a late tick; result %s the committed repo file%s" % (
        moved, "equals" if json.loads(new) == repo else "DIFFERS from", "" if style else " (S3 copy reformatted: no known style)")
    out[key] = (old, new if new != old else None, note)
    log("%s: %s" % (key, note))
    key = "config/schedule-manifest.json"
    old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
    new, n = cp.apply_schedule_manifest(old, plan["actions"], final)
    sched = json.loads(new)
    have = {e["name"] for e in sched["schedules"]}
    added = [e for e in po.late_tick_entries(sched, plan) if e["name"] not in have]
    if added:
        sched["schedules"] = sorted(sched["schedules"] + added, key=lambda e: e["name"])
        new = json.dumps(sched, indent=1, ensure_ascii=False) + "\n"
    repo = json.loads((ROOT / key).read_text(encoding="utf-8"))
    note = "%d entries retimed, %d schedule entries added; result %s the committed repo file" % (n, len(added), "equals" if sched == repo else "DIFFERS from")
    out[key] = (old, new if new != old else None, note)
    log("%s: %s" % (key, note))
    return out


def run(r, dry_run, workers=4):
    import boto3
    from botocore.config import Config
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=30)
    events = boto3.client("events", region_name=REGION, config=cfg)
    scheduler = boto3.client("scheduler", region_name=REGION, config=cfg)
    iam = boto3.client("iam", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    stamp = ca.stamp()
    store = ca.Store(s3, PRIVATE, "backups/pipeline-order/%s" % stamp)
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    floor = json.loads((ROOT / "config/cadence-floor.json").read_text(encoding="utf-8"))
    exempt = floor.get("exempt_triggers", [])
    s = plan["summary"]
    r.heading("ops %s - pipeline order: %s" % ("6585 apply" if not dry_run else "6584 DRY RUN", plan["rule"][:100]))
    r.kv(actions=len(plan["actions"]), late_ticks=len(plan["late_ticks"]), tick_members_moved=s["moved_tick_members"],
         races_before=s["before"]["buckets"].get("race", 0), races_after=s["after"]["buckets"].get("race", 0),
         stale_before=s["before"]["buckets"].get("stale", 0), stale_after=s["after"]["buckets"].get("stale", 0),
         mean_input_age_h="%s -> %s" % (s["before"]["mean_input_age_h"], s["after"]["mean_input_age_h"]),
         max_move_min=s["move_minutes"]["max"], dry_run=dry_run, backups="s3://%s/backups/pipeline-order/%s/" % (PRIVATE, stamp))
    if po.main(["--check"]) != 0:
        r.fail("config/pipeline-order-plan.json is not what the committed configs produce; rebuild it")
        raise SystemExit(1)

    r.section("1. live snapshot")
    snap = ca.snapshot_triggers(events, scheduler, log=r.log)
    before_bad = ca.violations(snap, exempt)
    r.log("records %d, enabled triggers able to fire more than once a day: %d" % (len(snap), len(before_bad)))
    if len(snap) < 400:
        r.fail("snapshot incomplete (%d records)" % len(snap))
        raise SystemExit(1)

    r.section("2. retimes (%s)" % ("would change" if dry_run else "changing"))
    results = ca.apply_actions(plan["actions"], events, scheduler, store, dry_run=dry_run, workers=workers, canary=not dry_run, log=r.log)
    summary = ca.summarise(results)
    r.kv(**summary)
    for res in results:
        if res["status"] in ("drift", "missing", "failed", "skipped_managed"):
            r.warn("%s %s: %s %s" % (res["kind"], res["name"], res["status"], res.get("live") or res.get("error") or ""))
    if any(res.get("aborted") for res in results):
        r.fail("canary failed; nothing else was touched")
        raise SystemExit(1)

    r.section("3. late tick(s)")
    lts, role = late_tick_state(events, scheduler, iam, plan, snap, r.log)
    lt_problems = []
    for name, st in lts.items():
        if not st["parent_present"] or not st["parent_target_is_scheduler"] or st["parent_state"] != "ENABLED":
            lt_problems.append("%s: parent tick rule not as expected" % name)
        if st["present"] and (st["live"] != plan["late_ticks"][name]["expr"] or st["live_state"] != "ENABLED" or st["live_target_fn"] != po.SCHEDULER_FN):
            lt_problems.append("%s: schedule %s exists with a different definition" % (name, st["schedule"]))
        if not st["role"] or st["role_may_invoke_scheduler"] is not True:
            lt_problems.append("%s: no proven invoke role for the Scheduler (template %s, role %s, allowed %s)" % (name, st["template_schedule"], st["role"], st["role_may_invoke_scheduler"]))
    for p in lt_problems:
        r.warn(p)
    if not dry_run and not lt_problems:
        for name, lt in sorted(plan["late_ticks"].items()):
            if lts[name]["present"]:
                r.log("late tick %s already in place" % name)
                continue
            if not create_late_tick(scheduler, name, lt, role, store, r.log):
                r.fail("late tick %s not as planned" % name)
                raise SystemExit(1)

    r.section("4. S3 manifests")
    edits = manifest_edits(s3, plan, snap, r.log)
    for key, (old, new, note) in edits.items():
        if new is None:
            r.log("%s: no change needed" % key)
            continue
        if not dry_run:
            store.put(ca.safe_key("manifest", key.split("/")[-1]), {"text": old})
            s3.put_object(Bucket=LIVE, Key=key, Body=new.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            back = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
            if back != new:
                r.fail("%s read-back differs" % key)
                raise SystemExit(1)
            r.ok("%s uploaded (previous copy backed up)" % key)

    r.section("5. verification")
    if dry_run:
        rules_on_bus = len(ca.retrying(lambda: ca.pages(events, "list_rules", "Rules", EventBusName="default")))
        r.kv(would_apply=summary.get("would_apply", 0), already=summary.get("already", 0), drift=summary.get("drift", 0),
             missing=summary.get("missing", 0), failed=summary.get("failed", 0), late_tick_problems=len(lt_problems), rules_on_default_bus=rules_on_bus)
        if summary.get("drift") or summary.get("missing") or summary.get("failed") or lt_problems:
            r.warn("plan needs attention before apply (see above)")
        else:
            r.ok("dry run clean: %d retimes ready, %d late tick(s) creatable, nothing drifted" % (summary.get("would_apply", 0), len(plan["late_ticks"])))
        return
    after = ca.snapshot_triggers(events, scheduler, log=r.log)
    live = {(t["kind"], t["name"]): (t.get("expr") or "", t.get("state")) for t in after if t.get("name") and not t.get("err")}
    wrong = [(a["kind"], a["name"], live.get((a["kind"], a["name"]))) for a in plan["actions"]
             if live.get((a["kind"], a["name"])) != (a["after"]["expr"], a["after"]["state"])]
    for lt in plan["late_ticks"].values():
        if live.get((lt["kind"], lt["name"])) != (lt["expr"], "ENABLED"):
            wrong.append((lt["kind"], lt["name"], live.get((lt["kind"], lt["name"]))))
    bad = ca.violations(after, exempt)
    r.kv(planned_triggers_live_as_planned=len(plan["actions"]) + len(plan["late_ticks"]) - len(wrong), not_as_planned=len(wrong),
         enabled_violators_after=len(bad), enabled_violators_before=len(before_bad))
    for w in wrong[:20]:
        r.warn("not at its planned expression: %s %s live=%s" % w)
    for b in bad[:20]:
        r.warn("able to fire >1/day: %s %s %s" % (b["kind"], b["name"], b["expr"]))
    store.put("after-snapshot.json", {"generated_at": ca.stamp(), "triggers": after})
    if wrong or bad or summary.get("failed"):
        r.fail("rollout incomplete: %d not as planned, %d violators, %d failed writes" % (len(wrong), len(bad), summary.get("failed", 0)))
        raise SystemExit(1)
    r.ok("pipeline order applied: %d triggers retimed, %d late tick(s) live, cadence floor intact" % (summary.get("applied", 0) + summary.get("already", 0), len(plan["late_ticks"])))
